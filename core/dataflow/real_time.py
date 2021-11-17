"""
Import as:

import core.dataflow.real_time as cdtfretim
"""

import asyncio
import collections
import datetime
import logging
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    cast,
)

import numpy as np
import pandas as pd

import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.hasyncio as hhasynci
import helpers.hnumpy as hhnumpy
import helpers.printing as hprintin

_LOG = logging.getLogger(__name__)

# There are different ways of reproducing real-time behaviors:
# 1) True real-time
#    - We are running against the real prod / QA system
#    - The query to a DB returns the true real-time values, with different results
#      depending on the actual wall-clock time
# 2) Simulated real-time
#   - The advancing of time is simulated through calling a method (e.g.,
#     `set_current_time(simulated_time)`) or through a simulated version of the
#     `asyncio` `EventLoop`
# 3) Replayed time
#    - The wall-clock time is transformed into a historical wall-clock time
#    - E.g., 5:30pm today is remapped to 9:30pm of 2021-01-04
#    - Data is returned depending on the current wall-clock time
#      - The data can be frozen from a real-time system or synthetic
# 4) Simulated replayed time
#    - The time is simulated in terms of events and replayed in the past


# #############################################################################
# ReplayedTime.
# #############################################################################


# TODO(gp): -> _ReplayedTime
class ReplayedTime:
    """
    Allow to test a real-time system replaying current times in the past.

    A use case is the following:
    - Assume we have captured data in an interval starting on `2021-01-04 9:30am`
      (called `initial_replayed_dt`) until the following day `2021-01-05 9:30am`
    - We want to replay this data in real-time starting now, which is by example
      `2021-06-04 10:30am` (called `initial_wall_clock_dt`)
    - We use this class to map times after `2021-06-04 10:30am` to the corresponding
      time after `2021-01-04 9:30am`
    - E.g., when we ask to this class the current "replayed" time at (wall clock
      time) `2021-06-04 12:00pm`, the class returns `2021-01-04 11:00am`, since 1hr
      has passed since the `initial_wall_clock_dt`

    In other terms this class mocks `datetime.datetime.now()` so that the actual
    wall clock time `initial_wall_clock_dt` corresponds to `initial_replayed_dt`.
    """

    def __init__(
        self,
        initial_replayed_dt: pd.Timestamp,
        get_wall_clock_time: hdatetim.GetWallClockTime,
        *,
        speed_up_factor: float = 1.0,
    ):
        """
        Constructor.

        If param `initial_replayed_dt` has timezone info then this class works in
        the same timezone.

        :param initial_replayed_dt: the time that we want the current wall clock
            time to correspond to
        :param get_wall_clock_time: return the wall clock time. It is usually
            a closure of `hdatetim.get_wall_clock_time()`. The returned time needs
            to have the same timezone as `initial_replayed_dt`
        """
        # This is the original time we want to "rewind" to.
        hdbg.dassert_isinstance(initial_replayed_dt, pd.Timestamp)
        self._initial_replayed_dt = initial_replayed_dt
        hdbg.dassert_isinstance(get_wall_clock_time, Callable)
        self._get_wall_clock_time = get_wall_clock_time
        hdbg.dassert_lt(0, speed_up_factor)
        self._speed_up_factor = speed_up_factor
        # This is when the experiment starts.
        self._initial_wall_clock_dt = self._get_wall_clock_time()
        _LOG.debug(
            hprintin.to_str(
                "self._initial_replayed_dt self._initial_wall_clock_dt"
            )
        )
        hdatetim.dassert_tz_compatible(
            self._initial_replayed_dt, self._initial_wall_clock_dt
        )
        hdbg.dassert_lte(
            self._initial_replayed_dt,
            self._initial_wall_clock_dt,
            msg="Replaying time can be done only for the past. "
            "The future can't be replayed yet",
        )

    def get_wall_clock_time(self) -> pd.Timestamp:
        """
        Transform the current time into the time corresponding to the real-time
        experiment starting at `initial_simulated_dt`.
        """
        now = self._get_wall_clock_time()
        hdbg.dassert_lte(self._initial_wall_clock_dt, now)
        elapsed_time = now - self._initial_wall_clock_dt
        current_replayed_dt = (
            self._initial_replayed_dt + self._speed_up_factor * elapsed_time
        )
        return current_replayed_dt


def get_replayed_wall_clock_time(
    tz: str,
    initial_replayed_dt: pd.Timestamp,
    *,
    event_loop: Optional[asyncio.AbstractEventLoop] = None,
    speed_up_factor: float = 1.0,
) -> hdatetim.GetWallClockTime:
    """
    Return a function reporting the wall clock time using a replayed approach
    for real and simulated time.
    """
    get_wall_clock_time = lambda: hdatetim.get_current_time(
        tz, event_loop=event_loop
    )
    replayed_time = ReplayedTime(
        initial_replayed_dt, get_wall_clock_time, speed_up_factor=speed_up_factor
    )
    return replayed_time.get_wall_clock_time


# #############################################################################


# TODO(gp): This doesn't belong here, but it's not clear where should it go.
def generate_synthetic_data(
    columns: List[str],
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    *,
    freq: str = "1T",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic data used to mimic real-time data.
    """
    hdatetim.dassert_tz_compatible(start_datetime, end_datetime)
    hdbg.dassert_lte(start_datetime, end_datetime)
    dates = pd.date_range(start_datetime, end_datetime, freq=freq)
    # TODO(gp): Filter by ATH, if needed.
    # Random walk with increments independent and uniform in [-0.5, 0.5].
    with hhnumpy.random_seed_context(seed):
        data = np.random.rand(len(dates), len(columns)) - 0.5  # type: ignore[var-annotated]
    df = pd.DataFrame(data, columns=columns, index=dates)
    df = df.cumsum()
    return df


def get_data_as_of_datetime(
    df: pd.DataFrame,
    knowledge_datetime_col_name: Optional[str],
    datetime_: pd.Timestamp,
    *,
    delay_in_secs: int = 0,
    allow_future_peeking: bool = False,
) -> pd.DataFrame:
    """
    Extract data available at `datetime_` from a df indexed with knowledge
    time.

    :param df: df indexed with timestamp representing knowledge time
    :param knowledge_datetime_col_name: column in the df representing the knowledge time,
        or `None` for using the index
    :param datetime_: the "as of" timestamp (in the sense of `<=`)
    :param delay_in_secs: represent how long it takes for the simulated system to
        respond. E.g., if the data comes from a DB, `delay_in_secs` is the delay
        of the data query with respect to the knowledge time.

    E.g., if the "as of" timestamp is `2021-07-13 13:01:00` and the simulated system
    takes 4 seconds to respond, all and only data before `2021-07-13 13:00:56` is
    returned.
    """
    _LOG.debug(hprintin.df_to_short_str("Before get_data_as_of_datetime", df))
    _LOG.debug(
        hprintin.to_str("knowledge_datetime_col_name datetime_ delay_in_secs")
    )
    hdbg.dassert_lte(0, delay_in_secs)
    datetime_eff = datetime_ - datetime.timedelta(seconds=delay_in_secs)
    # TODO(gp): We could / should use binary search.
    hdatetim.dassert_tz_compatible_timestamp_with_df(
        datetime_, df, knowledge_datetime_col_name
    )
    if not allow_future_peeking:
        if knowledge_datetime_col_name is None:
            # Filter based on the index.
            mask = df.index <= datetime_eff
        else:
            # Filter based on a column.
            hdbg.dassert_in(knowledge_datetime_col_name, df.columns)
            mask = df[knowledge_datetime_col_name] <= datetime_eff
        df = df[mask]
    else:
        # Sometimes we need to allow the future peeking. E.g., to know what's the
        # execution price of an order that will terminate in the future.
        # raise ValueError("Future peeking")
        pass
    _LOG.debug(hprintin.df_to_short_str("After get_data_as_of_datetime", df))
    return df


# #############################################################################
# Real time.
# #############################################################################


async def _sleep(sleep_in_secs: float) -> None:
    hdbg.dassert_lte(0, sleep_in_secs)
    await asyncio.sleep(sleep_in_secs)


def align_on_time_grid(
    get_wall_clock_time: hdatetim.GetWallClockTime,
    grid_time_in_secs: float,
    *,
    event_loop: Optional[asyncio.AbstractEventLoop],
    use_high_resolution: bool = False,
) -> None:
    """
    Wait until the current wall clock time is aligned on `grid_time_in_secs`.

    E.g., for `grid_time_in_secs` = 2, if wall clock time is `2021-07-29 10:45:51`,
    then this function terminates when the wall clock is `2021-07-29 10:46:00`.

    :param use_high_resolution: `time.sleep()` has low resolution, so by
        default this function spins on the wall clock until the proper amount of
        time has elapsed
    """
    _LOG.info("Aligning on %s secs", grid_time_in_secs)

    def _wait(sleep_in_secs: float) -> None:
        _LOG.debug("wall_clock=%s", get_wall_clock_time())
        _LOG.debug("wait for %s secs", sleep_in_secs)
        if sleep_in_secs > 0:
            coroutine = _sleep(sleep_in_secs)
            hhasynci.run(coroutine, event_loop=event_loop, close_event_loop=False)
            _LOG.debug("wall_clock=%s", get_wall_clock_time())

    _LOG.debug("Aligning at wall_clock=%s ...", get_wall_clock_time())
    current_time = get_wall_clock_time()
    # Align on the time grid.
    hdbg.dassert_lt(0, grid_time_in_secs)
    freq = f"{grid_time_in_secs}S"
    target_time = current_time.ceil(freq)
    hdbg.dassert_lte(current_time, target_time)
    _LOG.debug("target_time=%s", target_time)
    secs_to_wait = (target_time - current_time).total_seconds()
    #
    if use_high_resolution:
        # Wait for a bit and then busy wait.
        hdbg.dassert_eq(
            event_loop, None, "High resolution sleep works only in real-time"
        )
        _wait(secs_to_wait - 1)
        # Busy waiting. OS courses says to never do this, but in this case we need
        # a high-resolution wait.
        while True:
            current_time = get_wall_clock_time()
            if current_time >= target_time:
                break
    else:
        _wait(secs_to_wait)
    _LOG.debug("Aligning done at wall_clock=%s", get_wall_clock_time())


class Event(
    collections.namedtuple("Event", "num_it current_time wall_clock_time")
):
    """
    Information about the real time execution.

    :param num_it: number of iteration of the clock
    :param current_time: the simulated time
    :param wall_clock_time: the actual wall clock time of the running system for
        accounting
    """

    def __str__(self) -> str:
        return self.to_str(
            include_tenths_of_secs=False, include_wall_clock_time=True
        )

    # From
    # https://docs.python.org/3/library/collections.html#
    #    namedtuple-factory-function-for-tuples-with-named-fields

    def to_str(
        self, include_tenths_of_secs: bool, include_wall_clock_time: bool
    ) -> str:
        vals = []
        vals.append("num_it=%s" % self.num_it)
        #
        current_time = self.current_time
        if not include_tenths_of_secs:
            current_time = current_time.replace(microsecond=0, nanosecond=0)
        vals.append("current_time='%s'" % current_time)
        #
        if include_wall_clock_time:
            vals.append("wall_clock_time='%s'" % self.wall_clock_time)
        return " ".join(vals)


class Events(List[Event]):
    """
    A list of events.
    """

    def __str__(self) -> str:
        return "\n".join(map(str, self))

    def to_str(self, *args: Any, **kwargs: Any) -> str:
        return "\n".join([x.to_str(*args, **kwargs) for x in self])


async def execute_with_real_time_loop(
    get_wall_clock_time: hdatetim.GetWallClockTime,
    sleep_interval_in_secs: float,
    time_out_in_secs: Optional[int],
    workload: Callable[[pd.Timestamp], Any],
) -> AsyncGenerator[Tuple[Event, Any], None]:
    """
    Execute a function using an event loop.

    :param get_wall_clock_time: function returning the current true or simulated time
    :param sleep_interval_in_secs: the loop wakes up every `sleep_interval_in_secs`
        true or simulated seconds
    :param time_out_in_secs: for how long to execute the loop. `None` means an infinite loop
    :param workload: function executing the workload

    :return: a Tuple with:
        - an execution trace representing the events in the real-time loop; and
        - a list of results returned by the workload function
    """
    _LOG.debug(hprintin.to_str("sleep_interval_in_secs time_out_in_secs"))
    hdbg.dassert(
        callable(get_wall_clock_time),
        "get_wall_clock_time='%s' is not callable",
        str(get_wall_clock_time),
    )
    hdbg.dassert_lt(0, sleep_interval_in_secs)
    num_iterations: Optional[int] = None
    if time_out_in_secs is not None:
        # TODO(gp): Consider using a real-time check instead of number of iterations.
        num_iterations = int(time_out_in_secs / sleep_interval_in_secs)
        hdbg.dassert_lt(0, num_iterations)
    _LOG.debug(hprintin.to_str("num_iterations"))
    #
    num_it = 1
    while True:
        wall_clock_time = get_wall_clock_time()
        # For the wall clock time, we always use the real one. This is used only for
        # book-keeping.
        real_wall_clock_time = hdatetim.get_current_time(tz="ET")
        _LOG.debug(
            "\n%s",
            hprintin.frame(
                "Real-time loop: "
                "num_it=%s / %s: wall_clock_time='%s' real_wall_clock_time='%s'"
                % (num_it, num_iterations, wall_clock_time, real_wall_clock_time),
                char1="<",
            ),
        )
        # Update the current events.
        event = Event(num_it, wall_clock_time, real_wall_clock_time)
        _LOG.debug("event='%s'", str(event))
        # Execute workload.
        _LOG.debug("await ...")
        # TODO(gp): Compensate for drift.
        result = await asyncio.gather(  # type: ignore[var-annotated]
            asyncio.sleep(sleep_interval_in_secs),
            # We need to use the passed `wall_clock_time` since that's what being
            # used as real, simulated, replayed time.
            workload(wall_clock_time),
        )
        _LOG.debug("await done")
        _, workload_result = result
        yield event, workload_result
        # Exit, if needed.
        if num_iterations is not None and num_it >= num_iterations:
            break
        num_it += 1


async def execute_all_with_real_time_loop(
    *args: Tuple[Any], **kwargs: Dict[str, Any]
) -> Tuple[List[Event], List[Any]]:
    """
    Execute the entire event loop until the end.

    This is a way to bridge the async to sync semantic. It is
    conceptually equivalent to adding a list around a Python generator.
    """
    vals = zip(
        *[
            v
            async for v in execute_with_real_time_loop(
                *args, **kwargs  # type: ignore[arg-type]
            )
        ]
    )
    events, results = list(vals)
    events = Events(events)
    results = list(results)
    return events, results
