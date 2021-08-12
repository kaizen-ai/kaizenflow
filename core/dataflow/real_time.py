"""
Import as:

import core.dataflow.real_time as cdtfrt
"""

import asyncio
import collections
import datetime
import logging
import time
from typing import Any, Callable, Iterator, List, Optional, Tuple, cast

import numpy as np
import pandas as pd

import helpers.datetime_ as hdatetime
import helpers.dbg as dbg
import helpers.hnumpy as hnumpy
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# There are different ways of reproducing real-time behaviors:
# 1) True real-time
#    - We are running against the real prod / QA system
#    - The same query to a DB returns the true real-time values, which change
#      depending on the actual wall-clock time
# 2) Simulated real-time
#   - The advancing of time is simulated through calling a method (e.g.,
#     `set_current_time(simulated_time)`) or through a simulted version of the
#     `asyncio` `EventLoop`
# 3) Replayed time
#    - The wall-clock time is transformed in a historical wall-clock time (e.g.,
#      5:30pm today is remapped to 9:30pm of 2021-01-04)
#    - Data is returned depending on the current wall-clock time
#      - The data can be frozen from a real-time system or synthetic
# 4) Simulated replayed time
#    - The time is simulated in terms of events and replayed in the past


# TODO(gp): This doesn't belong here, but it's not clear where should it go.
def generate_synthetic_data(
    columns: List[str],
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic data used to mimic real-time data.
    """
    hdatetime.dassert_tz_compatible(start_datetime, end_datetime)
    dbg.dassert_lte(start_datetime, end_datetime)
    dates = pd.date_range(start_datetime, end_datetime, freq="1T")
    # TODO(gp): Filter by ATH, if needed.
    # Random walk with increments independent and uniform in [-0.5, 0.5].
    with hnumpy.random_seed_context(seed):
        data = np.random.rand(len(dates), len(columns)) - 0.5
    df = pd.DataFrame(data, columns=columns, index=dates)
    df = df.cumsum()
    return df


# #############################################################################
# Time generator
# #############################################################################


class ReplayedTime:
    """
    Allow to test a real-time system replaying current times in the past.

    A use case is the following:
    - Assume we have captured data in an interval starting on `2021-01-04 9:30am`
      (which we call `initial_replayed_dt`) until the following day `2021-01-05 9:30am`
    - We want to replay this data in real-time starting now, which is by example
      `2021-06-04 10:30am` (which we call `initial_wall_clock_dt`)
    - We use this class to map times after `2021-06-04 10:30am` to the corresponding
      time after `2021-01-04 9:30am`
    - E.g., when we ask to this class the current "replayed" time at (wall clock
      time) `2021-06-04 12:00pm`, the class returns `2021-01-04 11:00am`, since 1hr
      has passed since the `initial_wall_clock_dt`

    In other terms this class mocks `datetime.datetime.now()` so that the actual
    wall clock time `initial_wall_clock_dt` corresponds to `initial_replayed_dt`

    """

    def __init__(
        self,
            initial_replayed_dt: pd.Timestamp,
            get_wall_clock_time: hdatetime.GetWallClockTime,
    ):
        """
        Constructor.

        If param `initial_replayed_dt` has timezone info then this class works in
        the same timezone.

        :param initial_replayed_dt: the time that we want the current wall clock
            time to correspond to
        :param get_wall_clock_time: return the wall clock time. It is usually
            a closure of `hdatetime.get_wall_clock_time()`. The returned time needs
            to have the same timezone as `initial_replayed_dt`
        """
        # This is the original time we want to "rewind" to.
        self._initial_replayed_dt = initial_replayed_dt
        self._get_wall_clock_time = get_wall_clock_time
        # This is when the experiment start.
        self._initial_wall_clock_dt = self._get_wall_clock_time()
        _LOG.debug(
            hprint.to_str(
                "self._initial_replayed_dt self._initial_wall_clock_dt"))
        hdatetime.dassert_tz_compatible(self._initial_replayed_dt,
                                        self._initial_wall_clock_dt)
        dbg.dassert_lte(
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
        dbg.dassert_lte(self._initial_wall_clock_dt, now)
        elapsed_time = now - self._initial_wall_clock_dt
        current_replayed_dt = self._initial_replayed_dt + elapsed_time
        return current_replayed_dt


def get_data_as_of_datetime(
        df: pd.DataFrame, datetime_: pd.Timestamp, delay_in_secs: int = 0
) -> pd.DataFrame:
    """
    Extract data available at `datetime_` from a df indexed with knowledge time.

    :param df: df indexed with timestamp representing knowledge time
    :param datetime_: the "as of" timestamp
    :param delay_in_secs: represent how long it takes for the simulated system to
        respond. E.g., if the data comes from a DB, `delay_in_secs` is the delay
        of the data query with respect to the knowledge time.

    E.g., if the "as of" timestamp is `2021-07-13 13:01:00` and the simulated system
    takes 4 seconds to respond, all and only data before `2021-07-13 13:00:56` is
    returned.
    """
    dbg.dassert_lte(0, delay_in_secs)
    hdatetime.dassert_tz_compatible_timestamp_with_df(datetime_, df)
    # TODO(gp): We could also use the `timestamp_db` field if available.
    datetime_eff = datetime_ - datetime.timedelta(seconds=delay_in_secs)
    mask = df.index <= datetime_eff
    df = df[mask].copy()
    return df


# #############################################################################


def execute_every_2_seconds(datetime_: pd.Timestamp) -> bool:
    """
    Return true every other second.
    """
    ret = datetime_.second % 2 == 0
    ret = cast(bool, ret)
    return ret


def execute_every_5_minutes(datetime_: pd.Timestamp) -> bool:
    """
    Return true if `datetime_` is aligned on a 5 minute grid.
    """
    ret = datetime_.minute % 5 == 0
    ret = cast(bool, ret)
    return ret


# #############################################################################
# Real time loop.
# #############################################################################


def align_on_even_second(use_time_sleep: bool = False) -> None:
    """
    Wait until the current wall clock time reports an even number of seconds.

    E.g., if wall clock time is `2021-07-29 10:45:51`, then this function
    terminates when the wall clock is `2021-07-29 10:46:00`.

    :param use_time_sleep: `time.sleep()` has low resolution, so by default this
        function spins on the wall clock until the proper amount of time has elapsed
    """
    current_time = hdatetime.get_current_time(tz="ET")
    # Align on 2 seconds.
    target_time = current_time.round("2S")
    if target_time < current_time:
        target_time += datetime.timedelta(seconds=2)
    if use_time_sleep:
        secs_to_wait = (target_time - current_time).total_seconds()
        # _LOG.debug(hprint.to_str("current_time target_time secs_to_wait"))
        dbg.dassert_lte(0, secs_to_wait)
        time.sleep(secs_to_wait)
    else:
        # Busy waiting. OS courses says to never do this, but in this case we need
        # a high-resolution wait.
        while True:
            current_time = hdatetime.get_current_time(tz="ET")
            if current_time >= target_time:
                break


class Event(
    collections.namedtuple(
        "Event", "num_it current_time wall_clock_time"
    )
):
    """
    Information about the real time execution.
    """

    def __str__(self) -> str:
        return self.to_str(include_tenths_of_secs=False,
                           include_wall_clock_time=True)

    # From
    # https://docs.python.org/3/library/collections.html#
    #    namedtuple-factory-function-for-tuples-with-named-fields

    def to_str(self, include_tenths_of_secs: bool, include_wall_clock_time: bool) -> str:
        vals = []
        vals.append("num_it=%s" % self.num_it)
        #
        current_time = self.current_time
        if not include_tenths_of_secs:
            current_time = current_time.replace(microsecond=0)
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
    get_wall_clock_time: hdatetime.GetWallClockTime,
    sleep_interval_in_secs: float,
    time_out_in_secs: Optional[int],
    workload: Callable[[pd.Timestamp], Any],
) -> Tuple[Events, List[Any]]:
    """
    Execute a function using a true, simulated, replayed event loop.

    :param sleep_interval_in_secs: the loop wakes up every `sleep_interval_in_secs`
        true or simulated seconds
    :param num_iterations: number of loops to execute. `None` means an infinite loop
    :param get_wall_clock_time: function returning the current true or simulated time
    :param workload: function executing the workload

    :return: a Tuple with:
        - an execution trace representing the events in the real-time loop; and
        - a list of results returned by the workload function
    """
    dbg.dassert_lt(0, sleep_interval_in_secs)
    if time_out_in_secs is not None:
        # TODO(gp): Consider using a real-time check instead of number of iterations.
        num_iterations = int(time_out_in_secs / sleep_interval_in_secs)
        dbg.dassert_lt(0, num_iterations)
    else:
        num_iterations = None
    #
    events = Events()
    results = []
    num_it = 1
    while True:
        wall_clock_time = get_wall_clock_time()
        # For the wall clock time, we always use the real one. This is used only for
        # book-keeping.
        real_wall_clock_time = hdatetime.get_current_time(tz="ET")
        # Update the current events.
        event = Event(num_it, wall_clock_time, real_wall_clock_time)
        _LOG.debug("event='%s'", str(event))
        events.append(event)
        # Execute workload.
        result = await asyncio.gather(
            asyncio.sleep(sleep_interval_in_secs),
            # We need to use the passed `wall_clock_time` since that's what being
            # used as real, simulated, replayed time.
            workload(wall_clock_time),
        )
        results.append(result[1])
        # Exit, if needed.
        if num_iterations is not None and num_it >= num_iterations:
            break
        num_it += 1
    return events, results
