"""
Import as:

import core.dataflow.real_time as cdtfrt
"""

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

_LOG = logging.getLogger(__name__)

# There are different ways of reproducing a real-time behaviors:
# 1) True real-time
#    - We are running against the real prod / QA system
#    - The same query to a DB returns the true values, which change depending on the
#      actual wall-clock time
# 2) Replayed real-time
#    - Real or synthetic data is returned depending on the current wall-clock time
#    - The wall-clock time is transformed in historical wall-clock time (e.g.,
#      5:30pm today is remapped to 9:30pm of 2021-01-04)
# 3) Simulated real-time
#   - There is no wall-clock time, but the advancing of time is simulated through
#     calling a method (e.g., `set_current_time(simulated_time)`)


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


def get_data_as_of_datetime(
    df: pd.DataFrame, datetime_: pd.Timestamp, delay_in_secs: int = 0
) -> pd.DataFrame:
    """
    Extract data from a df (indexed with knowledge time) available at
    `datetime_`

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


class ReplayRealTime:
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

    :param initial_replayed_dt: if it has timezone info then this class works
        returns times in the same timezone
    """

    def __init__(
        self, initial_replayed_dt: pd.Timestamp, speed_up_factor: float = 1.0
    ):
        """
        :param initial_replayed_dt: this is the time that we want the current
            wall clock time to correspond to
        :param speed_up_factor: how fast time passes. One wall clock second
            corresponds to `speed_up_factor` replayed seconds
        """
        # This is the original time we want to "rewind" to.
        _LOG.debug("initial_replayed_dt=%s", initial_replayed_dt)
        self._initial_replayed_dt = initial_replayed_dt
        # This is when the experiment start.
        now = self._get_wall_clock_time()
        self._initial_wall_clock_dt = now
        dbg.dassert_lte(
            self._initial_replayed_dt,
            self._initial_wall_clock_dt,
            msg="Replaying time can be done only for the past. "
            "The future can't be replayed yet",
        )
        #
        self._speed_up_factor = speed_up_factor

    def get_replayed_current_time(self) -> pd.Timestamp:
        """
        When replaying data, transform the current time into the corresponding
        time if the real-time experiment started at `initial_simulated_dt`.
        """
        now = self._get_wall_clock_time()
        dbg.dassert_lte(self._initial_wall_clock_dt, now)
        elapsed_time = self._speed_up_factor * (now - self._initial_wall_clock_dt)
        current_replayed_dt = self._initial_replayed_dt + elapsed_time
        return current_replayed_dt

    def _get_wall_clock_time(self) -> pd.Timestamp:
        if self._initial_replayed_dt.tz is None:
            tz = None
        else:
            tz = self._initial_replayed_dt.tz
        _LOG.debug("Using tz '%s'", tz)
        now = pd.Timestamp(datetime.datetime.now(tz))
        _LOG.debug("now='%s'", now)
        return now


def get_simulated_current_time(
    start_datetime: pd.Timestamp, end_datetime: pd.Timestamp, freq: str = "1T"
) -> Iterator[pd.Timestamp]:
    """
    Iterator yielding timestamps in the given interval and with the given
    frequency.

    E.g., `freq = "1T"` can be used to simulate a system sampled every minute.
    """
    datetimes = pd.date_range(start_datetime, end_datetime, freq=freq)
    for dt in datetimes:
        yield dt


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


def align_on_even_second(use_time_sleep: bool = False) -> None:
    """
    Wait until the current wall clock time reports an even number of seconds.

    E.g., if wall clock time is `2021-07-29 10:45:51`, then this function
    terminates when the wall clock is `2021-07-29 10:46:00`.

    :param use_time_sleep: `time.sleep()` has low resolution, so by default this
        function spins on the clock until the proper amount of time has elapsed
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
        # Busy waiting. OS classes says to never do this, but in this case we need
        # a high-resolution wait.
        while True:
            current_time = hdatetime.get_current_time(tz="ET")
            if current_time >= target_time:
                break


# #############################################################################
# Real time loop.
# #############################################################################


# Information about the real time execution.
RealTimeEvent = collections.namedtuple(
    "RealTimeEvent", "num_it current_time wall_clock_time need_execute"
)


def real_time_event_to_str(rte: RealTimeEvent) -> str:
    dbg.dassert_isinstance(rte, RealTimeEvent)
    vals = []
    vals.append("num_it=%s" % rte.num_it)
    timestamp_as_str = rte.current_time.strftime("%Y%m%d_%H%M%S")
    # Add tenths of second.
    timestamp_as_str += "%s" % int(rte.current_time.microsecond // 1e5)
    vals.append("current_time=%s" % timestamp_as_str)
    vals.append("need_execute=%s" % rte.need_execute)
    return " ".join(vals)


ExecutionTrace = List[RealTimeEvent]


# Type for a function that return the current (true or replayed) time as a timestamp.
GetCurrentTimeFunction = Callable[[], pd.Timestamp]


def execute_with_real_time_loop(
    sleep_interval_in_secs: float,
    num_iterations: Optional[int],
    get_current_time: GetCurrentTimeFunction,
    need_to_execute: Callable[[pd.Timestamp], bool],
    workload: Callable[[pd.Timestamp], Any],
) -> Tuple[ExecutionTrace, List[Any]]:
    """
    Execute a function using a true or simulated real-time loop.

    :param sleep_interval_in_secs: the loop wakes up every `sleep_interval_in_secs`
        true or simulated seconds
    :param num_iterations: number of loops to execute. `None` means an infinite loop
    :param get_current_time: function returning the current true or simulated time
    :param need_to_execute: function returning true when the DAG needs to be
        executed
    :param workload: function executing the work when `need_to_execute()` requires to

    :return: a Tuple with an execution trace representing the events in the
        real-time loop and a list of results from the workload
    """
    dbg.dassert_lt(0, sleep_interval_in_secs)
    if num_iterations is not None:
        dbg.dassert_lt(0, num_iterations)
    #
    execution_trace = []
    results = []
    num_it = 1
    while True:
        # Compute.
        current_time = get_current_time()
        execute = need_to_execute(current_time)
        wall_clock_time = hdatetime.get_current_time(tz="ET")
        event = RealTimeEvent(num_it, current_time, wall_clock_time, execute)
        _LOG.debug("event='%s'", str(event))
        execution_trace.append(event)
        # Execute the workload, if needed.
        if execute:
            _LOG.debug("  -> execute")
            result = workload(current_time)
            results.append((current_time, result))
        # Exit, if needed.
        if num_iterations is not None and num_it >= num_iterations:
            break
        # Go to sleep.
        time.sleep(sleep_interval_in_secs)
        num_it += 1
    return execution_trace, results
