"""
Import as:

import dataflow_amp.real_time.utils as dartu
"""

import datetime
import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import pytz

#import core.dataflow as cdataf
import helpers.dbg as dbg
import helpers.cache as hcache
import helpers.datetime_ as hdatetime
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# TODO(gp): Do not commit this.
#_LOG.setLevel(logging.DEBUG)
#assert 0

# There are various levels of real-time:
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


# TODO(gp): Share with SimulatedRealTimeSyntheticDataSource
def generate_synthetic_data(
    columns: List[str],
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    seed: int = 42) -> pd.DataFrame:
    """
    Load some example data from the RT DB.
    """
    dbg.dassert_lte(start_datetime, end_datetime)
    dates = pd.date_range(start_datetime, end_datetime, freq="1T")
    # TODO(gp): Filter by ATH, if needed.
    # Random walk with increments independent and uniform in [-0.5, 0.5].
    with hnumpy.random_seed_context(seed):
        data = np.random.rand(len(dates), len(columns)) - 0.5
    df = pd.DataFrame(data, columns=self._columns, index=dates)
    df = df.cumsum()
    return df


def get_data_as_of_datetime(df: pd.DataFrame, datetime_: pd.Timestamp, db_delay_in_secs: int =0):
    """
    Extract data from the example RT DB at time `datetime_`, assuming that the DB
    takes `db_delay_in_secs` to update.

    I.e., the data market `2021-07-13 13:01:00`
    """
    #hdatetime.dassert_has_tz(datetime_)
    # Convert in UTC since the RT DB uses implicitly UTC.
    #datetime_utc = datetime_.astimezone(pytz.timezone("UTC")).replace(tzinfo=None)
    # TODO(gp): We could also use the `timestamp_db` field.
    datetime_eff = datetime_ - datetime.timedelta(seconds=db_delay_in_secs)
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
      `2021-06-04 10:30am` (which we call `initial_wallclock_dt`)
    - We use this class to map times after `2021-06-04 10:30am` to the corresponding
      time after `2021-01-04 9:30am`
    - E.g., when we ask to this class the current "replayed" time at (wall clock
      time) `2021-06-04 12:00pm`, the class returns `2021-01-04 11:00am`, since 1hr
      has passed since the `initial_wallclock_dt`

    In other terms this class mocks `datetime.datetime.now()` so that the actual
    wallclock time `initial_wallclock_dt` corresponds to `initial_replayed_dt`

    :param initial_replayed_dt: if it has timezone info then this class works
        returns times in the same timezone
    """

    def __init__(self, initial_replayed_dt: pd.Timestamp, speed_up_factor: float=1.0):
        """
        :param initial_replayed_dt: this is the time that we want the current
            wallclock time to correspond to
        :param speed_up_factor: how fast time passes. One wallclock second
            corresponds to `speed_up_factor` replayed seconds
        """
        # This is the original time we want to "rewind" to.
        _LOG.debug("initial_replayed_dt=%s", initial_replayed_dt)
        self._initial_replayed_dt = initial_replayed_dt
        # This is when the experiment start.
        now = self._get_wallclock_time()
        self._initial_wallclock_dt = now
        dbg.dassert_lte(self._initial_replayed_dt, self._initial_wallclock_dt,
                        msg="Replaying time can be done only for the past. "
                        "The future can't be replayed yet")
        #
        self._speed_up_factor = speed_up_factor

    def get_replayed_current_time(self) -> pd.Timestamp:
        """
        When replaying data, transform the current time into the corresponding time if
        the real-time experiment started at `initial_simulated_dt`.
        """
        now = self._get_wallclock_time()
        dbg.dassert_lte(self._initial_wallclock_dt, now)
        elapsed_time = now - self._initial_wallclock_dt
        current_replayed_dt = self._initial_replayed_dt + elapsed_time
        return current_replayed_dt

    def _get_wallclock_time(self) -> pd.Timestamp:
        if self._initial_replayed_dt.tz is None:
            tz = None
        else:
            tz = self._initial_replayed_dt.tz
        _LOG.debug("Using tz '%s'", tz)
        now = pd.Timestamp(datetime.datetime.now(tz))
        _LOG.debug("now='%s'", now)
        return now


def get_simulated_current_time(start_datetime: pd.Timestamp, end_datetime: pd.Timestamp,
                               freq: str = "1T"):
    # Simulates a sleep(60)
    datetimes = pd.date_range(start_datetime, end_datetime, freq=freq)
    for dt in datetimes:
        yield dt


def execute_every_5_mins(datetime_: pd.Timestamp) -> bool:
    """
    Return true if the DAG needs to be executed.
    """
    return datetime_.minute % 5 == 0

import time

# TODO(gp)_: This should go somewhere else.
def real_time_loop(
        sleep_interval_in_secs: float,
        num_iterations: Optional[int],
        get_current_time: Callable[[], pd.Timestamp],
        need_to_execute: Callable[[pd.Timestamp], bool]
        ):
    """
    :param is_dag_to_execute: return true if the DAG needs to be executed.
    """
    dbg.dassert_lt(0, sleep_interval_in_secs)
    if num_iterations is not None:
        dbg.dassert_lt(0, num_iterations)
    num_it = 1
    while True:
        current_time = get_current_time()
        execute = need_to_execute(current_time)
        _LOG.debug("num_it=%s/%s: current_time=%s", num_it, num_iterations, current_time)
        if execute:
            _LOG.debug("  -> execute")
        if num_iterations is not None and num_it >= num_iterations:
            break
        time.sleep(sleep_interval_in_secs)
        num_it += 1
