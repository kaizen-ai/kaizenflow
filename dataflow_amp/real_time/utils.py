"""
Import as:

import dataflow_amp.real_time.utils as dartu
"""

import datetime
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import pytz

import core.dataflow as cdataf
import helpers.dbg as dbg
import helpers.cache as hcache
import helpers.datetime_ as hdatetime
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


#@lru_cache.
def load_db_example_data_from_disk() -> pd.DataFrame:
    """
    Load some example data from the RT DB.
    """
    file_name = "/app/bars_qa.csv"
    # datetime_cols = ["start_time", "end_time"]
    df = pd.read_csv(file_name, index_col=0, parse_dates=[6,7])
    df.sort_values(by="end_time", inplace=True)
    return df


def get_db_data(datetime_: datetime.datetime, db_delay_in_secs=0):
    """
    Get the data from the example RT DB at time `datetime_`, assuming that the DB
    takes `db_delay_in_secs` to update.

    I.e., the data market `2021-07-13 13:01:00`
    """
    hdatetime.dassert_has_tz(datetime_)
    df = load_db_example_data()
    # Convert in UTC since the RT DB uses implicitly UTC.
    datetime_utc = datetime_.astimezone(pytz.timezone("UTC")).replace(tzinfo=None)
    # TODO(gp): We could also use the `timestamp_db` field.
    datetime_utc_eff = datetime_utc - datetime.timedelta(seconds=db_delay_in_secs)
    mask = df["end_time"] <= datetime_utc_eff
    df = df[mask].copy()
    return df


import numpy as np
#
# class RealTimeSyntheticDataSource(cdataf.DataSource):
#     def __init__(
#             self,
#             nid: str,
#             columns: List[str],
#             start_date: Optional[hdatetime.Datetime] = None,
#             end_date: Optional[hdatetime.Datetime] = None,
#     ) -> None:
#         super().__init__(nid)
#         self._columns = columns
#         self._start_date = start_date
#         self._end_date = end_date
#         # This indicates what time it is, so that the node can emit data up to that
#         # time. In practice, it is used to simulate the inexorable passing of time.
#         self._now = None
#         # Store the entire history of the data.
#         self._entire_df = None
#
#     def set_now_time(self, datetime_: pd.Timestamp) -> None:
#         """
#         Set the simulation time.
#         """
#         dbg.dassert_isinstance(datetime_, pd.Timestamp)
#         # Time only moves forward.
#         if self._now is not None:
#             dbg.dassert_lte(self._now, datetime_)
#         self._now = datetime_
#
#     def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
#         self._get_data_until_now()
#         return super().fit()
#
#     def predict(self) -> Optional[Dict[str, pd.DataFrame]]:
#         self._get_data_until_now()
#         return super().predict()
#
#     def _get_data_until_now(self) -> None:
#         """
#         Get data stored inside the node up and including `self._now`.
#
#         E.g., if `self._now = pd.Timestamp("2010-01-04 09:34:00"`)`, then self.df
#         is set to something like:
#         ```
#                                 close    volume
#         ...
#         2010-01-04 09:31:00 -0.377824 -0.660321
#         2010-01-04 09:32:00 -0.508435 -0.349565
#         2010-01-04 09:33:00 -0.151361  0.139516
#         2010-01-04 09:34:00  0.046069 -0.040318
#         ```
#         """
#         dbg.dassert_is_not(self._now, None, "now needs to be set with `set_now_time()`")
#         self._lazy_load()
#         self.df = self._entire_df.loc[:self._now]
#
#     def _lazy_load(self) -> None:
#         if self._entire_df is not None:
#             return
#         dates = pd.date_range(self._start_date, self._end_date, freq="1T")
#         data = np.random.rand(len(dates), len(self._columns)) - 0.5
#         df = pd.DataFrame(data, columns=self._columns, index=dates)
#         df = df.cumsum()
#         self._entire_df = df


def get_now_time():
    # Simulates a sleep(60)
    datetimes = [pd.Timestamp(dt + "-04:00") for dt in [
        "2021-07-15 15:01:00",
        "2021-07-15 15:02:00",
        "2021-07-15 15:03:00",
        "2021-07-15 15:04:00",
        "2022-07-15 15:05:00",
        "2021-07-15 15:06:00",
    ]]
    for dt in datetimes:
        dt = dt.to_pydatetime()
        yield dt


def is_dag_to_execute(datetime_: datetime.datetime):
    """
    Return true if the DAG needs to be executed.
    """
    hdatetime.dassert_has_tz(datetime_)
    return datetime_.minute % 5 == 0
