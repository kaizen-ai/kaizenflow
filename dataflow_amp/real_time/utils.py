import datetime
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import pytz

import core.dataflow as cdataf
import helpers.cache as hcache
import helpers.datetime_ as hdatetime
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)


#@lru_cache.
def load_db_example_data() -> pd.DataFrame:
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
