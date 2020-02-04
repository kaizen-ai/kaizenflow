"""
Import as:

import helpers.datetime_ as hdt
"""

import datetime

import pandas as pd
from typing import Union

import helpers.dbg as dbg


DATETIME_TYPE = Union[pd.Timestamp, datetime.datetime]


def get_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def check_et_timezone(dt):
    # TODO(gp): Check if dateutils is better.
    import pytz

    tzinfo = dt.tzinfo
    ret = tzinfo.zone in (
        pytz.timezone("US/Eastern").zone,
        pytz.timezone("America/New_York").zone,
    )
    dbg.dassert(
        ret,
        "dt=%s (type=%s) tzinfo=%s (type=%s) tzinfo.zone=%s",
        dt,
        type(dt),
        tzinfo,
        type(tzinfo),
        tzinfo.zone,
    )
    return True


def validate_datetime(timestamp: DATETIME_TYPE) -> pd.Timestamp:
    """
    Assert that timestamp is in UTC, convert to pd.Timestamp.

    :param timestamp: datetime object or pd.Timestamp
    :return: tz-aware pd.Timestamp
    """
    dbg.dassert_in(type(timestamp), (pd.Timestamp, datetime.datetime))
    pd_timestamp = pd.Timestamp(timestamp)
    dbg.dassert_eq(pd_timestamp.tzinfo.zone, "UTC")
    return pd_timestamp
