"""
Import as:

import helpers.datetime_ as hdt
"""

import datetime
from typing import Union

import pandas as pd

import helpers.dbg as dbg

DATETIME_TYPE = Union[pd.Timestamp, datetime.datetime]


def get_timestamp(utc: bool = False) -> str:
    if utc:
        timestamp = datetime.datetime.now()
    else:
        timestamp = datetime.datetime.utcnow()
    return timestamp.strftime("%Y%m%d_%H%M%S")


def check_et_timezone(dt: DATETIME_TYPE) -> bool:
    # TODO(gp): Check if dateutils is better.
    import pytz

    tzinfo = dt.tzinfo
    dbg.dassert(tzinfo, "Timestamp should be tz-aware.")
    ret = tzinfo.zone in (  # type: ignore
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
        tzinfo.zone,  # type: ignore
    )
    return True


def validate_datetime(timestamp: DATETIME_TYPE) -> pd.Timestamp:
    """
    Assert that timestamp is in UTC, convert to pd.Timestamp.

    :param timestamp: datetime object or pd.Timestamp
    :return: tz-aware pd.Timestamp
    """
    dbg.dassert_type_in(timestamp, [pd.Timestamp, datetime.datetime])
    pd_timestamp = pd.Timestamp(timestamp)
    dbg.dassert(pd_timestamp.tzinfo, "Timestamp should be tz-aware.")
    dbg.dassert_eq(pd_timestamp.tzinfo.zone, "UTC", "Timezone should be UTC.")
    return pd_timestamp
