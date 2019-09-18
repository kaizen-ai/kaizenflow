import datetime

import helpers.dbg as dbg


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
