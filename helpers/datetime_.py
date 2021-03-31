"""
Import as:

import helpers.datetime_ as hdatet
"""

import calendar
import datetime
import logging
import re
from typing import Callable, Iterable, Optional, Tuple, Union

import dateutil.parser as dparse
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

DATETIME_TYPE = Union[pd.Timestamp, datetime.datetime]


def get_timestamp(utc: bool = False) -> str:
    if utc:
        timestamp = datetime.datetime.utcnow()
    else:
        timestamp = datetime.datetime.now()
    return timestamp.strftime("%Y%m%d_%H%M%S")


def check_et_timezone(dt: DATETIME_TYPE) -> bool:
    # TODO(gp): Check if dateutils is better.
    import pytz

    tzinfo = dt.tzinfo
    dbg.dassert(tzinfo, "Timestamp should be tz-aware.")
    zone = tzinfo.zone  # type: ignore
    ret = zone in (
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
        zone,
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


def to_datetime(dates: Union[pd.Series, pd.Index]) -> Union[pd.Series, pd.Index]:
    """
    Convert string dates to datetime.

    This works like `pd.to_datetime`, but supports more date formats and shifts
    the dates to the end of period instead of the start.

    :param dates: series or index of dates to convert
    :return: datetime dates
    """
    # TODO(Julia): Support ISO 8601 weeks.
    # This function doesn't deal with mixed formats.
    dbg.dassert_isinstance(dates, Iterable)
    dbg.dassert(not isinstance(dates, str))
    # Try converting to datetime using `pd.to_datetime`.
    format_example_index = -1
    date_example = dates.tolist()[format_example_index]
    format_fix = _handle_incorrect_conversions(date_example)
    if format_fix is not None:
        format_, date_modifiction_func = format_fix
        dates = dates.map(date_modifiction_func)
        date_example = dates.tolist()[format_example_index]
    else:
        format_ = None
    datetime_dates = pd.to_datetime(dates, format=format_, errors="coerce")
    # Shift to end of period if conversion has been successful.
    if not pd.isna(datetime_dates).all():
        datetime_example = datetime_dates.tolist()[format_example_index]
        if (
            not pd.isna(datetime_example)
            and datetime_example.strftime("%Y-%m-%d") == date_example
        ):
            return datetime_dates
        shift_func = _shift_to_period_end(date_example)
        if shift_func is not None:
            datetime_dates = datetime_dates.map(shift_func)
        return datetime_dates
    # If standard conversion fails, attempt our own conversion.
    format_determination_output = _determine_date_format(date_example)
    if format_determination_output is None:
        return datetime_dates
    format_, date_modification_func = format_determination_output
    dates = dates.map(date_modification_func)
    return pd.to_datetime(dates, format=format_)


def _handle_incorrect_conversions(
    date: str,
) -> Optional[Tuple[Optional[str], Callable[[str], str]]]:
    """
    Change data pre-processing for cases when `pd.to_datetime` is mistaken.

    :param date: string date
    :return: date format and a function to apply to string dates before passing
        them into `pd.to_datetime()`
    """
    if len(date) in [7, 8]:
        # "2021-M2" is transformed to '2020-01-01 00:00:01' by
        # `pd.to_datetime`.
        if date[:4].isdigit() and date[4] in ["-", ".", "/"] and date[5] == "M":

            def modify_monthly_date(x: str) -> str:
                year_number = int(x[:4])
                month_number = x[6:]
                num_days_in_month = calendar.monthrange(
                    year_number, int(month_number)
                )[1]
                modified_x = f"{x[:4]}-{month_number}-{num_days_in_month}"
                return modified_x

            return "%Y-%m-%d", modify_monthly_date


def _shift_to_period_end(
    date: str,
) -> Optional[Callable[[DATETIME_TYPE], DATETIME_TYPE]]:
    """
    Get function to shift the dates to the end of period.

    :param date: string date
    :return: a function to shift the dates to the end of period. If `None`, no
        shift is needed
    """

    def shift_to_month_end(x: DATETIME_TYPE) -> DATETIME_TYPE:
        return x + pd.offsets.MonthEnd(0)

    def shift_to_quarter_end(x: DATETIME_TYPE) -> DATETIME_TYPE:
        return x + pd.offsets.QuarterEnd(0)

    def shift_to_year_end(x: DATETIME_TYPE) -> DATETIME_TYPE:
        return x + pd.offsets.YearEnd(0)

    if date[:4].isdigit():
        if len(date) == 7:
            if date[5:].isdigit():
                # "2020-12" format.
                return shift_to_month_end
            if date[5] == "Q":
                # "2021-Q1" format.
                return shift_to_quarter_end
        elif len(date) == 6:
            # "2021Q1" format.
            if date[4] == "Q":
                return shift_to_quarter_end
        elif len(date) == 4:
            # "2021" format.
            return shift_to_year_end
    # "September 2020" of "Sep 2020" format.
    # Get a flat list of month aliases. The full month name comes first.
    month_aliases = sum(dparse.parserinfo().MONTHS, ())[::-1]
    pattern = re.compile("|".join(month_aliases), re.IGNORECASE)
    match = pattern.search(date)
    if match is None:
        return
    span = match.span()
    date_without_month = f"{date[:span[0]]}{date[span[1]:]}".strip()
    if len(date_without_month) == 4 and date_without_month.isdigit():
        return shift_to_month_end


def _determine_date_format(
    date: str, date_standard: Optional[str] = None
) -> Optional[Tuple[str, Callable[[str], str]]]:
    """
    Determine date format for cases when `pd.to_datetime` fails.

    :param date: date string
    :param date_standard: "standard" or "ISO_8601", `None` defaults to
        "standard"
    :return: date format and a function to transform date strings before
        converting them to datetime using `pd.to_datetime`
    """
    date_standard = date_standard or "standard"
    if date_standard == "standard":
        year_format = "%Y"
        week_format = "%W"
        day_of_week_format = "%w"
    elif date_standard == "ISO_8601":
        year_format = "%G"
        week_format = "%V"
        day_of_week_format = "%u"
    else:
        raise ValueError(f"Invalid `date_standard`='{date_standard}'")
    # Determine format and original `date` modification function.
    format_ = ""
    if date[:4].isdigit():
        format_ += year_format
    elif date[0] == "Q" and len(date) == 7 and date[-4:].isdigit():
        # "Q1 2020" format.

        def modify_quarterly_data(x: str) -> str:
            year_number = x[-4:]
            quarter = int(x[1:2])
            last_month_of_quarter = 3 * quarter
            last_day_of_quarter = calendar.monthrange(
                int(year_number), last_month_of_quarter
            )[1]
            modified_x = (
                f"{year_number}-{last_month_of_quarter}-{last_day_of_quarter}"
            )
            return modified_x

        format_ = f"{year_format}-%m-%d"
        return format_, modify_quarterly_data
    else:
        _LOG.error("This format is not supported: '%s'", date)
        return
    next_char = date[4]
    # TODO(Julia): Support `["-", ".", "/", " "]` separators.
    if next_char == "-":
        if len(date) not in [7, 8]:
            _LOG.error("This format is not supported: '%s'", date)
            return
        format_ += next_char
        next_char = date[5]
        if next_char == "W":
            # "2020-W14" format.

            def modify_weekly_date(x: str) -> str:
                return x + "-6"

            date_modification_func = modify_weekly_date
            format_ += f"W{week_format}-{day_of_week_format}"
        elif next_char == "S":
            # "2020-S1" - semi-annual format.
            def modify_semiannual_date(x: str) -> str:
                return x.replace("S1", "06-30").replace("S2", "12-31")

            date_modification_func = modify_semiannual_date
            format_ += "%m-%d"
        elif next_char == "B":
            # "2020-B1" - bi-monthly format (every other month).
            # We'll index by the start of the month starting with January,
            # but let's check PiT.

            def modify_bimonthly_date(x: str) -> str:
                bimonth_number = x[6]
                month_number = int(bimonth_number) * 2 - 1
                modified_x = f"{x[:5]}{month_number}-01"
                return modified_x

            date_modification_func = modify_bimonthly_date
            format_ += "%m-%d"
        else:
            _LOG.error("This format is not supported: '%s'", date)
            return
    elif next_char == "M":
        # TODO(Julia): Check for string length.
        # "1959M01" format.

        def modify_monthly_date(x: str) -> str:
            year_number = int(x[:4])
            month_number = x[5:]
            num_days_in_month = calendar.monthrange(
                year_number, int(month_number)
            )[1]
            modified_x = f"{x[:4]}-{month_number}-{num_days_in_month}"
            return modified_x

        date_modification_func = modify_monthly_date
        format_ += "-%m-%d"
    else:
        _LOG.error("This format is not supported: '%s'", date)
        return
    return format_, date_modification_func
