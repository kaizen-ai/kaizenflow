"""
Import as:

import helpers.hwall_clock_time as hwacltim
"""

# This should have no dependencies besides Python standard libraries since it's used
# in `helpers/hlogging.py`.

import datetime
from typing import Callable, Optional

# Copied from `helpers/hdatetime.py`
#
# Function returning the current (true, replayed, simulated) wall-clock time as a
# timestamp.
_GetWallClockTime = Callable[[], "pd.Timestamp"]

_get_wall_clock_time_func: Optional[_GetWallClockTime] = None


def set_wall_clock_time(get_wall_clock_time_func: _GetWallClockTime) -> None:
    """
    Set the global function to retrieve the wall clock time.
    """
    assert callable(get_wall_clock_time_func)
    global _get_wall_clock_time_func
    _get_wall_clock_time_func = get_wall_clock_time_func


def get_wall_clock_time_func() -> Optional[_GetWallClockTime]:
    """
    Retrieve the global function retrieve the wall clock time.
    """
    return _get_wall_clock_time_func


# We don't want to import Pandas just for a type.
def get_wall_clock_time() -> Optional["pd.Timestamp"]:
    """
    Return the wall clock time (according to the set function) or `None` if no
    function was set.
    """
    func = _get_wall_clock_time_func
    if func is None:
        timestamp = None
    else:
        timestamp = func()
    return timestamp


# This is redundant with `hdatetime.get_current_time()` and
# `hdateti.get_current_timestamp_as_string()` but we keep them to simplify
# dependencies.
def get_machine_wall_clock_time() -> datetime.datetime:
    curr_timestamp = datetime.datetime.utcnow()
    return curr_timestamp


def get_machine_wall_clock_time_as_str() -> str:
    curr_timestamp = get_machine_wall_clock_time()
    curr_timestamp_as_str = curr_timestamp.strftime("%Y%m%d_%H%M%S")
    return curr_timestamp_as_str
