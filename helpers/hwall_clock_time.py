"""
Import as:

import helpers.hwall_clock_time as hwacltim
"""

# This should have no dependencies besides Python standard libraries since it's used
# in `helpers/hlogging.py`.

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
