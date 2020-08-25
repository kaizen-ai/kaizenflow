import logging
import time
from typing import Any, Dict

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

# #############################################################################


class Timer:
    """Measure time elapsed in one or more intervals."""

    def __init__(self, start_on_creation=True):
        """Create a timer.

        If "start_on_creation" is True start automatically the timer.
        """
        self._stop = None
        # Store the time for the last elapsed interval.
        self._last_elapsed = None
        # Store the total time for all the measured intervals.
        self._total_elapsed = 0.0
        if start_on_creation:
            # For better accuracy start the timer as last action, after all the
            # bookkeeping.
            self._start = time.time()
        else:
            self._start = None

    def stop(self):
        """Stop the timer and accumulate the interval."""
        # Timer must have not been stopped before.
        dbg.dassert(self.is_started() and not self.is_stopped())
        # For better accuracy stop the timer as first action.
        self._stop = time.time()
        # Update the total elapsed time.
        dbg.dassert_lte(self._start, self._stop)
        self._last_elapsed = self._stop - self._start
        self._total_elapsed += self._last_elapsed
        # Stop.
        self._start = None
        self._stop = None

    def get_elapsed(self):
        """Stop if not stopped already, and return the elapsed time."""
        if not self.is_stopped():
            self.stop()
        dbg.dassert_is_not(self._last_elapsed, None)
        return self._last_elapsed

    def __repr__(self):
        """Return string with the intervals measured so far."""
        measured_time = self._total_elapsed
        if self.is_started() and not self.is_stopped():
            # Timer still running.
            measured_time += time.time() - self._start
        ret = "%.3f secs" % measured_time
        return ret

    # /////////////////////////////////////////////////////////////////////////

    def resume(self):
        """Resume the timer after a stop."""
        # Timer must have been stopped before.
        dbg.dassert(self.is_started() or self.is_stopped())
        self._stop = None
        # Start last for better accuracy.
        self._start = time.time()

    def is_started(self):
        return self._start >= 0 and self._stop is None

    def is_stopped(self):
        return self._start is None and self._stop is None

    def get_total_elapsed(self):
        """Stop if not stopped already, and return the total elapsed time."""
        if not self.is_stopped():
            self.stop()
        return self._total_elapsed

    def accumulate(self, timer):
        """Accumulate the value of a timer to the current object."""
        # Both timers must be stopped.
        dbg.dassert(timer.is_stopped())
        dbg.dassert(self.is_stopped())
        dbg.dassert_lte(0.0, timer.get_total_elapsed())
        self._total_elapsed += timer.get_total_elapsed()


# #############################################################################

_DTIMER_INFO: Dict[int, Any] = {}


def dtimer_start(log_level, message):
    """
    - return: memento of the timer.
    """
    _LOG.log(log_level, "%s ...", message)
    idx = len(_DTIMER_INFO)
    info = log_level, message, Timer()
    _DTIMER_INFO[idx] = info
    return idx


def stop_timer(timer):
    timer.stop()
    elapsed_time = round(timer.get_elapsed(), 3)
    msg = "%.3f s" % elapsed_time
    return msg


def dtimer_stop(idx):
    """
    - return:
      - message as as string
      - time in seconds (int)
    """
    dbg.dassert_lte(0, idx)
    dbg.dassert_lt(idx, len(_DTIMER_INFO))
    log_level, message, timer = _DTIMER_INFO[idx]
    timer.stop()
    elapsed_time = round(timer.get_elapsed(), 3)
    msg = "%s done (%.3f s)" % (message, elapsed_time)
    del _DTIMER_INFO[idx]
    _LOG.log(log_level, msg)
    return msg, elapsed_time


class TimedScope:
    def __init__(self, log_level, message):
        self._log_level = log_level
        self._message = message
        self._idx = None

    def __enter__(self):
        self._idx = dtimer_start(self._log_level, self._message)
        return self

    def __exit__(self, *args):
        dtimer_stop(self._idx)


# Decorator.
def timed(f):
    def wrapper(*args, **kwargs):
        # if hasattr(f, "__name__"):
        func_name = f.__name__
        # else:
        #    func_name = dbg.get_function_name()
        timer = dtimer_start(0, func_name)
        v = f(*args, **kwargs)
        dtimer_stop(timer)
        return v

    return wrapper
