"""
Import as:

import helpers.timer as htimer
"""

import logging
import time
from typing import Any, Callable, Optional, Tuple, cast

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)

# #############################################################################


class Timer:
    """
    Measure time elapsed in one or more intervals.
    """

    def __init__(self, start_on_creation: bool = True):
        """
        Create a timer.

        If "start_on_creation" is True start automatically the timer.
        """
        self._stop: Optional[float] = None
        # Store the time for the last elapsed interval.
        self._last_elapsed: Optional[float] = None
        # Store the total time for all the measured intervals.
        self._total_elapsed = 0.0
        if start_on_creation:
            # For better accuracy start the timer as last action, after all the
            # bookkeeping.
            self._start: Optional[float] = time.time()
        else:
            self._start = None

    def __repr__(self) -> str:
        """
        Return string with the intervals measured so far.
        """
        measured_time = self._total_elapsed
        if self.is_started() and not self.is_stopped():
            # Timer still running.
            measured_time += time.time() - cast(float, self._start)
        ret = "%.3f secs" % measured_time
        return ret

    def stop(self) -> None:
        """
        Stop the timer and accumulate the interval.
        """
        # Timer must have not been stopped before.
        hdbg.dassert(self.is_started() and not self.is_stopped())
        # For better accuracy stop the timer as first action.
        self._stop = time.time()
        # Update the total elapsed time.
        # Sometimes we get numerical error tripping this assertion
        # (e.g., '1619552498.813126' <= '1619552498.805193') so we give
        # a little slack to the assertion.
        # hdbg.dassert_lte(self._start, self._stop + 1e-2)
        self._last_elapsed = cast(float, self._stop) - cast(float, self._start)
        self._total_elapsed += self._last_elapsed
        # Stop.
        self._start = None
        self._stop = None

    def get_elapsed(self) -> float:
        """
        Stop if not stopped already, and return the elapsed time.
        """
        if not self.is_stopped():
            self.stop()
        hdbg.dassert_is_not(self._last_elapsed, None)
        return cast(float, self._last_elapsed)

    # /////////////////////////////////////////////////////////////////////////

    def resume(self) -> None:
        """
        Resume the timer after a stop.
        """
        # Timer must have been stopped before.
        hdbg.dassert(self.is_started() or self.is_stopped())
        self._stop = None
        # Start last for better accuracy.
        self._start = time.time()

    def is_started(self) -> bool:
        return self._start is not None and self._start >= 0 and self._stop is None

    def is_stopped(self) -> bool:
        return self._start is None and self._stop is None

    def get_total_elapsed(self) -> float:
        """
        Stop if not stopped already, and return the total elapsed time.
        """
        if not self.is_stopped():
            self.stop()
        return self._total_elapsed

    def accumulate(self, timer: "Timer") -> None:
        """
        Accumulate the value of a timer to the current object.
        """
        # Both timers must be stopped.
        hdbg.dassert(timer.is_stopped())
        hdbg.dassert(self.is_stopped())
        hdbg.dassert_lte(0.0, timer.get_total_elapsed())
        self._total_elapsed += timer.get_total_elapsed()


# #############################################################################


_TimerMemento = Tuple[int, str, Timer]


def dtimer_start(log_level: int, message: str) -> _TimerMemento:
    """
    :return: memento of the timer.
    """
    _LOG.log(log_level, "%s ...", message)
    memento = log_level, message, Timer()
    return memento


def dtimer_stop(memento: _TimerMemento) -> Tuple[str, float]:
    """
    :return:
      - message as as string
      - time in seconds (int)
    """
    log_level, message, timer = memento
    timer.stop()
    elapsed_time = round(timer.get_elapsed(), 3)
    msg = "%s done (%.3f s)" % (message, elapsed_time)
    _LOG.log(log_level, msg)
    return msg, elapsed_time


# TODO(gp): Is this useful / used?
def stop_timer(timer: Timer) -> str:
    timer.stop()
    elapsed_time = round(timer.get_elapsed(), 3)
    msg = "%.3f s" % elapsed_time
    return msg


# #############################################################################
# Context manager.
# #############################################################################


class TimedScope:
    """
    Measure the execution time of a block of code.

    ```
    with htimer.TimedScope(logging.INFO, "Work") as ts:
        ... work work work ...
    ```
    """

    def __init__(self, log_level: int, message: str):
        self._log_level = log_level
        self._message = message
        self._memento: Optional[_TimerMemento] = None
        self.elapsed_time = None

    def __enter__(self) -> "TimedScope":
        self._memento = dtimer_start(self._log_level, self._message)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._idx is not None:
            msg, self.elapsed_time = dtimer_stop(self._memento)
            _ = msg


# #############################################################################
# Decorator.
# #############################################################################


def timed(f: Callable) -> Callable:
    """
    Decorator adding a timer around the invocation of a function.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        func_name = f.__name__
        #
        timer = dtimer_start(0, func_name)
        v = f(*args, **kwargs)
        dtimer_stop(timer)
        return v

    return wrapper


# #############################################################################

# TODO(gp): Add an object that accumulates the times from multiple timers.
#  E.g., use a dict for message -> time
