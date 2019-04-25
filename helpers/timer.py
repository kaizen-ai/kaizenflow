import logging
import time

import helpers.dbg as dbg

_log = logging.getLogger(__name__)

# #############################################################################


class Timer(object):
    """
    Measure time elapsed in one or more intervals.
    """

    def __init__(self, startOnCreation=True):
        """
        Create a timer. If "startOnCreation" is True start automatically the
        timer.
        """
        self._stop = None
        # Store the time for the last elapsed interval.
        self._lastElapsed = None
        # Store the total time for all the measured intervals.
        self._totalElapsed = 0.0
        if startOnCreation:
            # For better accuracy start the timer as last action, after all the
            # bookkeeping.
            self._start = time.time()
        else:
            self._start = None

    def stop(self):
        """
        Stop the timer and accumulate the interval.
        """
        # Timer must have not been stopped before.
        dbg.dassert(self.is_started() and not self.is_stopped())
        # For better accuracy stop the timer as first action.
        self._stop = time.time()
        # Update the total elapsed time.
        dbg.dassert_lte(self._start, self._stop)
        self._lastElapsed = self._stop - self._start
        self._totalElapsed += self._lastElapsed
        # Stop.
        self._start = None
        self._stop = None

    def get_elapsed(self):
        """
        Stop if not stopped already, and return the elapsed time.
        """
        if not self.is_stopped():
            self.stop()
        dbg.dassert_is_not(self._lastElapsed, None)
        return self._lastElapsed

    def __repr__(self):
        """
        Return string with the intervals measured so far.
        """
        measuredTime = self._totalElapsed
        if self.is_started() and not self.is_stopped():
            # Timer still running.
            measuredTime += time.time() - self._start
        ret = "%.3f secs" % measuredTime
        return ret

    # /////////////////////////////////////////////////////////////////////////

    def resume(self):
        """
        Resume the timer after a stop.
        """
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
        """
        Stop if not stopped already, and return the total elapsed time.
        """
        if not self.is_stopped():
            self.stop()
        return self._totalElapsed

    def accumulate(self, timer):
        """
        Accumulate the value of a timer to the current object.
        """
        # Both timers must be stopped.
        dbg.dassert(timer.is_stopped())
        dbg.dassert(self.is_stopped())
        dbg.dassert_lte(0.0, timer.get_total_elapsed())
        self._totalElapsed += timer.get_total_elapsed()


# #############################################################################

_DtimerInfo = {}


def dtimer_start(level, message):
    """
    - return: memento of the timer.
    """
    _log.info("%s ...", message)
    idx = len(_DtimerInfo)
    info = level, message, Timer()
    _DtimerInfo[idx] = info
    return idx


def stop_timer(timer):
    timer.stop()
    elapsedTime = round(timer.get_elapsed(), 3)
    msg = "%.3f s" % elapsedTime
    return msg


def dtimer_stop(idx):
    """
    - return:
      - message as as string
      - time in seconds (int)
    """
    dbg.dassert_lte(0, idx)
    dbg.dassert_lt(idx, len(_DtimerInfo))
    level, message, timer = _DtimerInfo[idx]
    timer.stop()
    elapsedTime = round(timer.get_elapsed(), 3)
    msg = "%s done (%.3f s)" % (message, elapsedTime)
    del _DtimerInfo[idx]
    _log.info(msg)
    return msg, elapsedTime


class TimedScope(object):

    def __init__(self, level, message):
        self._level = level
        self._message = message

    def __enter__(self):
        self._idx = dtimer_start(self._level, self._message)
        return self

    def __exit__(self, *args):
        dtimer_stop(self._idx)


# Decorator.
def timed(f):

    def wrapper(*args, **kwargs):
        #if hasattr(f, "__name__"):
        func_name = f.__name__
        #else:
        #    func_name = dbg.get_function_name()
        timer = dtimer_start(0, func_name)
        v = f(*args, **kwargs)
        dtimer_stop(timer)
        return v

    return wrapper
