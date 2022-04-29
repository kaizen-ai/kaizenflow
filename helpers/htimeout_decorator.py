#!/usr/bin/env python
"""
`exit_after` decorator which is used to limit function execution time.

KeyboardInterrupt will be raised if time limit is exceed. Import as:

Import as:

import helpers.htimeout_decorator as htimdeco
"""

import _thread
import sys
import threading
from typing import Any


def timeout_handler() -> None:
    sys.stderr.flush()
    # Raise KeyboardInterrupt.
    _thread.interrupt_main()


def exit_after(timeout_sec: int) -> Any:
    """
    Exit process if function execution takes longer than s seconds.

    :param timeout_sec: time limit
    """

    def outer(fn: Any) -> Any:
        def inner(*args: Any, **kwargs: Any) -> Any:
            timer = threading.Timer(timeout_sec, timeout_handler)
            timer.start()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer.cancel()
            return result

        return inner

    return outer
