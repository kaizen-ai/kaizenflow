"""
Wrappers around `asyncio` to allow to switch true and simulated real-time
loops.

Import as:

import helpers.hasyncio as hhasynci
"""
import asyncio
import contextlib
import datetime
from typing import Any, Coroutine, Iterator, Optional

import async_solipsism

import helpers.dbg as hdbg


# TODO(gp): We could make this a mixin and add this behavior to both asyncio and
#  async_solipsism event loop.
class _EventLoop(async_solipsism.EventLoop):
    """
    An `async_solipsism.EventLoop` returning also the wall-clock time.
    """

    def __init__(self):
        super().__init__()
        self._initial_dt = datetime.datetime.utcnow()

    def get_current_time(self) -> datetime.datetime:
        # `loop.time()` returns the number of seconds as `float` from when the event
        # loop was created.
        num_secs = super().time()
        return self._initial_dt + datetime.timedelta(seconds=num_secs)


# From https://stackoverflow.com/questions/49555991
@contextlib.contextmanager
def solipsism_context() -> Iterator:
    """
    Context manager to isolate an `asyncio_solipsism` event loop.
    """
    # Use the variation of solipsistic `EventLoop` above.
    event_loop = _EventLoop()
    asyncio.set_event_loop(event_loop)
    try:
        yield event_loop
    finally:
        asyncio.set_event_loop(None)


# TODO(gp): For some reason `asyncio.run()` doesn't seem to pick up the new event
#  loop. So we use a re-implementation of `run` that does that.
def run(
    coroutine: Coroutine,
    *,
    event_loop: Optional[asyncio.AbstractEventLoop] = None
) -> Any:
    """
    `asyncio.run()` wrapper that allows to use a specified `EventLoop`.

    :param coroutine: the coroutine to run
    :param event_loop: the event loop to use. `None` means the standard `asyncio`
        event loop
    :return: same output of `run_until_complete()`
    """
    if event_loop is None:
        # Use a normal `asyncio` EventLoop.
        event_loop = asyncio.new_event_loop()
    hdbg.dassert_issubclass(event_loop, asyncio.AbstractEventLoop)
    try:
        ret = event_loop.run_until_complete(coroutine)
    finally:
        event_loop.close()
    return ret
