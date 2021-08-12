"""
Wrappers around `asyncio` to allow to switch true and simulated real-time loops.
"""
import asyncio
import datetime
import contextlib
from typing import Any, Iterator

import async_solipsism

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
    loop = _EventLoop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        asyncio.set_event_loop(None)


# # TODO(gp): For some reason `asyncio.run()` doesn't seem to pick up the set event
# #  loop. So we use a re-implementation of `run` that does that.
# def run(coroutine, loop: Optional[asyncio.AbstractEventLoop] = None) -> Any:
#     """
#     `asyncio.run()` wrapper that allows to use a specified `EventLoop`.
#     """
#     if loop is None:
#         # Use a normal `asyncio` EventLoop.
#         loop = asyncio.new_event_loop()
#     dbg.dassert_issubclass(loop, asyncio.AbstractEventLoop)
#     try:
#         ret = loop.run_until_complete(coroutine)
#     finally:
#         loop.close()
#     return ret
