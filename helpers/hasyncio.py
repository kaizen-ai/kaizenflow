import asyncio
import contextlib
from typing import Any, Iterator

import async_solipsism


def run(coroutine, loop) -> Any:
    if loop is None:
        loop = asyncio.new_event_loop()
    try:
        ret = loop.run_until_complete(coroutine)
    finally:
        loop.close()
    return ret


# From https://stackoverflow.com/questions/49555991
@contextlib.contextmanager
def solipsism_context() -> Iterator:
    """
    Context manager to isolate an `asyncio_solipsism` event loop.
    """
    loop = async_solipsism.EventLoop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        asyncio.set_event_loop(None)
