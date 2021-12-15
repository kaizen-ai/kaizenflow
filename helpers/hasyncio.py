"""
Import as:

import helpers.hasyncio as hasynci
"""
import asyncio
import contextlib
import datetime
import logging
import math
import random
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Iterator,
    Optional,
    Tuple,
    Union,
    cast,
)

import async_solipsism
import pandas as pd

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# #############################################################################
# Wrappers around `asyncio` to switch among true and simulated real-time loops.
# #############################################################################


# TODO(gp): We could make this a mixin and add this behavior to both asyncio and
#  async_solipsism event loop.
# TODO(gp): -> _AsyncSolipsismEventLoop
# TODO(gp): Consider injecting a `get_wall_clock_time: hdatetim.GetWallClockTime`
#  in the event loop so we can simplify the interfaces. An event loop always needs
#  a function to get the wall clock.
class _EventLoop(async_solipsism.EventLoop):
    """
    An `async_solipsism.EventLoop` returning also the wall-clock time.
    """

    # TODO(gp): If we pass an `initial_replayed_dt` we could incorporate here also
    #  the replayed time approach and can remove `ReplayedTime` object.
    def __init__(self) -> None:
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


async def gather_coroutines_with_wall_clock(
    event_loop: asyncio.AbstractEventLoop, *coroutines: List[Coroutine]
) -> List[Any]:
    """
    Inject a wall clock associated to `event_loop` in all the coroutines and then
    gathers them in a single coroutine.
    """
    get_wall_clock_time = lambda: hdateti.get_current_time(
        tz="ET", event_loop=event_loop
    )
    # Construct the coroutines here by passing the `get_wall_clock_time()`
    # function.
    coroutines = [coro(get_wall_clock_time) for coro in coroutines]
    #
    result = await asyncio.gather(*coroutines)
    return result


# TODO(gp): For some reason `asyncio.run()` doesn't seem to pick up the new event
#  loop. So we use a re-implementation of `run` that does that.
def run(
    coroutine: Coroutine,
    event_loop: Optional[asyncio.AbstractEventLoop],
    close_event_loop: bool = True,
) -> Any:
    """
    `asyncio.run()` wrapper that allows to use a specified `EventLoop`.

    :param coroutine: the coroutine to run
    :param event_loop: the event loop to use. `None` means the standard `asyncio`
        event loop
    :param close_event_loop: if False the event loop is not closed, so that we can
        run multiple times in the same event loop
    :return: same output of `run_until_complete()`
    """
    if event_loop is None:
        # Use a normal `asyncio` EventLoop.
        event_loop = asyncio.new_event_loop()
    hdbg.dassert_issubclass(event_loop, asyncio.AbstractEventLoop)
    try:
        ret = event_loop.run_until_complete(coroutine)
    finally:
        if close_event_loop:
            event_loop.close()
    return ret


# #############################################################################
# Asynchronous polling.
# #############################################################################


# The result of a polling function in terms of a bool indicating success (which
# when True stops the polling) and a result.
PollOutput = Tuple[bool, Any]
# A polling function accepts any inputs and needs to return a `PollOutput`
# in terms of (success, result). Typically polling functions don't accept any inputs
# and are built through lambdas and closures.
PollingFunction = Callable[[Any], PollOutput]


async def poll(
    polling_func: PollingFunction,
    sleep_in_secs: float,
    timeout_in_secs: float,
    get_wall_clock_time: hdateti.GetWallClockTime,
) -> Tuple[int, Any]:
    """
    Call `polling_func` every `sleep_in_secs` until `polling_func` returns
    success or there is a timeout, if no success is achieved within
    `timeout_in_secs`.

    :param polling_func: function returning a tuple (success, value)
    :return:
        - number of iterations before a successful call to `polling_func`
        - result from `polling_func`
    :raises: TimeoutError in case of timeout
    """
    _LOG.debug(hprint.to_str("polling_func sleep_in_secs timeout_in_secs"))
    hdbg.dassert_lt(0, sleep_in_secs)
    hdbg.dassert_lt(0, timeout_in_secs)
    max_num_iter = math.ceil(timeout_in_secs / sleep_in_secs)
    hdbg.dassert_lte(1, max_num_iter)
    num_iter = 1
    while True:
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "# Iter %s/%s: wall clock time=%s"
                % (num_iter, max_num_iter, get_wall_clock_time()),
                char1="<",
            ),
        )
        # Poll.
        success, value = polling_func()
        _LOG.debug("success=%s, value=%s", success, value)
        # If success, then exit.
        if success:
            # The function returned.
            _LOG.debug(
                "poll done: wall clock time=%s",
                get_wall_clock_time(),
            )
            return num_iter, value
        # Otherwise update state.
        num_iter += 1
        if num_iter > max_num_iter:
            msg = "Timeout for " + hprint.to_str(
                "polling_func sleep_in_secs timeout_in_secs"
            )
            _LOG.error(msg)
            raise TimeoutError(msg)
        _LOG.debug("sleep for %s secs", sleep_in_secs)
        await asyncio.sleep(sleep_in_secs)


def get_poll_kwargs(
    get_wall_clock_time: hdateti.GetWallClockTime,
    *,
    sleep_in_secs: float = 1.0,
    timeout_in_secs: float = 10.0,
) -> Dict[str, Any]:
    # TODO(gp): Add checks.
    poll_kwargs = {
        "sleep_in_secs": sleep_in_secs,
        "timeout_in_secs": timeout_in_secs,
        "get_wall_clock_time": get_wall_clock_time,
    }
    return poll_kwargs


# #############################################################################
# Wait.
# #############################################################################


# Represent a deterministic, if float, or random delay in [a, b] if a Tuple.
# All values are in seconds.
WaitInSecs = Union[float, Tuple[float, float]]


async def wait(
    delay_in_secs: WaitInSecs,
    # TODO(gp): How to handle random seed here?
    seed: int = 42,
) -> None:
    """
    Wait a deterministic or a randomized delay.
    """
    # Extract or compute the delay.
    if isinstance(delay_in_secs, float):
        pass
    elif isinstance(delay_in_secs, tuple):
        hdbg.dassert_eq(len(delay_in_secs), 2)
        min_, max_ = delay_in_secs
        hdbg.dassert_lte(0, min_)
        hdbg.dassert_lte(min_, max_)
        delay_in_secs = random.rand(min_, max_)
    else:
        raise ValueError(f"Invalid delay_in_secs='delay_in_secs'")
    # Wait.
    hdbg.dassert_lte(0, delay_in_secs)
    delay_in_secs = cast(float, delay_in_secs)
    # TODO(gp): -> use asyncio.sleep()
    await asyncio.wait(delay_in_secs)


async def wait_until(
    timestamp: pd.Timestamp,
    get_wall_clock_time: hdateti.GetWallClockTime,
) -> None:
    """
    Wait until the wall clock time is `timestamp`.
    """
    curr_timestamp = get_wall_clock_time()
    # We only wait for times in the future.
    hdbg.dassert_lte(curr_timestamp, timestamp)
    #
    time_in_secs = (curr_timestamp - timestamp).seconds
    hdbg.dassert_lte(0, time_in_secs)
    asyncio.sleep(time_in_secs)
