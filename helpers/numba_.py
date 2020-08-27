import logging
from typing import Callable

try:
    import numba

    numba_available = True
except ImportError:
    numba_available = False

_LOG = logging.getLogger(__name__)

# Switch to enable numba at run-time.
# For using in notebooks you need to force a reload of the library, like:
#   import importlib
#   importlib.reload(numba_)
#   numba_.USE_NUMBA = False

USE_NUMBA = True


def jit(f: Callable) -> Callable:

    if USE_NUMBA and not numba_available:
        _LOG.warning("numba is not installed")
    use_numba = USE_NUMBA and numba_available

    if use_numba:
        _LOG.debug("Using numba!")
        wrapper = numba.jit(f)
    else:

        def wrapper(*args, **kwargs):
            _LOG.debug("Not using numba!")
            return f(*args, **kwargs)

    return wrapper
