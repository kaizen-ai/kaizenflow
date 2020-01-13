#!/usr/bin/env python
"""
Import as:

import helpers.cache as hcac
"""

import argparse
import functools
import logging
import os
from typing import Any, Callable

import joblib

import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################

_USE_CACHING: bool = True


def set_caching(val: bool) -> None:
    global _USE_CACHING
    _LOG.warning("Setting caching to %s -> %s", _USE_CACHING, val)
    _USE_CACHING = val


def is_caching_enabled() -> bool:
    return _USE_CACHING


def get_disk_cache_name() -> str:
    if ut.in_unit_test_mode():
        cache_name = "tmp.joblib.unittest.cache"
    else:
        cache_name = "tmp.joblib.cache"
    return cache_name


def get_disk_cache_path() -> str:
    cache_name = get_disk_cache_name()
    file_name = os.path.join(git.get_client_root(super_module=True), cache_name)
    file_name = os.path.abspath(file_name)
    return file_name


# This is the global disk cache.
_DISK_CACHE = None


def get_disk_cache() -> Any:
    """
    Return the object storing the disk cache.
    """
    _LOG.debug("get_disk_cache")
    global _DISK_CACHE
    if not _DISK_CACHE:
        file_name = get_disk_cache_path()
        _DISK_CACHE = joblib.Memory(file_name, verbose=0, compress=1)
    return _DISK_CACHE


def reset_disk_cache() -> None:
    _LOG.warning("Resetting disk cache '%s'", get_disk_cache_path())
    get_disk_cache().clear(warn=True)


class Cached:
    """
    Decorator wrapping a function in a disk and memory cache.

    If the function value was not cached either in memory or on disk, the
    function `f` is executed and the value is stored.

    The decorator uses 2 levels of caching:
        - disk cache: useful for retrieving the state among different
          executions or when one does a "Reset" of a notebook;
        - memory cache: useful for multiple execution in notebooks, without
          resetting the state.
    """

    def __init__(
        self,
        func: Callable,
        use_mem_cache: bool = True,
        use_disk_cache: bool = True,
    ):
        # This is used to make the class have the same attributes (e.g.,
        # `__name__`, `__doc__`, `__dict__`) as the called function.
        functools.update_wrapper(self, func)
        self._func = func
        self._use_mem_cache = use_mem_cache
        self._use_disk_cache = use_disk_cache
        self._reset_cache_tracing()
        # Create decorated functions with different caches and store pointers
        # of these functions. Note that we need to build the functions in the
        # constructor since we need to have a single instance of the decorated
        # functions. On the other side, e.g., if we created these functions in
        # `__call__`, they will be recreated at every invocation, creating a
        # new memory cache at every invocation.
        # Create the disk cache object, if needed.
        _ = get_disk_cache()

        @_DISK_CACHE.cache
        def _execute_func_from_disk_cache(*args: Any, **kwargs: Any) -> Any:
            # If we get here, we didn't hit neither memory nor the disk cache.
            self._last_used_disk_cache = False
            _LOG.debug(
                "%s(args=%s kwargs=%s): execute the intrinsic function",
                self._func.__name__,
                args,
                kwargs,
            )
            obj = self._func(*args, **kwargs)
            return obj

        @functools.lru_cache(maxsize=None)
        def _execute_func_from_mem_cache(*args: Any, **kwargs: Any) -> Any:
            _LOG.debug(
                "%s: use_mem_cache=%s use_disk_cache=%s",
                self._func.__name__,
                self._use_mem_cache,
                self._use_disk_cache,
            )
            # If we get here, we know that we didn't hit the memory cache,
            # but we don't know about the disk cache.
            if self._use_mem_cache:
                self._last_used_mem_cache = False
                _LOG.debug(
                    "%s(args=%s kwargs=%s): trying to read from disk",
                    self._func.__name__,
                    args,
                    kwargs,
                )
                obj = _execute_func_from_disk_cache(*args, **kwargs)
            else:
                _LOG.debug("Skipping disk cache")
                obj = self._func(*args, **kwargs)
            return obj

        def _execute_func(*args: Any, **kwargs: Any) -> Any:
            _LOG.debug(
                "%s: use_mem_cache=%s use_disk_cache=%s",
                self._func.__name__,
                self._use_mem_cache,
                self._use_disk_cache,
            )
            if self._use_mem_cache:
                _LOG.debug(
                    "%s(args=%s kwargs=%s): trying to read from memory: %s",
                    self._func.__name__,
                    args,
                    kwargs,
                    _execute_func_from_mem_cache.cache_info(),
                )
                obj = _execute_func_from_mem_cache(*args, **kwargs)
            else:
                _LOG.debug("Skipping memory cache")
                self._last_used_mem_cache = False
                if self._use_disk_cache:
                    obj = _execute_func_from_disk_cache(*args, **kwargs)
                else:
                    _LOG.debug("Skipping disk cache")
                    self._last_used_disk_cache = False
                    obj = self._func(*args, **kwargs)
            return obj

        self._execute_func = _execute_func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not is_caching_enabled():
            _LOG.debug("Caching is disabled")
            self._last_used_disk_cache = self._last_used_mem_cache = False
            obj = self._func(*args, **kwargs)
        else:
            self._reset_cache_tracing()
            obj = self._execute_func(*args, **kwargs)
        return obj

    def get_last_cache_accessed(self) -> str:
        if self._last_used_disk_cache and self._last_used_mem_cache:
            # We executed `read_data` -> we hit the mem cache.
            return "mem"
        elif self._last_used_disk_cache and not self._last_used_mem_cache:
            # We executed `_read_data_from_mem_cache` -> we hit the disk cache.
            return "disk"
        elif not self._last_used_disk_cache and not self._last_used_mem_cache:
            # We executed the function -> we didn't hit any cache.
            return "no_cache"
        else:
            # We hit the disk cache but not the
            dbg.dassert(not self._last_used_disk_cache)
            dbg.dassert(self._last_used_mem_cache)
            return "disk"

    def _reset_cache_tracing(self) -> None:
        """
        Reset the values used to track which cache we are hitting when executing
        the cached function.
        """
        # Note that we can only know when a cache doesn't trigger, since
        # otherwise the memory / disk caching decorator will intercept the call
        # and avoid our code to be executed. For this reason, if we are
        # interested in tracing caching, we need to set the values to True and
        # then have the decorator set it to False.
        self._last_used_disk_cache = True
        self._last_used_mem_cache = True


# #############################################################################

# TODO(gp): Move to a different file, e.g., manage_cache.py


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("positional", nargs=1, choices=["reset_cache"])
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    action = args.positional[0]
    if action == "reset_cache":
        reset_disk_cache()
    else:
        dbg.dfatal("Invalid action='%s'" % action)


if __name__ == "__main__":
    _main(_parse())
