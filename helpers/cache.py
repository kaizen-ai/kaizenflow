#!/usr/bin/env python
"""
Import as:

import helpers.cache as cache

# Use as:
import functools

import helpers.cache as cache


def _read_data(*args, **kwargs):
    _LOG.info("Reading ...")
    ...
    return ...


MEMORY = cache.get_disk_cache()

@MEMORY.cache
def _read_data_from_disk(*args, **kwargs):
    _LOG.info("Reading from disk cache: %s %s", *args, **kwargs)
    data = _read_data(*args, **kwargs)
    return data


@functools.lru_cache(maxsize=None)
def read_data(*args, **kwargs):
    _LOG.info("Reading from mem cache: %s %s", *args, **kwargs)
    data = _read_data_from_disk(*args, **kwargs)
    return data
"""

# #############################################################################

import argparse
import logging
import os

import joblib

import helpers.dbg as dbg
import helpers.git as git
import helpers.parser as prsr

_LOG = logging.getLogger(__name__)


_MEMORY = None


_USE_CACHING : bool = True



def set_caching(val: bool):
    global _USE_CACHING
    _LOG.warning("Setting caching to %s -> %s", _USE_CACHING, val)
    _USE_CACHING = val


def is_caching_enabled() -> bool:
    return _USE_CACHING


# ##############################################################################

_MEMORY = None


def get_disk_cache_name() -> str:
    if hut.in_unit_test_mode():
        cache_name = "tmp.joblib.unittest.cache"
    else:
        cache_name = "tmp.joblib.cache"
    return cache_name


def get_disk_cache_path() -> str:
    cache_name = get_disk_cache_name()
    file_name = os.path.join(git.get_client_root(super_module=True), cache_name)
    file_name = os.path.abspath(file_name)
    return file_name


# TODO(gp): Add return type.
def get_disk_cache():
    """
    Return the object storing the disk cache.
    """
    _LOG.debug("get_disk_cache")
    global _MEMORY
    if not _MEMORY:
        file_name = get_disk_cache_path()
        _MEMORY = joblib.Memory(file_name, verbose=0, compress=1)
    return _MEMORY


def reset_disk_cache() -> None:
    _LOG.warning("Resetting disk cache '%s'", get_disk_cache_path())
    get_disk_cache().clear(warn=True)


def cached2(f):
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
    if not is_caching_enabled:
        _LOG.debug("Caching is disabled")
        return f

    memory = get_disk_cache()
    _LOG.debug("memory=%s", memory)

    @memory.cache
    def _read_data_from_disk_cache(*args, **kwargs):
        _LOG.debug(
            "%s args=%s kwargs=%s: reading from disk",
            f.__name__,
            str(args),
            str(kwargs),
        )
        obj = f(*args, **kwargs)
        return obj

    # TODO(gp): We loose the state. We need to put in the ctor.
    @functools.lru_cache(maxsize=None)
    def _read_data_from_mem_cache(*args, **kwargs):
        _LOG.debug(
            "%s args=%s kwargs=%s: reading from memory",
            f.__name__,
            str(args),
            str(kwargs),
        )
        obj = _read_data_from_disk_cache(*args, **kwargs)
        return obj

    def read_data(*args, **kwargs):
        _LOG.debug(
            "%s args=%s kwargs=%s: reading from memory: %s",
            f.__name__,
            str(args),
            str(kwargs),
            _read_data_from_mem_cache.cache_info()
        )
        obj = _read_data_from_mem_cache(*args, **kwargs)
        return obj

    return read_data


import functools


class cached:
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

    def __init__(self, func):
        # This is used to make the class have the same attributes (e.g.,
        # `__name__`, `__doc__`, `__dict__`) as the called function.
        functools.update_wrapper(self, func)
        self.func = func
        self.reset_cache_tracing()

    def reset_cache_tracing(self):
        """
        Reset the values used to track which cache we are hitting when executing
        the cached function.
        """
        # Note that we can only know when a cache doesn't trigger, since
        # otherwise the memory / disk caching decorator will intercept the call
        # and avoid our code to be executed. For this reason, if we are
        # interested in tracing caching, we need to set the values to True and
        # then have the decorator set it to False.
        self.last_used_disk_cache = True
        self.last_used_mem_cache = True

    def get_last_cache(self):
        if self.last_used_disk_cache and self.last_used_mem_cache:
            # We executed `read_data` -> we hit the mem cache.
            return "mem"
        elif self.last_used_disk_cache and not self.last_used_mem_cache:
            # We executed `_read_data_from_mem_cache` -> we hit the disk cache.
            return "disk"
        elif not self.last_used_disk_cache and not self.last_used_mem_cache:
            # We executed the function -> we didn't hit any cache.
            return "no_cache"
        else:
            # We hit the disk cache but not the
            dbg.dassert(not self.last_used_disk_cache)
            dbg.dassert(self.last_used_mem_cache)
            return "disk"

    def __call__(self, *args, **kwargs):
        if not is_caching_enabled:
            _LOG.debug("Caching is disabled")
            return self.func

        memory = get_disk_cache()
        _LOG.debug("memory=%s", memory)

        @memory.cache
        def _read_data_from_disk_cache(*args, **kwargs):
            # If we get here, we didn't hit either memory or disk cache.
            self.last_used_disk_cache = False
            _LOG.debug(
                "%s args=%s kwargs=%s: execute the function",
                self.func.__name__, str(args), str(kwargs),
            )
            obj = self.func(*args, **kwargs)
            return obj

        @functools.lru_cache(maxsize=None)
        def _read_data_from_mem_cache(*args, **kwargs):
            # If we get here, we know that we didn't hit the memory cache,
            # but we don't know about the disk cache.
            self.last_used_mem_cache = False
            _LOG.debug(
                "%s args=%s kwargs=%s: trying to read from disk",
                self.func.__name__, str(args), str(kwargs),
            )
            obj = _read_data_from_disk_cache(*args, **kwargs)
            return obj

        def read_data(*args, **kwargs):
            # TODO(gp): Call self.reset_cache_tracing() if not too convoluted.
            self.last_used_disk_cache = True
            self.last_used_mem_cache = True
            _LOG.debug(
                "%s args=%s kwargs=%s: trying to read from memory: %s",
                self.func.__name__, str(args), str(kwargs),
                _read_data_from_mem_cache.cache_info()
            )
            obj = _read_data_from_mem_cache(*args, **kwargs)
            return obj

        obj = read_data(*args, **kwargs)

        return obj

# ##############################################################################

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
