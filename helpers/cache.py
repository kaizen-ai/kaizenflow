"""Import as:

import helpers.cache as hcac
"""

import copy
import functools
import logging
import os
import time
from typing import Any, Callable, Optional

import joblib

import helpers.dbg as dbg
import helpers.git as git
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

_USE_CACHING: bool = True
# This is the global disk cache.
_DISK_CACHE: Any = None
# This is the global memory cache.
_MEMORY_CACHE: Any = None
_MEMORY_TMPFS_PATH = "/mnt/tmpfs"
# Log level for information about the high level behavior of the caching
# layer.
_LOG_LEVEL = logging.DEBUG

# #############################################################################


def set_caching(val: bool) -> None:
    global _USE_CACHING
    _LOG.warning("Setting caching to %s -> %s", _USE_CACHING, val)
    _USE_CACHING = val


def is_caching_enabled() -> bool:
    return _USE_CACHING


def get_disk_cache_name(tag: Optional[str]) -> str:
    if hut.in_unit_test_mode():
        cache_name = "tmp.joblib.unittest.cache"
        if tag is not None:
            cache_name += f".{tag}"
    else:
        dbg.dassert_is(tag, None)
        cache_name = "tmp.joblib.cache"
    return cache_name


def get_memory_cache_name(tag: Optional[str]) -> str:
    if hut.in_unit_test_mode():
        cache_name = "tmp.joblib.unittest.memory.cache"
        if tag is not None:
            cache_name += f".{tag}"
    else:
        dbg.dassert_is(tag, None)
        cache_name = "tmp.joblib.memory.cache"
    return cache_name


def get_disk_cache_path(tag: Optional[str]) -> str:
    cache_name = get_disk_cache_name(tag)
    file_name = os.path.join(git.get_client_root(super_module=True), cache_name)
    file_name = os.path.abspath(file_name)
    return file_name


def get_memory_cache_path(tag: Optional[str]) -> str:
    cache_name = get_memory_cache_name(tag)
    file_name = os.path.join(_MEMORY_TMPFS_PATH, cache_name)
    file_name = os.path.abspath(file_name)
    return file_name


def get_disk_cache(tag: Optional[str]) -> Any:
    """Return the object storing the disk cache."""
    _LOG.debug("get_disk_cache")
    if tag is None:
        global _DISK_CACHE
        if not _DISK_CACHE:
            file_name = get_disk_cache_path(tag)
            _DISK_CACHE = joblib.Memory(file_name, verbose=0, compress=1)
        disk_cache = _DISK_CACHE
    else:
        # Build a one-off cache.
        file_name = get_disk_cache_path(tag)
        disk_cache = joblib.Memory(file_name, verbose=0, compress=1)
    return disk_cache


def get_memory_cache(tag: Optional[str]) -> Any:
    """Return the object storing the memory cache."""
    _LOG.debug("get_memory_cache")
    if tag is None:
        global _MEMORY_CACHE
        if not _MEMORY_CACHE:
            file_name = get_memory_cache_path(tag)
            _MEMORY_CACHE = joblib.Memory(file_name, verbose=0, compress=1)
        memory_cache = _MEMORY_CACHE
    else:
        # Build a one-off cache.
        file_name = get_memory_cache_path(tag)
        memory_cache = joblib.Memory(file_name, verbose=0, compress=1)
    return memory_cache


def reset_disk_cache(tag: Optional[str]) -> None:
    _LOG.warning("Resetting disk cache '%s'", get_disk_cache_path(tag))
    disk_cache = get_disk_cache(tag)
    disk_cache.clear(warn=True)


class Cached:
    """Decorator wrapping a function in a disk and memory cache.

    If the function value was not cached either in memory or on disk, the
    function `f` is executed and the value is stored.

    The decorator uses 2 levels of caching:
    - disk cache: useful for retrieving the state among different executions or
      when one does a "Reset" of a notebook;
    - memory cache: useful for multiple execution in notebooks, without
      resetting the state.
    """

    def __init__(
        self,
        func: Callable,
        use_mem_cache: bool = True,
        use_disk_cache: bool = True,
        set_verbose_mode: bool = True,
        tag: Optional[str] = None,
    ):
        # This is used to make the class have the same attributes (e.g.,
        # `__name__`, `__doc__`, `__dict__`) as the called function.
        functools.update_wrapper(self, func)
        self._func = func
        self._use_mem_cache = use_mem_cache
        self._use_disk_cache = use_disk_cache
        self._set_verbose_mode = set_verbose_mode
        self._tag = tag
        self._reset_cache_tracing()
        # Create decorated functions with different caches and store pointers
        # of these functions. Note that we need to build the functions in the
        # constructor since we need to have a single instance of the decorated
        # functions. On the other side, e.g., if we created these functions in
        # `__call__`, they will be recreated at every invocation, creating a
        # new memory cache at every invocation.
        # Create the disk cache object, if needed.
        self._disk_cache = get_disk_cache(tag)
        self._memory_cache = get_memory_cache(tag)
        self._disk_cached_func = self._disk_cache.cache(self._func)
        self._memory_cached_func = self._memory_cache.cache(self._func)

        def _execute_func_from_disk_cache(*args: Any, **kwargs: Any) -> Any:
            func_id, args_id = self._get_memory_identifiers(*args, **kwargs)
            if not self._has_disk_cached_version(func_id, args_id):
                # If we get here, we didn't hit neither memory nor the disk cache.
                self._last_used_disk_cache = False
                _LOG.debug(
                    "%s(args=%s kwargs=%s): execute the intrinsic function",
                    self._func.__name__,
                    args,
                    kwargs,
                )
            obj = self._disk_cached_func(*args, **kwargs)
            return obj

        self._execute_func_from_disk_cache = _execute_func_from_disk_cache

        def _execute_func_from_mem_cache(*args: Any, **kwargs: Any) -> Any:
            func_id, args_id = self._get_memory_identifiers(*args, **kwargs)
            _LOG.debug(
                "%s: use_mem_cache=%s use_disk_cache=%s",
                self._func.__name__,
                self._use_mem_cache,
                self._use_disk_cache,
            )
            if self._has_memory_cached_version(func_id, args_id):
                obj = self._memory_cached_func(*args, **kwargs)
            else:
                # If we get here, we know that we didn't hit the memory cache,
                # but we don't know about the disk cache.
                self._last_used_mem_cache = False
                #
                if self._use_disk_cache:
                    _LOG.debug(
                        "%s(args=%s kwargs=%s): trying to read from disk",
                        self._func.__name__,
                        args,
                        kwargs,
                    )
                    obj = _execute_func_from_disk_cache(*args, **kwargs)
                else:
                    _LOG.warning("Skipping disk cache")
                    obj = self._memory_cached_func(*args, **kwargs)
                self._store_memory_cached_version(func_id, args_id, obj)
            return obj

        self._execute_func_from_mem_cache = _execute_func_from_mem_cache

        def _execute_func(*args: Any, **kwargs: Any) -> Any:
            _LOG.debug(
                "%s: use_mem_cache=%s use_disk_cache=%s",
                self._func.__name__,
                self._use_mem_cache,
                self._use_disk_cache,
            )
            if self._use_mem_cache:
                _LOG.debug(
                    "%s(args=%s kwargs=%s): trying to read from memory",
                    self._func.__name__,
                    args,
                    kwargs,
                )
                obj = _execute_func_from_mem_cache(*args, **kwargs)
            else:
                _LOG.warning("Skipping memory cache")
                if self._use_disk_cache:
                    obj = _execute_func_from_disk_cache(*args, **kwargs)
                else:
                    _LOG.warning("Skipping disk cache")
                    obj = self._func(*args, **kwargs)
            return obj

        self._execute_func = _execute_func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._set_verbose_mode:
            perf_counter_start = time.perf_counter()
        if not is_caching_enabled():
            _LOG.warning("Caching is disabled")
            self._last_used_disk_cache = self._last_used_mem_cache = False
            obj = self._func(*args, **kwargs)
        else:
            self._reset_cache_tracing()
            obj = self._execute_func(*args, **kwargs)
            _LOG.log(
                _LOG_LEVEL,
                "%s: executed from '%s'",
                self._func.__name__,
                self.get_last_cache_accessed(),
            )
            # TODO(gp): We make a copy, but we should do something better
            # (PartTask1071).
            obj = copy.deepcopy(obj)
        if self._set_verbose_mode:
            perf_counter = time.perf_counter() - perf_counter_start
            _LOG.info(
                "data was retrieved from %s in %f sec",
                self.get_last_cache_accessed(),
                perf_counter,
            )
        return obj

    def clear_memory_cache(self) -> None:
        _LOG.warning("%s: clearing memory cache", self._func.__name__)
        self._memory_cached_func.clear()

    def clear_disk_cache(self) -> None:
        _LOG.warning("%s: clearing disk cache", self._func.__name__)
        self._disk_cached_func.clear()

    def clear_all_cache(self) -> None:
        self.clear_memory_cache()
        self.clear_disk_cache()

    def get_last_cache_accessed(self) -> str:
        if self._last_used_mem_cache:
            ret = "mem"
        elif self._last_used_disk_cache:
            ret = "disk"
        else:
            ret = "no_cache"
        return ret

    def _get_memory_identifiers(self, *args, **kwargs):
        func_id, args_id = self._memory_cached_func._get_output_identifiers(
            *args, **kwargs
        )
        return func_id, args_id

    def _get_disk_identifiers(self, *args, **kwargs):
        func_id, args_id = self._memory_cached_func._get_output_identifiers(
            *args, **kwargs
        )
        return func_id, args_id

    def _has_memory_cached_version(self, func_id, args_id):
        has_memory_cached_version = self._memory_cached_func.store_backend.contains_item(
            [func_id, args_id]
        )
        return has_memory_cached_version

    def _has_disk_cached_version(self, func_id, args_id):
        has_disk_cached_version = self._disk_cached_func.store_backend.contains_item(
            [func_id, args_id]
        )
        return has_disk_cached_version

    def _store_memory_cached_version(self, func_id, args_id, obj):
        self._memory_cached_func.store_backend.dump_item([func_id, args_id], obj)

    def _reset_cache_tracing(self) -> None:
        """Reset the values used to track which cache we are hitting when
        executing the cached function."""
        # Note that we can only know when a cache doesn't trigger, since
        # otherwise the memory / disk caching decorator will intercept the call
        # and avoid our code to be executed. For this reason, if we are
        # interested in tracing caching, we need to set the values to True and
        # then have the decorator set it to False.
        self._last_used_disk_cache = self._use_disk_cache
        self._last_used_mem_cache = self._use_mem_cache
