"""Import as:

import helpers.cache as hcac
"""

import copy
import functools
import logging
import os
import time
from typing import Any, Callable, Optional, Tuple

import joblib
import joblib.func_inspect as jfi

import helpers.dbg as dbg
import helpers.git as git

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


def get_cache_name(cache_type: str, tag: Optional[str]) -> str:
    cache_name = "tmp.joblib"
    cache_name += f".{cache_type}"
    if tag is not None:
        cache_name += f".{tag}"
    cache_type += ".cache"
    return cache_name


def get_cache_path(cache_type: str, tag: Optional[str]) -> str:
    cache_name = get_cache_name(cache_type, tag)
    file_name = os.path.join(git.get_client_root(super_module=True), cache_name)
    file_name = os.path.abspath(file_name)
    return file_name


def get_disk_cache(tag: Optional[str]) -> Any:
    """Return the object storing the disk cache."""
    _LOG.debug("get_disk_cache")
    if tag is None:
        global _DISK_CACHE
        if not _DISK_CACHE:
            file_name = get_cache_path("disk", tag)
            _DISK_CACHE = joblib.Memory(file_name, verbose=0, compress=1)
        disk_cache = _DISK_CACHE
    else:
        # Build a one-off cache.
        file_name = get_cache_path("disk", tag)
        disk_cache = joblib.Memory(file_name, verbose=0, compress=1)
    return disk_cache


def get_memory_cache(tag: Optional[str]) -> Any:
    """Return the object storing the memory cache."""
    _LOG.debug("get_memory_cache")
    if tag is None:
        global _MEMORY_CACHE
        if not _MEMORY_CACHE:
            file_name = get_cache_path("mem", tag)
            _MEMORY_CACHE = joblib.Memory(file_name, verbose=0, compress=1)
        memory_cache = _MEMORY_CACHE
    else:
        # Build a one-off cache.
        file_name = get_cache_path("mem", tag)
        memory_cache = joblib.Memory(file_name, verbose=0, compress=1)
    return memory_cache


def reset_disk_cache(tag: Optional[str]) -> None:
    _LOG.warning("Resetting disk cache '%s'", get_cache_path("disk", tag))
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
        # Create the disk and mem cache object, if needed.
        self._disk_cache = get_disk_cache(tag)
        self._memory_cache = get_memory_cache(tag)
        # Create the functions decorated with the caching layer.
        self._disk_cached_func = self._disk_cache.cache(self._func)
        self._memory_cached_func = self._memory_cache.cache(self._func)

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

    def clear_cache(self, cache_type: Optional[str] = None) -> None:
        """Clear all cache, or a cache by type.

        :param cache_type: type of a cache to clear, or None to clear all caches
        """
        if cache_type is None:
            disk_cache = self._get_cache("disk")
            disk_cache.clear()
            mem_cache = self._get_cache("mem")
            mem_cache.clear()
        else:
            cache = self._get_cache(cache_type)
            cache.clear()

    def get_last_cache_accessed(self) -> str:
        """Get the last used cache in the latest call.

        :return: type of a cache used in the last call
        """
        if self._last_used_mem_cache:
            ret = "mem"
        elif self._last_used_disk_cache:
            ret = "disk"
        else:
            ret = "no_cache"
        return ret

    def _get_identifiers(
        self, cache_type: str, args: Any, kwargs: Any
    ) -> Tuple[str, str]:
        """Get digests for current function and arguments to be used in cache.

        :param cache_type: type of a cache
        :param args: original arguments of the call
        :param kwargs: original kw-arguments of the call
        :return: digests of the function and current arguments
        """
        cache = self._get_cache(cache_type)
        # pylint: disable=protected-access
        func_id, args_id = cache._get_output_identifiers(*args, **kwargs)
        return func_id, args_id

    def _get_cache(self, cache_type: str) -> joblib.MemorizedResult:
        """Get the instance of a cache by type.

        :param cache_type: type of a cache
        :return: instance of the cache from joblib
        """
        if cache_type == "mem":
            cache = self._memory_cached_func
        elif cache_type == "disk":
            cache = self._disk_cached_func
        else:
            dbg.dfatal("Unknown cache type: %s", cache_type)
        return cache

    def _has_cached_version(
        self, cache_type: str, func_id: str, args_id: str
    ) -> bool:
        """Check if cache contains entry for corresponding function and
        arguments digests.

        :param cache_type: type of a cache
        :param func_id: digest of the function obtained from _get_identifiers
        :param args_id: digest of arguments obtained from _get_identifiers
        :return: whether there is an entry
        """
        cache = self._get_cache(cache_type)
        has_cached_version = cache.store_backend.contains_item([func_id, args_id])
        return bool(has_cached_version)

    def _store_cached_version(
        self, cache_type: str, func_id: str, args_id: str, obj: Any
    ) -> None:
        """Store returned value from the intrinsic in the cache.

        :param cache_type: type of a cache
        :param func_id: digest of the function obtained from _get_identifiers
        :param args_id: digest of arguments obtained from _get_identifiers
        :param obj: return value of the intrinsic function
        """
        cache = self._get_cache(cache_type)
        # Write out function code to the cache.
        func_code, _, first_line = jfi.get_func_code(cache.func)
        # pylint: disable=protected-access
        cache._write_func_code(func_code, first_line)
        # Store the returned value into the cache.
        cache.store_backend.dump_item([func_id, args_id], obj)

    def _reset_cache_tracing(self) -> None:
        """Reset the values used to track which cache we are hitting when
        executing the cached function."""
        self._last_used_disk_cache = self._use_disk_cache
        self._last_used_mem_cache = self._use_mem_cache

    def _execute_func_from_disk_cache(self, *args: Any, **kwargs: Any) -> Any:
        func_id, args_id = self._get_identifiers("disk", args, kwargs)
        if not self._has_cached_version("disk", func_id, args_id):
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

    def _execute_func_from_mem_cache(self, *args: Any, **kwargs: Any) -> Any:
        func_id, args_id = self._get_identifiers("mem", args, kwargs)
        _LOG.debug(
            "%s: use_mem_cache=%s use_disk_cache=%s",
            self._func.__name__,
            self._use_mem_cache,
            self._use_disk_cache,
        )
        if self._has_cached_version("mem", func_id, args_id):
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
                obj = self._execute_func_from_disk_cache(*args, **kwargs)
            else:
                _LOG.warning("Skipping disk cache")
                obj = self._memory_cached_func(*args, **kwargs)
            self._store_cached_version("mem", func_id, args_id, obj)
        return obj

    def _execute_func(self, *args: Any, **kwargs: Any) -> Any:
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
            obj = self._execute_func_from_mem_cache(*args, **kwargs)
        else:
            _LOG.warning("Skipping memory cache")
            if self._use_disk_cache:
                obj = self._execute_func_from_disk_cache(*args, **kwargs)
            else:
                _LOG.warning("Skipping disk cache")
                obj = self._func(*args, **kwargs)
        return obj
