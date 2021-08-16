import logging
import tempfile
import time
from typing import Any, Callable, Tuple

import numpy as np
import pandas as pd

import helpers.cache as hcache
import helpers.dbg as dbg
import helpers.git as git
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# #############################################################################


def _get_add_function() -> Callable:
    """
    Return a function with the ability to track state, used for testing.
    """

    def func(x: int, y: int) -> int:
        func.executed = True  # type: ignore[attr-defined]
        return x + y

    func.executed = False  # type: ignore[attr-defined]
    return func


def _reset_add_function(func: Callable) -> None:
    """
    Reset the function before another execution, so we can verify if it was
    executed or not.

    We should do this every time we run the cached version of the
    function.
    """
    func.executed = False  # type: ignore[attr-defined]
    dbg.dassert(not func.executed)  # type: ignore[attr-defined]


# #############################################################################


class _ResetGlobalCacheHelper(hut.TestCase):
    """
    Create a global cache for each test method and resets it at every test
    method invocation.
    """

    def setUp(self) -> None:
        super().setUp()
        # Create a tag like "TestCacheFeatures::test_without_caching1".
        self.cache_tag = "%s::%s" % (
            self.__class__.__name__,
            self._testMethodName,
        )
        # Clean all the caches before this test method is run.
        self._remove_all_caches()

    def tearDown(self) -> None:
        # Clean and remove all the caches after the test method is run.
        self._remove_all_caches()
        #
        super().tearDown()

    def _remove_all_caches(self) -> None:
        """
        Clean and remove all the caches for this test.
        """
        cache_type = "all"
        hcache.clear_global_cache(
            cache_type, tag=self.cache_tag, destroy=True
        )

    def _get_f_cf_functions(
        self, **cached_kwargs: Any
    ) -> Tuple[Callable, hcache.Cached]:
        """
        Create the intrinsic function `f` and its cached version `cf`.
        """
        # Make sure that we are using the unit test cache.
        # disk_cache_name = hcache._get_cache_name("disk", self.cache_tag)
        # _LOG.debug("disk_cache_name=%s", disk_cache_name)
        # _LOG.debug(
        #     "disk_cache_path=%s", hcache._get_cache_path("disk", self.cache_tag)
        # )

        # TODO(gp): Add an assertion.
        # Create the intrinsic function.
        f = _get_add_function()
        # Create the cached function.
        cf = hcache.Cached(f, tag=self.cache_tag, **cached_kwargs)
        # Reset all the caches.
        hcache.clear_global_cache("all", self.cache_tag)
        cf._reset_cache_tracing()
        return f, cf

    def _execute_and_check_state(
        self,
        f: Callable,
        cf: hcache.Cached,
        val1: int,
        val2: int,
        exp_f_state: bool,
        exp_cf_state: str,
    ) -> None:
        """
        Call the function `f(val1, val2) and its cached function `cf(val1,
        val2)` and check whether the intrinsic function was executed and what
        caches were used, according to `exp_f_state` and `exp_cf_state`.
        """
        _LOG.debug(
            "val1=%s, val2=%s, exp_f_state=%s, exp_cf_state=%s",
            val1,
            val2,
            exp_f_state,
            exp_cf_state,
        )
        # Reset the intrinsic function since we want to verify if it was called
        # or not when we call the cached function.
        _reset_add_function(f)
        # Call the cached function.
        act = cf(val1, val2)
        exp = val1 + val2
        # Check the result.
        self.assertEqual(act, exp)
        # Check which function was executed and what caches were used.
        _LOG.debug("f.executed='%s.", f.executed)
        self.assertEqual(f.executed, exp_f_state)  # type: ignore[attr-defined]
        _LOG.debug("cf.get_last_cache_accessed=%s", cf.get_last_cache_accessed())
        self.assertEqual(cf.get_last_cache_accessed(), exp_cf_state)


# #############################################################################


class TestCacheFunctions(hut.TestCase):
    def test_get_cache_name1(self) -> None:
        """
        Make sure we are using the unit test cache and not the development
        cache, by checking the name of the disk cache.
        """
        cache_tag = "unittest"
        disk_cache_name = hcache._get_cache_name("disk", cache_tag)
        _LOG.debug("disk_cache_name=%s", disk_cache_name)
        self.assertIn(cache_tag, disk_cache_name)


# #############################################################################


class TestGlobalCache1(_ResetGlobalCacheHelper):
    def test_without_caching1(self) -> None:
        """
        If we execute two times without caching, we get two executions of the
        intrinsic function.
        """
        f = _get_add_function()
        self.assertFalse(f.executed)  # type: ignore[attr-defined]
        # Execute.
        act = f(3, 4)
        self.assertEqual(act, 7)
        # Check that the function was executed.
        self.assertTrue(f.executed)  # type: ignore[attr-defined]
        # Reset.
        _reset_add_function(f)
        self.assertFalse(f.executed)  # type: ignore[attr-defined]
        # Execute again.
        act = f(3, 4)
        self.assertEqual(act, 7)
        # Check that the function is executed again, since there is no caching.
        self.assertTrue(f.executed)  # type: ignore[attr-defined]

    def test_with_caching1(self) -> None:
        """
        - Leave the caches enabled
        - Show that the memory cache is used
        """
        f, cf = self._get_f_cf_functions()
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Execute the third time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 3rd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching2(self) -> None:
        """
        - Leave the caches enabled
        - Cache different values
        """
        f, cf = self._get_f_cf_functions()
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Use a different workload.
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Use a different workload.
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching3(self) -> None:
        """
        - Disable both mem and disk cache.
        - Cache a single value
        """
        f, cf = self._get_f_cf_functions(
            use_mem_cache=False, use_disk_cache=False
        )
        # Execute the first time.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_with_caching4(self) -> None:
        """
        - Disable only the disk cache
        - Cache different values
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=False)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        #
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching5(self) -> None:
        """
        - Disable only the memory cache
        - Cache different values
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=False, use_disk_cache=True)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        #
        self._execute_and_check_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="disk"
        )

    # ////////////////////////////////////////////////////////////////////////////

    def test_with_caching_mem_reset(self) -> None:
        """
        - Use only the memory cache
        - Execute and cache
        - Reset the mem cache
        - Execute again
        - Check that the cached function is recomputed
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=False)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Reset memory cache.
        hcache.clear_global_cache("mem", self.cache_tag)
        # Execute the third time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 3rd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_with_caching_disk_reset(self) -> None:
        """
        Same as `test_with_caching_mem_reset()` but using the disk cache.
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=False, use_disk_cache=True)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        # Reset disk cache.
        hcache.clear_global_cache("disk", self.cache_tag)
        # Execute the third time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 3rd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_with_caching_mem_reset2(self) -> None:
        """
        - Use both caches
        - Execute and cache
        - Reset the mem cache
        - Execute again
        - Check that the cached value is found in the disk cache
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=True)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Reset memory cache.
        hcache.clear_global_cache("mem", self.cache_tag)
        # Execute the third time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 3rd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )

    # ////////////////////////////////////////////////////////////////////////////

    def test_redefined_function(self) -> None:
        """
        If the cached function is redefined, but it's still the same, then the
        intrinsic function should not be recomputed.
        """
        # Define the function inline imitating working in a notebook.
        add = _get_add_function()
        cached_add = hcache.Cached(add, tag=self.cache_tag)
        # Execute the first time.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use memory cache.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Redefine the function inline.
        add = _get_add_function()
        cached_add = hcache.Cached(add, tag=self.cache_tag)
        # Execute the third time. Should still use memory cache.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Execute the fourth time. Should still use memory cache.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Check that call with other arguments miss the cache.
        self._execute_and_check_state(
            add, cached_add, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_changed_function(self) -> None:
        """
        If the function is redefined, but the code is not the same, then the
        intrinsic function should be recomputed.
        """
        # Define the function imitating working in a notebook.
        def add(x: int, y: int) -> int:
            add.executed = True  # type: ignore[attr-defined]
            return x + y

        cached_add = hcache.Cached(add, tag=self.cache_tag)
        # Execute the first time.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use memory cache.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Redefine the function with different code.

        # pylint: disable=function-redefined
        def add(x: int, y: int) -> int:  # type: ignore[no-redef]
            add.executed = True
            z = x + y
            return z

        cached_add = hcache.Cached(add, tag=self.cache_tag)
        # Execute the third time. Should still use memory cache.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the fourth time. Should still use memory cache.
        self._execute_and_check_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Check that call with other arguments miss the cache.
        self._execute_and_check_state(
            add, cached_add, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )


# #############################################################################


class _ResetFunctionSpecificCacheHelper(_ResetGlobalCacheHelper):
    def setUp(self) -> None:
        super().setUp()
        # Create temp directories to store the cache.
        self.disk_cache_temp_dir = tempfile.mkdtemp()
        # Clear global cache.
        hcache.clear_global_cache("all", tag=self.cache_tag)

    # TODO(gp): Pass `disk_cache_path=self.disk_cache_temp_dir` and reuse the value.
    def _get_f_cf_functions(self, **cached_kwargs: Any
        ) -> Tuple[Callable, hcache.Cached]:
        """
        Create the intrinsic function `f` and its cached version `cf`.
        """
        # Create the intrinsic function.
        f = _get_add_function()
        # Create the cached function using the function specific cache.
        cf = hcache.Cached(
            f,
            disk_cache_path=self.disk_cache_temp_dir,
            tag=self.cache_tag,
            **cached_kwargs,
        )
        # Reset the caches.
        cf.clear_cache("all")
        cf._reset_cache_tracing()
        return f, cf


class TestFunctionSpecificCache1(_ResetFunctionSpecificCacheHelper):
    def test_with_caching1(self) -> None:
        """
        - Test using the function-specific disk cache
        - Disable function-specific cache and switching to global cache
        - Test using the global cache
        """
        # Use a global cache and
        _LOG.debug("\n%s", hprint.frame("Starting"))
        _LOG.debug("# get_global_cache_info()=\n%s", hcache.get_global_cache_info(tag=self.cache_tag))
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=True)
        _LOG.debug("# cf.get_info()=\n%s", cf.get_info())
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Clear the global cache.
        _LOG.debug("\n%s", hprint.frame("clear_global_cache"))
        hcache.clear_global_cache("all")
        # # Verify that function is executed with global cache.
        # _LOG.debug("\n%s", hprint.frame("Executing the 3rd time"))
        # self._execute_and_check_state(
        #     f, cf, 3, 4, exp_f_state=False, exp_cf_state="no_cache"
        # )
        # #
        # _LOG.debug("\n%s", hprint.frame("Executing the 4th time"))
        # self._execute_and_check_state(
        #     f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        # )
        # # Clear the global cache.
        # hcache.clear_global_cache("all")
        # # Verify that it is *NOT* executed with specific cache.
        # _LOG.debug("\n%s", hprint.frame("Executing the 5th time"))
        # self._execute_and_check_state(
        #     f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        # )

    def test_with_caching2(self) -> None:
        """
        - Test using the function-specific disk cache
        - Disable function-specific cache and switching to global cache
        - Test using the global cache
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=False)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Clear the global cache.
        hcache.clear_global_cache("all")
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 2nd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        # Use the global cache.
        cf.set_cache_path(None)
        # Verify that function is executed with global cache.
        _LOG.debug("\n%s", hprint.frame("Executing the 3rd time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        _LOG.debug("\n%s", hprint.frame("Executing the 4th time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        # Restore back specific cache.
        cf.set_cache_path(self.disk_cache_temp_dir)
        # Verify that it is *NOT* executed with specific cache.
        _LOG.debug("\n%s", hprint.frame("Executing the 5th time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )


# #############################################################################


class TestCachePerformance(_ResetGlobalCacheHelper):
    def test_performance_dataframe(self) -> None:
        """
        Test performance of the cache over pandas DataFrame.
        """
        # Create a somewhat big DataFrame with random data.
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        print("testing pandas dataframe, with sample size", df.shape)
        self._test_performance(df)

    def test_performance_series(self) -> None:
        """
        Test performance of the cache over pandas Series.
        """
        # Create a somewhat big DataFrame with random data.
        s = pd.Series(np.random.randint(0, 100, size=100))
        print("testing pandas series, with sample size", s.shape)
        self._test_performance(s)

    @staticmethod
    # pylint: disable=unused-argument
    def _computation(*args: Any) -> None:
        """
        Simulate work.

        :param args: throw away arguments
        """
        # Emulate small quantity of work.
        time.sleep(0.01)

    @staticmethod
    def _timeit(func: Callable, *args: Any) -> float:
        """
        Get performance measure of the call to fn with args.

        :param fn: callable function
        :param args: any arguments to pass to the function fn
        :return: precise time in seconds
        """
        perf_start = time.perf_counter()
        func(*args)
        perf_diff = time.perf_counter() - perf_start
        return perf_diff

    def _test_performance(self, val: Any) -> None:
        """
        Test performance of the cache over some argument val.

        :param val: any hashable argument
        """
        # Create cached versions of the computation function.
        _mem_cached_computation = hcache.Cached(
            self._computation,
            tag=self.cache_tag,
            use_mem_cache=True,
            use_disk_cache=False,
        )
        _disk_cached_computation = hcache.Cached(
            self._computation,
            tag=self.cache_tag,
            use_mem_cache=False,
            use_disk_cache=True,
        )
        # First step: no cache.
        no_cache_ct = self._timeit(lambda: self._computation(val))
        print("no cache run time=%f" % no_cache_ct)
        # Second step: memory cache.
        memory_no_cache_ct = self._timeit(lambda: _mem_cached_computation(val))
        print("empty memory cache run time=%f" % memory_no_cache_ct)
        print(
            "empty memory cache overhead=%f" % (memory_no_cache_ct - no_cache_ct)
        )
        memory_cache_ct = self._timeit(lambda: _mem_cached_computation(val))
        print("hot memory cache run time=%f" % memory_cache_ct)
        print("hot memory cache benefit=%f" % (no_cache_ct - memory_cache_ct))
        # Third step: disk cache.
        disk_no_cache_ct = self._timeit(lambda: _disk_cached_computation(val))
        print("empty disk cache run time=%f" % disk_no_cache_ct)
        print("empty disk cache overhead=%f" % (disk_no_cache_ct - no_cache_ct))
        disk_cache_ct = self._timeit(lambda: _disk_cached_computation(val))
        print("hot disk cache run time=%f" % disk_cache_ct)
        print("hot disk cache benefit=%f" % (no_cache_ct - disk_cache_ct))


# #############################################################################


class TestCacheDecorator(_ResetGlobalCacheHelper):
    def test_decorated_function(self) -> None:
        """
        Test decorator with both caches enabled.
        """
        # Define the function inline imitating working in a notebook.
        @hcache.cache(tag=self.cache_tag)
        def add(x: int, y: int) -> int:
            add.__wrapped__.executed = True
            return x + y

        # Execute the first time.
        self._execute_and_check_state(
            add.__wrapped__, add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use memory cache.
        self._execute_and_check_state(
            add.__wrapped__, add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )

    def test_decorated_function_no_mem(self) -> None:
        """
        Test decorator with only disk cache.
        """
        # Define the function inline imitating working in a notebook.
        @hcache.cache(tag=self.cache_tag, use_mem_cache=False)
        def add(x: int, y: int) -> int:
            add.__wrapped__.executed = True
            return x + y

        # Execute the first time.
        self._execute_and_check_state(
            add.__wrapped__, add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use disk cache.
        self._execute_and_check_state(
            add.__wrapped__, add, 1, 2, exp_f_state=False, exp_cf_state="disk"
        )


# #############################################################################


class TestAmpTask1407(_ResetGlobalCacheHelper):
    def test1(self) -> None:
        """
        A class method can't be cached.
        """

        class _AmpTask1407Class:
            def __init__(self, string: str) -> None:
                self._string = string

            @hcache.cache(tag=self.cache_tag)
            def print(self, n: int) -> str:
                string = ""
                for _ in range(n):
                    string += "hello" + ("o" * len(self._string)) + " "
                return string

        klass = _AmpTask1407Class("test")
        with self.assertRaises(ValueError):
            klass.print(5)

    def test2(self) -> None:
        """
        A static method can be cached.
        """

        class _AmpTask1407Class:
            def __init__(self, string: str) -> None:
                self._string = string

            @hcache.cache(tag=self.cache_tag)
            def print(self, n: int) -> str:
                string = ""
                for _ in range(n):
                    string += "hello" + ("o" * len(self._string)) + " "
                return string

            @staticmethod
            @hcache.cache(tag=self.cache_tag)
            def static_print(n: int) -> str:
                print("--> hello: ", n)
                string = ""
                for _ in range(n):
                    string += "hello" + ("o" * len("world")) + " "
                return string

        klass = _AmpTask1407Class("test")
        klass.static_print(5)
        self.assertEqual(klass.static_print.get_last_cache_accessed(), "no_cache")
        #
        klass.static_print(5)
        self.assertEqual(klass.static_print.get_last_cache_accessed(), "mem")
        klass.static_print(5)
        self.assertEqual(klass.static_print.get_last_cache_accessed(), "mem")
        #
        klass.static_print(6)
        self.assertEqual(klass.static_print.get_last_cache_accessed(), "no_cache")
        klass.static_print(6)
        self.assertEqual(klass.static_print.get_last_cache_accessed(), "mem")


# #############################################################################


class TestCachingOnS3(_ResetFunctionSpecificCacheHelper):

    def setUp(self) -> None:
        super().setUp()
        # Get a directory to store the cache on S3.
        self.disk_cache_dir = self.get_s3_scratch_dir()
        # Clear global cache.
        hcache.clear_global_cache("all", tag=self.cache_tag)

    def test_with_caching1(self) -> None:
        """
        - Test using the function-specific cache
        - Disable function-specific cache and switching to global cache
        - Test using the global cache
        """
        f, cf = self._get_f_cf_functions()
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", hprint.frame("Executing the 1st time"))
        self._execute_and_check_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )



# TODO(gp): Add a test for verbose mode in __call__
# TODO(gp): get_info
