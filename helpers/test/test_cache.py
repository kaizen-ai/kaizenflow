import logging
import time
from typing import Any, Callable, Tuple

import numpy as np
import pandas as pd

import helpers.cache as hcac
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# #############################################################################


def _get_add_function() -> Callable:
    """Return function for testing with the ability to track state."""
    #
    def fn(x: int, y: int) -> int:
        fn.executed = True
        return x + y

    #
    fn.executed = False
    #
    return fn


class Test_cache1(hut.TestCase):
    def test1(self) -> None:
        """Cache unit tests need to clean up the cache, so we need to make sure
        we are using the unit test cache, and not the dev cache."""
        cache_tag = "unittest"
        disk_cache_name = hcac.get_cache_name("disk", cache_tag)
        _LOG.debug("disk_cache_name=%s", disk_cache_name)
        self.assertIn(cache_tag, disk_cache_name)


class Test_cache2(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cache_tag = "%s::%s" % (
            self.__class__.__name__,
            self._testMethodName,
        )

    def tearDown(self) -> None:
        hcac.destroy_cache("disk", tag=self.cache_tag)
        hcac.destroy_cache("mem", tag=self.cache_tag)
        #
        super().tearDown()

    def test_without_caching1(self) -> None:
        """Test that we get two executions of a function, if we execute two
        times, without caching."""
        f = self._get_function()
        # Execute.
        act = f(3, 4)
        self.assertEqual(act, 7)
        # Check that the function was executed.
        self.assertTrue(f.executed)
        # Execute again.
        self._reset_function(f)
        act = f(3, 4)
        self.assertEqual(act, 7)
        # Check that the function is executed again, since there is no caching.
        self.assertTrue(f.executed)

    def test_with_caching1(self) -> None:
        """Test that caching the same value works."""
        f, cf = self._get_f_cf_functions()
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Execute the third time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 3rd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching2(self) -> None:
        """Test that caching mixing different values works."""
        f, cf = self._get_f_cf_functions()
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching3(self) -> None:
        """Test disabling both mem and disk cache."""
        f, cf = self._get_f_cf_functions(
            use_mem_cache=False, use_disk_cache=False
        )
        # Execute the first time.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_with_caching4(self) -> None:
        """Test that caching mixing different values works, when we disable the
        disk cache."""
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=False)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching5(self) -> None:
        """Test that caching mixing different values works, when we disable the
        memory cache."""
        f, cf = self._get_f_cf_functions(use_mem_cache=False, use_disk_cache=True)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="disk"
        )

    def test_with_caching_mem_reset(self) -> None:
        """Test resetting mem cache."""
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=False)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the first time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        # Reset memory cache.
        cf.clear_cache("mem")
        # Execute the 3rd time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 3rd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_with_caching_disk_reset(self) -> None:
        """Test resetting disk cache."""
        f, cf = self._get_f_cf_functions(use_mem_cache=False, use_disk_cache=True)
        # Execute the first time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the first time: verify that it is *NOT* executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        # Reset disk cache.
        cf.clear_cache("disk")
        # Execute the 3rd time: verify that it is executed.
        _LOG.debug("\n%s", prnt.frame("Executing the 3rd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_redefined_function(self) -> None:
        """Test if the function is redefined, but it's still the same, the
        intrinsic function should not be recomputed."""
        # Define the function inline imitating working in a notebook.
        add = self._get_add_function()
        cached_add = hcac.Cached(add, tag=self.cache_tag)
        # Execute the first time.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use memory cache.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Redefine the function inline.
        add = self._get_add_function()
        cached_add = hcac.Cached(add, tag=self.cache_tag)
        # Execute the third time. Should still use memory cache.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Execute the fourth time. Should still use memory cache.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Check that call with other arguments miss the cache.
        self._check_cache_state(
            add, cached_add, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_changed_function(self) -> None:
        """Test if the function is redefined, but it is not the same, the
        intrinsic function should be recomputed."""
        # Define the function inline imitating working in a notebook.
        def add(x: int, y: int) -> int:
            add.executed = True
            return x + y

        cached_add = hcac.Cached(add, tag=self.cache_tag)
        # Execute the first time.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use memory cache.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Redefine the function inline. Change body.

        # pylint: disable=function-redefined
        def add(x: int, y: int) -> int:
            add.executed = True
            z = x + y
            return z

        cached_add = hcac.Cached(add, tag=self.cache_tag)
        # Execute the third time. Should still use memory cache.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the fourth time. Should still use memory cache.
        self._check_cache_state(
            add, cached_add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )
        # Check that call with other arguments miss the cache.
        self._check_cache_state(
            add, cached_add, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    @staticmethod
    def _get_add_function() -> Callable:
        def add(x: int, y: int) -> int:
            add.executed = True
            return x + y

        return add

    def _get_function(self) -> Callable:
        """Build a function that can be used to verify if it was executed or
        not."""
        f = _get_add_function()
        # Make sure the function starts from a non-executed state.
        self.assertFalse(f.executed)
        return f

    def _reset_function(self, f: Callable) -> None:
        """Reset the function before another execution, so we can verify if it
        was executed or not.

        We should do this every time we run the cached version of the
        function.
        """
        f.executed = False
        self.assertFalse(f.executed)

    def _check_cache_state(
        self,
        f: Callable,
        cf: hcac.Cached,
        val1: int,
        val2: int,
        exp_f_state: bool,
        exp_cf_state: str,
    ) -> None:
        """Call the (cached function) `cf(val1, val2)` and check whether the
        intrinsic function was executed and what caches were used."""
        _LOG.debug(
            "val1=%s, val2=%s, exp_f_state=%s, exp_cf_state=%s",
            val1,
            val2,
            exp_f_state,
            exp_cf_state,
        )
        # We reset the function since we want to verify if it was called or not,
        # when calling the cached function.
        self._reset_function(f)
        # Call the cached function.
        act = cf(val1, val2)
        exp = val1 + val2
        self.assertEqual(act, exp)
        # Check which function was executed and what caches were used.
        _LOG.debug("get_last_cache_accessed=%s", cf.get_last_cache_accessed())
        self.assertEqual(exp_cf_state, cf.get_last_cache_accessed())
        _LOG.debug("executed=%s", f.executed)
        self.assertEqual(exp_f_state, f.executed)

    def _get_f_cf_functions(self, **kwargs: Any) -> Tuple[Callable, hcac.Cached]:
        """Create the intrinsic function `f` and its cached version `cf`."""
        # Make sure that we are using the unit test cache.
        disk_cache_name = hcac.get_cache_name("disk", self.cache_tag)
        _LOG.debug("disk_cache_name=%s", disk_cache_name)
        _LOG.debug(
            "disk_cache_path=%s", hcac.get_cache_path("disk", self.cache_tag)
        )
        # Create the intrinsic function.
        f = self._get_function()
        # Create the cached function.
        cf = hcac.Cached(f, tag=self.cache_tag, **kwargs)
        # Reset everything and check that it's in the expected state.
        hcac.reset_cache("disk", self.cache_tag)
        hcac.reset_cache("mem", self.cache_tag)
        cf._reset_cache_tracing()
        return f, cf


class Test_cache_performance(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cache_tag = "%s::%s" % (
            self.__class__.__name__,
            self._testMethodName,
        )

    def tearDown(self) -> None:
        hcac.destroy_cache("disk", tag=self.cache_tag)
        hcac.destroy_cache("mem", tag=self.cache_tag)
        #
        super().tearDown()

    def test_performance_dataframe(self) -> None:
        """Test performance of the cache over pandas DataFrame."""
        # Create a somewhat big DataFrame with random data.
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )
        print("testing pandas dataframe, with sample size", df.shape)
        self._test_performance(df)

    def test_performance_series(self) -> None:
        """Test performance of the cache over pandas Series."""
        # Create a somewhat big DataFrame with random data.
        s = pd.Series(np.random.randint(0, 100, size=100))
        print("testing pandas series, with sample size", s.shape)
        self._test_performance(s)

    def _test_performance(self, val: Any) -> None:
        """Test performance of the cache over some argument val.

        :param val: any hashable argument
        """
        # Create cached versions of the computation function.
        _mem_cached_computation = hcac.Cached(
            self._computation,
            tag=self.cache_tag,
            use_mem_cache=True,
            use_disk_cache=False,
        )
        _disk_cached_computation = hcac.Cached(
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

    @staticmethod
    # pylint: disable=unused-argument
    def _computation(*args: Any) -> None:
        """Simulate work.

        :param args: throw away arguments
        """
        # Emulate small quantity of work.
        time.sleep(0.01)

    @staticmethod
    def _timeit(fn: Callable, *args: Any) -> float:
        """Get performance measure of the call to fn with args.

        :param fn: callable function
        :param args: any arguments to pass to the function fn
        :return: precise time in seconds
        """
        perf_start = time.perf_counter()
        fn(*args)
        perf_diff = time.perf_counter() - perf_start
        return perf_diff


class Test_decorator(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cache_tag = "%s::%s" % (
            self.__class__.__name__,
            self._testMethodName,
        )

    def tearDown(self) -> None:
        hcac.destroy_cache("disk", tag=self.cache_tag)
        hcac.destroy_cache("mem", tag=self.cache_tag)
        #
        super().tearDown()

    def test_decorated_function(self) -> None:
        """Test decorator with both caches enabled."""
        # Define the function inline imitating working in a notebook.
        @hcac.cache(tag=self.cache_tag)
        def add(x: int, y: int) -> int:
            add.__wrapped__.executed = True
            return x + y

        # Execute the first time.
        self._check_cache_state(
            add.__wrapped__, add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use memory cache.
        self._check_cache_state(
            add.__wrapped__, add, 1, 2, exp_f_state=False, exp_cf_state="mem"
        )

    def test_decorated_function_no_mem(self) -> None:
        """Test decorator with only disk cache."""
        # Define the function inline imitating working in a notebook.
        @hcac.cache(tag=self.cache_tag, use_mem_cache=False)
        def add(x: int, y: int) -> int:
            add.__wrapped__.executed = True
            return x + y

        # Execute the first time.
        self._check_cache_state(
            add.__wrapped__, add, 1, 2, exp_f_state=True, exp_cf_state="no_cache"
        )
        # Execute the second time. Must use disk cache.
        self._check_cache_state(
            add.__wrapped__, add, 1, 2, exp_f_state=False, exp_cf_state="disk"
        )

    def _check_cache_state(
        self,
        f: Callable,
        cf: hcac.Cached,
        val1: int,
        val2: int,
        exp_f_state: bool,
        exp_cf_state: str,
    ) -> None:
        """Call the (cached function) `cf(val1, val2)` and check whether the
        intrinsic function was executed and what caches were used."""
        _LOG.debug(
            "val1=%s, val2=%s, exp_f_state=%s, exp_cf_state=%s",
            val1,
            val2,
            exp_f_state,
            exp_cf_state,
        )
        # Reset the function.
        f.executed = False
        # Call the cached function.
        act = cf(val1, val2)
        exp = val1 + val2
        self.assertEqual(act, exp)
        # Check which function was executed and what caches were used.
        _LOG.debug("get_last_cache_accessed=%s", cf.get_last_cache_accessed())
        self.assertEqual(exp_cf_state, cf.get_last_cache_accessed())
        _LOG.debug("executed=%s", f.executed)
        self.assertEqual(exp_f_state, f.executed)
