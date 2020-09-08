import logging
from typing import Any, Tuple

import helpers.cache as hcac
import helpers.io_ as io_
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)

# #############################################################################


def _get_add_function():
    """Return function for testing with the ability to track state."""
    #
    def fn(x, y):
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


# All unit tests for the cache should be in this class since we have a single
# disk cache. Therefore if we used different classes and the unit tests were
# executed in parallel we would incur in race conditions for unit tests all
# resetting / using the same disk cache.
class Test_cache2(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cache_tag = "%s::%s" % (
            self.__class__.__name__,
            self._testMethodName,
        )

    def tearDown(self) -> None:
        # TODO(gp): Use a context manager to create / destroy a local cache.
        # For now we do it explicitly.
        disk_cache_path = hcac.get_cache_path("disk", self.cache_tag)
        _LOG.debug("Destroying disk_cache_path=%s", disk_cache_path)
        io_.delete_dir(disk_cache_path)
        memory_cache_path = hcac.get_cache_path("mem", self.cache_tag)
        _LOG.debug("Destroying memory_cache_path=%s", memory_cache_path)
        io_.delete_dir(memory_cache_path)
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

    def _get_function(self) -> _get_add_function:
        """Build a function that can be used to verify if it was executed or
        not."""
        f = _get_add_function()
        # Make sure the function starts from a non-executed state.
        self.assertFalse(f.executed)
        return f

    def _reset_function(self, f) -> None:
        """Reset the function before another execution, so we can verify if it
        was executed or not.

        We should do this every time we run the cached version of the
        function.
        """
        f.executed = False
        self.assertFalse(f.executed)

    def _check_cache_state(
        self,
        f: _get_function,
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

    def _get_f_cf_functions(
        self, **kwargs: Any
    ) -> Tuple[_get_function, hcac.Cached]:
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
        hcac.reset_disk_cache(self.cache_tag)
        cf._reset_cache_tracing()
        return f, cf
