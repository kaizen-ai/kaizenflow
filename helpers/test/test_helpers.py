import logging
import os
from typing import Any, Optional, Tuple

import pytest

import helpers.cache as hcac
import helpers.env as env
import helpers.git as git
import helpers.list as hlist
import helpers.printing as prnt
import helpers.s3 as hs3
import helpers.system_interaction as si
import helpers.unit_test as ut
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)


# #############################################################################
# cache.py
# #############################################################################


class _Function:
    """
    Mimic a function through `__call__()` and use state to track if a function
    was executed or not.

    It is used in the unit tests for the caching framework in order to check
    if a function was actually executed or the cached version was used.
    """

    def __init__(self) -> None:
        self.__name__ = "_Function"
        self.reset()

    def reset(self) -> None:
        self.executed = False

    def __call__(self, x: int, y: int) -> int:
        self.executed = True
        return x + y


class Test_cache1(ut.TestCase):
    def test1(self) -> None:
        """
        Cache unit tests need to clean up the cache, so we need to make sure
        we are using the unit test cache, and not the dev cache.
        """
        cache_tag = None
        disk_cache_name = hcac.get_disk_cache_name(cache_tag)
        _LOG.debug("disk_cache_name=%s", disk_cache_name)
        self.assertIn("unittest", disk_cache_name)


# All unit tests for the cache should be in this class since we have a single
# disk cache. Therefore if we used different classes and the unit tests were
# executed in parallel we would incur in race conditions for unit tests all
# resetting / using the same disk cache.
class Test_cache2(ut.TestCase):
    def _get_cache_tag(self) -> str:
        return self.__class__.__name__

    def _get_function(self) -> _Function:
        """
        Build a function that can be used to verify if it was executed or not.
        """
        f = _Function()
        # Make sure the function starts from a non-executed state.
        self.assertFalse(f.executed)
        return f

    def _reset_function(self, f: _Function) -> None:
        """
        Reset the function before another execution, so we can verify if it was
        executed or not.

        We should do this every time we run the cached version of the function.
        """
        f.reset()
        self.assertFalse(f.executed)

    def test_without_caching1(self) -> None:
        """
        Test that we get two executions of a function, if we execute two times,
        without caching.
        """
        f = self._get_function()
        #
        # Execute.
        #
        act = f(3, 4)
        self.assertEqual(act, 7)
        # Check that the function was executed.
        self.assertTrue(f.executed)
        #
        # Execute again.
        #
        self._reset_function(f)
        act = f(3, 4)
        self.assertEqual(act, 7)
        # Check that the function is executed again, since there is no caching.
        self.assertTrue(f.executed)

    def _check_cache_state(
        self,
        f: _Function,
        cf: hcac.Cached,
        val1: int,
        val2: int,
        exp_f_state: bool,
        exp_cf_state: str,
    ) -> None:
        """
        Call the (cached function) `cf(val1, val2)` and check whether the
        intrinsic function was executed and what caches were used.
        """
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
        self.assertEqual(cf.get_last_cache_accessed(), exp_cf_state)
        _LOG.debug("executed=%s", f.executed)
        self.assertEqual(f.executed, exp_f_state)

    def _get_f_cf_functions(self, **kwargs: Any) -> Tuple[_Function, hcac.Cached]:
        """
        Create the intrinsic function `f` and its cached version `cf`.
        """
        # Make sure that we are using the unit test cache.
        cache_tag = self._get_cache_tag()
        disk_cache_name = hcac.get_disk_cache_name(cache_tag)
        _LOG.debug("disk_cache_name=%s", disk_cache_name)
        _LOG.debug("disk_cache_path=%s", hcac.get_disk_cache_path(cache_tag))
        self.assertIn("unittest", disk_cache_name)
        # Create the intrinsic function.
        f = self._get_function()
        # Create the cached function.
        cf = hcac.Cached(f, tag=cache_tag, **kwargs)
        # Reset everything and check that it's in the expected state.
        hcac.reset_disk_cache(cache_tag)
        cf._reset_cache_tracing()
        return f, cf

    def test_with_caching1(self) -> None:
        """
        Test that caching the same value works.
        """
        f, cf = self._get_f_cf_functions()
        #
        # Execute the first time: verify that it is executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        # Execute the second time: verify that it is *NOT* executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        #
        # Execute the third time: verify that it is *NOT* executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 3rd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching2(self) -> None:
        """
        Test that caching mixing different values works.
        """
        f, cf = self._get_f_cf_functions()
        #
        # Execute the first time: verify that it is executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        # Execute the second time: verify that it is *NOT* executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching3(self) -> None:
        """
        Test disabling both mem and disk cache.
        """
        f, cf = self._get_f_cf_functions(
            use_mem_cache=False, use_disk_cache=False
        )
        #
        # Execute the first time.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        # Execute the second time.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )

    def test_with_caching4(self) -> None:
        """
        Test that caching mixing different values works, when we disable the
        disk cache.
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=True, use_disk_cache=False)

        #
        # Execute the first time: verify that it is executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        # Execute the second time: verify that it is *NOT* executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="mem"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="mem"
        )

    def test_with_caching5(self) -> None:
        """
        Test that caching mixing different values works, when we disable the
        memory cache.
        """
        f, cf = self._get_f_cf_functions(use_mem_cache=False, use_disk_cache=True)

        #
        # Execute the first time: verify that it is executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 1st time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=True, exp_cf_state="no_cache"
        )
        #
        # Execute the second time: verify that it is *NOT* executed.
        #
        _LOG.debug("\n%s", prnt.frame("Executing the 2nd time"))
        self._check_cache_state(
            f, cf, 3, 4, exp_f_state=False, exp_cf_state="disk"
        )
        #
        self._check_cache_state(
            f, cf, 4, 4, exp_f_state=False, exp_cf_state="disk"
        )


# #############################################################################
# env.py
# #############################################################################


class Test_env1(ut.TestCase):
    def test_get_system_signature1(self) -> None:
        txt = env.get_system_signature()
        _LOG.debug(txt)


# #############################################################################
# git.py
# #############################################################################


class Test_git1(ut.TestCase):
    """
    Unfortunately we can't check the outcome of some of these functions since we
    don't know in which dir we are running. Thus we test that the function
    completes and visually inspect the outcome, if needed.
    TODO(gp): If we have Jenkins on AM side we could test for the outcome at
     least in that set-up.
    """

    @staticmethod
    def _helper(func_call: str) -> None:
        act = eval(func_call)
        _LOG.debug("%s=%s", func_call, act)

    def test_get_git_name1(self) -> None:
        func_call = "git.get_repo_symbolic_name(super_module=True)"
        self._helper(func_call)

    def test_is_inside_submodule1(self) -> None:
        func_call = "git.is_inside_submodule()"
        self._helper(func_call)

    def test_get_client_root1(self) -> None:
        func_call = "git.get_client_root(super_module=True)"
        self._helper(func_call)

    def test_get_client_root2(self) -> None:
        func_call = "git.get_client_root(super_module=False)"
        self._helper(func_call)

    def test_get_path_from_git_root1(self) -> None:
        file_name = "helpers/test/test_helpers.py"
        act = git.get_path_from_git_root(file_name, super_module=False)
        _LOG.debug("get_path_from_git_root()=%s", act)

    def test_get_repo_symbolic_name1(self) -> None:
        func_call = "git.get_repo_symbolic_name(super_module=True)"
        self._helper(func_call)

    def test_get_repo_symbolic_name2(self) -> None:
        func_call = "git.get_repo_symbolic_name(super_module=False)"
        self._helper(func_call)

    def test_get_modified_files1(self) -> None:
        func_call = "git.get_modified_files()"
        self._helper(func_call)

    def test_get_previous_committed_files1(self) -> None:
        func_call = "git.get_previous_committed_files()"
        self._helper(func_call)

    def test_git_log1(self) -> None:
        func_call = "git.git_log()"
        self._helper(func_call)

    def test_git_log2(self) -> None:
        func_call = "git.git_log(my_commits=True)"
        self._helper(func_call)

    def test_git_all_repo_symbolic_names1(self) -> None:
        func_call = "git.get_all_repo_symbolic_names()"
        self._helper(func_call)

    def test_git_all_repo_symbolic_names2(self) -> None:
        all_repo_sym_names = git.get_all_repo_symbolic_names()
        for repo_sym_name in all_repo_sym_names:
            repo_github_name = git.get_repo_github_name(repo_sym_name)
            _LOG.debug(
                ut.to_string("repo_sym_name")
                + " -> "
                + ut.to_string("repo_github_name")
            )
            git.get_repo_prefix(repo_github_name)
            _LOG.debug(
                ut.to_string("repo_sym_name")
                + " -> "
                + ut.to_string("repo_sym_name_tmpA")
            )

    def test_get_amp_abs_path1(self) -> None:
        amp_dir = git.get_amp_abs_path()
        # Check.
        self.assertTrue(os.path.exists(amp_dir))
        if si.get_user_name() != "jenkins":
            # Jenkins checks out amp repo in directories with different names,
            # e.g., amp.dev.build_clean_env.run_slow_coverage_tests.
            self.assert_equal(os.path.basename(amp_dir), "amp")

    def test_get_branch_name1(self) -> None:
        _ = git.get_branch_name()

    @pytest.mark.skipif('si.get_user_name() == "jenkins"', reason="#781")
    @pytest.mark.skipif(
        'git.get_repo_symbolic_name(super_module=False) == "alphamatic/amp"'
    )
    def test_get_submodule_hash1(self) -> None:
        dir_name = "amp"
        _ = git.get_submodule_hash(dir_name)

    def test_get_head_hash1(self) -> None:
        dir_name = "."
        _ = git.get_head_hash(dir_name)

    def test_get_remote_head_hash1(self) -> None:
        dir_name = "."
        _ = git.get_head_hash(dir_name)

    def test_report_submodule_status1(self) -> None:
        dir_names = ["."]
        short_hash = True
        _ = git.report_submodule_status(dir_names, short_hash)

    def _helper_group_hashes(
        self, head_hash: str, remh_hash: str, subm_hash: Optional[str], exp: str
    ) -> None:
        act = git._group_hashes(head_hash, remh_hash, subm_hash)
        self.assert_equal(act, exp)

    def test_group_hashes1(self) -> None:
        head_hash = "a2bfc704"
        remh_hash = "a2bfc704"
        subm_hash = None
        exp = "head_hash = remh_hash = a2bfc704"
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    def test_group_hashes2(self) -> None:
        head_hash = "22996772"
        remh_hash = "92167662"
        subm_hash = "92167662"
        exp = """head_hash = 22996772
remh_hash = subm_hash = 92167662"""
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    def test_group_hashes3(self) -> None:
        head_hash = "7ea03eb6"
        remh_hash = "7ea03eb6"
        subm_hash = "7ea03eb6"
        exp = "head_hash = remh_hash = subm_hash = 7ea03eb6"
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)


# #############################################################################
# list.py
# #############################################################################


class Test_list_1(ut.TestCase):
    def test_find_duplicates1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(list_out, [])

    def test_find_duplicates2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.find_duplicates(list_)
        self.assertEqual(set(list_out), set("a f".split()))

    def test_remove_duplicates1(self) -> None:
        list_ = "a b c d".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d".split())

    def test_remove_duplicates2(self) -> None:
        list_ = "a b c a d e f f".split()
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "a b c d e f".split())

    def test_remove_duplicates3(self) -> None:
        list_ = "a b c a d e f f".split()
        list_ = list(reversed(list_))
        list_out = hlist.remove_duplicates(list_)
        self.assertEqual(list_out, "f e d a c b".split())


# #############################################################################
# numba.py
# #############################################################################


class Test_numba_1(ut.TestCase):
    def test1(self) -> None:
        # TODO(gp): Implement this.
        pass


# #############################################################################
# printing.py
# #############################################################################


class Test_printing1(ut.TestCase):
    def test_color_highlight1(self) -> None:
        for c in prnt.COLOR_MAP:
            _LOG.debug(prnt.color_highlight(c, c))


# #############################################################################
# s3.py
# #############################################################################


class Test_s3_1(ut.TestCase):
    def test_get_path1(self) -> None:
        file_path = (
            "s3://default00-bucket/kibot/All_Futures_Continuous_Contracts_daily"
        )
        bucket_name, file_path = hs3.parse_path(file_path)
        self.assertEqual(bucket_name, "default00-bucket")
        self.assertEqual(
            file_path, "kibot/All_Futures_Continuous_Contracts_daily"
        )

    @pytest.mark.slow
    def test_ls1(self) -> None:
        file_path = os.path.join(
            hs3.get_path(), "kibot/All_Futures_Continuous_Contracts_daily"
        )
        file_names = hs3.ls(file_path)
        # We rely on the fact that Kibot data is not changing.
        self.assertEqual(len(file_names), 253)


# #############################################################################
# unit_test.py
# #############################################################################


class Test_unit_test1(ut.TestCase):
    @pytest.mark.amp
    def test_purify_txt_from_client1(self) -> None:
        super_module_path = git.get_client_root(super_module=True)
        # TODO(gp): We should remove the current path.
        txt = r"""
************* Module input [pylint]
$SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py: Your code has been rated at -10.00/10 (previous run: -10.00/10, +0.00) [pylint]
$SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:20: W605 invalid escape sequence '\s' [flake8]
$SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:9: F821 undefined name 're' [flake8]
cmd line='$SUPER_MODULE/dev_scripts/linter.py -f $SUPER_MODULE/amp/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py --linter_log $SUPER_MODULE/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/linter.log'
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [E0602(undefined-variable), ] Undefined variable 're' [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [W1401(anomalous-backslash-in-string), ] Anomalous backslash in string: '\s'. String constant might be missing an r prefix. [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: error: Name 're' is not defined [mypy]
"""
        txt = txt.replace("$SUPER_MODULE", super_module_path)
        exp = r"""
************* Module input [pylint]
$GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py: Your code has been rated at -10.00/10 (previous run: -10.00/10, +0.00) [pylint]
$GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:20: W605 invalid escape sequence '\s' [flake8]
$GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3:9: F821 undefined name 're' [flake8]
cmd line='$GIT_ROOT/dev_scripts/linter.py -f $GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py --linter_log $GIT_ROOT/dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/linter.log'
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [E0602(undefined-variable), ] Undefined variable 're' [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: [W1401(anomalous-backslash-in-string), ] Anomalous backslash in string: '\s'. String constant might be missing an r prefix. [pylint]
dev_scripts/test/Test_linter_py1.test_linter1/tmp.scratch/input.py:3: error: Name 're' is not defined [mypy]
"""
        act = ut.purify_txt_from_client(txt)
        self.assert_equal(act, exp)


# #############################################################################
# user_credentials.py
# #############################################################################


class Test_user_credentials1(ut.TestCase):
    def test_get_credentials1(self) -> None:
        data = usc.get_credentials()
        _LOG.debug("data=%s", data)
