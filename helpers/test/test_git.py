import logging
from typing import Optional

import helpers.git as git
import helpers.unit_test as hut
import helpers.system_interaction as hsinte

_LOG = logging.getLogger(__name__)

# Unfortunately we can't check the outcome of some of these functions since we
# don't know in which dir we are running. Thus we just test that the function
# completes and visually inspect the outcome, if possible.


def _execute_func_call(func_call: str) -> None:
    """
    Execute a function call, e.g., `func_call = "git.get_modified_files()"`.
    """
    act = eval(func_call)
    _LOG.debug("\n-> %s=\n  '%s'", func_call, act)


class Test_git_submodule1(hut.TestCase):
    def test_get_client_root1(self) -> None:
        func_call = "git.get_client_root(super_module=True)"
        _execute_func_call(func_call)

    def test_get_client_root2(self) -> None:
        func_call = "git.get_client_root(super_module=False)"
        _execute_func_call(func_call)

    def test_get_branch_name1(self) -> None:
        _ = git.get_branch_name()

    def test_is_inside_submodule1(self) -> None:
        func_call = "git.is_inside_submodule()"
        _execute_func_call(func_call)

    def test_is_amp(self) -> None:
        func_call = "git.is_amp()"
        _execute_func_call(func_call)

    def test_is_lem(self) -> None:
        func_call = "git.is_lem()"
        _execute_func_call(func_call)

    def test_get_path_from_supermodule1(self) -> None:
        func_call = "git.get_path_from_supermodule()"
        _execute_func_call(func_call)

    def test_get_submodule_paths1(self) -> None:
        func_call = "git.get_submodule_paths()"
        _execute_func_call(func_call)


class Test_git_submodule2(hut.TestCase):

    # def test_get_submodule_hash1(self) -> None:
    #     dir_name = "amp"
    #     _ = git._get_submodule_hash(dir_name)

    def test_get_remote_head_hash1(self) -> None:
        dir_name = "."
        _ = git.get_head_hash(dir_name)

    # def test_report_submodule_status1(self) -> None:
    #     dir_names = ["."]
    #     short_hash = True
    #     _ = git.report_submodule_status(dir_names, short_hash)

    def test_get_head_hash1(self) -> None:
        dir_name = "."
        _ = git.get_head_hash(dir_name)

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
        exp = """
        head_hash = 22996772
        remh_hash = subm_hash = 92167662
        """
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    def test_group_hashes3(self) -> None:
        head_hash = "7ea03eb6"
        remh_hash = "7ea03eb6"
        subm_hash = "7ea03eb6"
        exp = "head_hash = remh_hash = subm_hash = 7ea03eb6"
        #
        self._helper_group_hashes(head_hash, remh_hash, subm_hash, exp)

    def _helper_group_hashes(
        self, head_hash: str, remh_hash: str, subm_hash: Optional[str], exp: str
    ) -> None:
        act = git._group_hashes(head_hash, remh_hash, subm_hash)
        self.assert_equal(act, exp, fuzzy_match=True)


class Test_git_repo_name1(hut.TestCase):
    def test_parse_github_repo_name1(self) -> None:
        repo_name = "git@github.com:alphamatic/amp"
        act = git._parse_github_repo_name(repo_name)
        self.assert_equal(act, "alphamatic/amp")

    def test_parse_github_repo_name2(self) -> None:
        repo_name = "https://github.com/alphamatic/amp"
        act = git._parse_github_repo_name(repo_name)
        exp = "alphamatic/amp"
        self.assert_equal(act, exp)

    def test_get_repo_full_name_from_dirname1(self) -> None:
        func_call = "git.get_repo_full_name_from_dirname(dir_name='.')"
        _execute_func_call(func_call)

    def test_get_repo_full_name_from_client1(self) -> None:
        func_call = "git.get_repo_full_name_from_client(super_module=True)"
        _execute_func_call(func_call)

    def test_get_repo_full_name_from_client2(self) -> None:
        func_call = "git.get_repo_full_name_from_client(super_module=False)"
        _execute_func_call(func_call)

    def test_get_repo_name1(self) -> None:
        short_name = "amp"
        mode = "short_name"
        act = git.get_repo_name(short_name, mode)
        exp = "alphamatic/amp"
        self.assert_equal(act, exp)

    def test_get_repo_name2(self) -> None:
        full_name = "alphamatic/amp"
        mode = "full_name"
        act = git.get_repo_name(full_name, mode)
        exp = "amp"
        self.assert_equal(act, exp)

    def test_get_repo_name3(self) -> None:
        full_name = "alphamatic/lemonade"
        mode = "full_name"
        act = git.get_repo_name(full_name, mode)
        exp = "lem"
        self.assert_equal(act, exp)

    def test_get_repo_name4(self) -> None:
        full_name = "alphamatic/dev_tools"
        mode = "full_name"
        act = git.get_repo_name(full_name, mode)
        exp = "dev_tools"
        self.assert_equal(act, exp)

    def test_get_all_repo_names1(self) -> None:
        mode = "short_name"
        act = git.get_all_repo_names(mode)
        exp = ["amp", "dev_tools", "lem"]
        self.assert_equal(str(act), str(exp))

    def test_get_all_repo_names2(self) -> None:
        mode = "full_name"
        act = git.get_all_repo_names(mode)
        exp = ["alphamatic/amp", "alphamatic/dev_tools", "alphamatic/lemonade"]
        self.assert_equal(str(act), str(exp))

    def test_get_repo_name_rountrip1(self) -> None:
        """
        Test round-trip transformation for get_repo_name().
        """
        # Get the short name for all the repos.
        mode = "short_name"
        all_repo_short_names = git.get_all_repo_names(mode)
        # Round trip.
        for repo_short_name in all_repo_short_names:
            repo_full_name = git.get_repo_name(repo_short_name, "short_name")
            repo_short_name_tmp = git.get_repo_name(repo_full_name, "full_name")
            self.assert_equal(repo_short_name, repo_short_name_tmp)

    def test_get_task_prefix_from_repo_short_name1(self) -> None:
        short_name = "dev_tools"
        act = git.get_task_prefix_from_repo_short_name(short_name)
        exp = "DevToolsTask"
        self.assert_equal(act, exp)


class Test_git_path1(hut.TestCase):
    def test_get_path_from_git_root1(self) -> None:
        file_name = "helpers/test/test_git.py"
        act = git.get_path_from_git_root(file_name, super_module=False)
        _LOG.debug("get_path_from_git_root()=%s", act)


class Test_git_modified_files1(hut.TestCase):
    def test_get_modified_files1(self) -> None:
        func_call = "git.get_modified_files()"
        _execute_func_call(func_call)

    def test_get_previous_committed_files1(self) -> None:
        func_call = "git.get_previous_committed_files()"
        _execute_func_call(func_call)

    def test_get_modified_files_in_branch1(self) -> None:
        # This test needs a reference to Git master branch.
        git.fetch_origin_master_if_needed()
        #
        func_call = "git.get_modified_files_in_branch('master')"
        _execute_func_call(func_call)

    def test_get_summary_files_in_branch1(self) -> None:
        # This test needs a reference to Git master branch.
        git.fetch_origin_master_if_needed()
        #
        func_call = "git.get_summary_files_in_branch('master')"
        _execute_func_call(func_call)

    def test_git_log1(self) -> None:
        func_call = "git.git_log()"
        _execute_func_call(func_call)