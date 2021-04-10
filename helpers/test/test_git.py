import logging
from typing import Optional

import pytest

import helpers.git as git
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_git1(hut.TestCase):
    """
    Unfortunately we can't check the outcome of some of these functions since
    we don't know in which dir we are running.

    Thus we test that the function completes and visually inspect the outcome,
    if needed.
    """

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

    @pytest.mark.not_docker
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
                hut.to_string("repo_sym_name")
                + " -> "
                + hut.to_string("repo_github_name")
            )
            git.get_repo_prefix(repo_github_name)
            _LOG.debug(
                hut.to_string("repo_sym_name")
                + " -> "
                + hut.to_string("repo_sym_name_tmpA")
            )

    def test_get_branch_name1(self) -> None:
        _ = git.get_branch_name()

    @pytest.mark.not_docker(reason="Issue #3482")
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

    @pytest.mark.not_docker(reason="Issue #3482")
    @pytest.mark.skipif(
        'si.get_server_name() == "docker-instance"', reason="Issue #1522, #1831"
    )
    def test_report_submodule_status1(self) -> None:
        dir_names = ["."]
        short_hash = True
        _ = git.report_submodule_status(dir_names, short_hash)

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

    def _helper_group_hashes(
        self, head_hash: str, remh_hash: str, subm_hash: Optional[str], exp: str
    ) -> None:
        act = git._group_hashes(head_hash, remh_hash, subm_hash)
        self.assert_equal(act, exp)

    @staticmethod
    def _helper(func_call: str) -> None:
        act = eval(func_call)
        _LOG.debug("%s=%s", func_call, act)
