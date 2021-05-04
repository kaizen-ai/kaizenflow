# This should only test helper functions from `lib_tasks.py`.
# `test_tasks.py` associated to `tasks.py` should test specific task targets.

import logging
import os

import helpers.git as git
import helpers.system_interaction as hsinte
import helpers.unit_test as hut
import lib_tasks as ltasks

_LOG = logging.getLogger(__name__)


class TestLibTasks1(hut.TestCase):
    def test_get_build_tag1(self) -> None:
        build_tag = ltasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)

    def test_get_gh_issue_title1(self) -> None:
        self._gh_login()
        issue_id = 1
        repo = "amp"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "AmpTask1_Bridge_Python_and_R"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title2(self) -> None:
        self._gh_login()
        issue_id = 1
        repo = "lem"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "LemTask1_Adapt_the_usual_infra_from_my_codebase"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title3(self) -> None:
        self._gh_login()
        issue_id = 1
        repo = "dev_tools"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "DevToolsTask1_Migration_from_amp"
        self.assert_equal(act, exp)

    @staticmethod
    def _gh_login() -> None:
        """
        Log in inside GitHub.
        """
        env_var = "GH_ACTION_ACCESS_TOKEN"
        if os.environ.get(env_var, None):
            # If the env var exists and it's not None.
            _LOG.warning("Using env var '%s' to log in GitHub", env_var)
            cmd = "echo $GH_ACTION_ACCESS_TOKEN | gh auth login --with-token"
            hsinte.system(cmd)
        # Check that we are logged in.
        cmd = "gh auth status"
        hsinte.system(cmd)


class TestLibTasksRunTests1(hut.TestCase):

    def test_find_test_files1(self) -> None:
        files = ltasks._find_test_files()
        # For sure there are more than 1 test files: at least this one.
        self.assertGreater(len(files), 1)

    def test_find_test_files2(self) -> None:
        git_root = git.get_client_root(super_module=True)
        files = ltasks._find_test_files(git_root)
        # For sure there are more than 1 test files: at least this one.
        self.assertGreater(len(files), 1)

    def test_find_test_class1(self) -> None:
        act = ltasks._find_test_class("TestLibTasksRunTests1")
        act = list(map(hut.remove_amp_references, act))
        exp = ["test/test_lib_tasks.py::TestLibTasksRunTests1"]
        self.assert_equal(str(act), str(exp))

