import logging
import os

import helpers.system_interaction as hsinte
import helpers.unit_test as hut
import lib_tasks as ltasks

_LOG = logging.getLogger(__name__)


class TestLibTasks1(hut.TestCase):
    def test_get_build_tag1(self) -> None:
        build_tag = ltasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)

    def _gh_login(self) -> None:
        """
        Log in inside GitHub.
        """
        if hsinte.is_inside_ci():
            _LOG.warning("Inside CI: authorizing")
            env_var = "GH_ACTION_ACCESS_TOKEN"
            dbg.dassert_in(env_var, os.environ)
            cmd = "echo $GH_ACTION_ACCESS_TOKEN | gh auth login --with-token"
            hsinte.system(cmd)
        # Check that we are logged in.
        cmd = "gh auth status"
        hsinte.system(cmd)

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
