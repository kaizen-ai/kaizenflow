import logging

import helpers.dbg as dbg
import helpers.unit_test as hut
import lib_tasks as ltasks

_LOG = logging.getLogger(__name__)


class TestLibTasks1(hut.TestCase):

    def test_get_build_tag1(self) -> None:
        build_tag = ltasks._get_build_tag()
        _LOG.debug("build_tag=%s", build_tag)

    def test_get_gh_issue_title1(self) -> None:
        issue_id = 1
        repo = "amp"
        as_git_branch_name = True
        act = ltasks._get_gh_issue_title(repo, issue_id, as_git_branch_name)
        exp = ""
        self.assert_equal(act, exp)
