import logging
from typing import List

import helpers.dbg as dbg
import helpers.traceback_helper as htrace
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_Traceback1(hut.TestCase):
    def test_parse1(self) -> None:
        """
        Parse traceback with all files from Docker that actually exist in the
        current repo.
        """
        txt = """

        TEST
Traceback
    TEST
Traceback (most recent call last):
  File "/app/amp/helpers/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
    act = ltasks._get_gh_issue_title(issue_id, repo)
  File "/app/amp/helpers/lib_tasks.py", line 1265, in _get_gh_issue_title
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
  File "/app/amp/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
    if repo_short_name == "amp":
NameError: name 'repo_short_name' is not defined
    TEST TEST TEST
"""
        purify_from_client = True
        # pylint: disable=line-too-long
        exp_cfile = [
            (
                "helpers/test/test_lib_tasks.py",
                27,
                "test_get_gh_issue_title2:act = ltasks._get_gh_issue_title(issue_id, repo)",
            ),
            (
                "helpers/lib_tasks.py",
                1265,
                "_get_gh_issue_title:task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)",
            ),
            (
                "helpers/git.py",
                397,
                'get_task_prefix_from_repo_short_name:if repo_short_name == "amp":',
            ),
        ]
        exp_cfile = htrace.cfile_to_str(exp_cfile)
        # pylint: enable=line-too-long
        exp_traceback = """
Traceback (most recent call last):
  File "$GIT_ROOT/helpers/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
    act = ltasks._get_gh_issue_title(issue_id, repo)
  File "$GIT_ROOT/helpers/lib_tasks.py", line 1265, in _get_gh_issue_title
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
  File "$GIT_ROOT/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
    if repo_short_name == "amp":
        """.rstrip().lstrip()
        self._parse_traceback_helper(
            txt, purify_from_client, exp_cfile, exp_traceback
        )

    def test_parse_empty_traceback1(self) -> None:
        """
        Parse an empty traceback file.
        """
        txt = """

        TEST
Traceback
    TEST TEST TEST
"""
        purify_from_client = True
        exp_cfile: List[htrace.CFILE_ROW] = []
        exp_cfile = htrace.cfile_to_str(exp_cfile)
        exp_traceback = "None"
        self._parse_traceback_helper(
            txt, purify_from_client, exp_cfile, exp_traceback
        )

    def test_parse2(self) -> None:
        """
        Parse a traceback file with both files from Docker and local files.
        """
        # Use references to this file so that we are independent from the file
        # layout.
        txt = """
Traceback (most recent call last):
  File "./helpers/test/test_traceback.py", line 146, in <module>
    _main(_parse())
  File "./helpers/test/test_traceback.py", line 105, in _main
    configs = cdtfut.get_configs_from_command_line(args)
  File "/app/amp/./helpers/test/test_traceback.py", line 228, in get_configs_from_command_line
    "config_builder": args.config_builder,
"""
        purify_from_client = True
        exp_cfile = """
        helpers/test/test_traceback.py:146:<module>:_main(_parse())
        helpers/test/test_traceback.py:105:_main:configs = cdtfut.get_configs_from_command_line(args)
        helpers/test/test_traceback.py:228:get_configs_from_command_line:"config_builder": args.config_builder,
        """
        exp_traceback = """
Traceback (most recent call last):
  File "./helpers/test/test_traceback.py", line 146, in <module>
    _main(_parse())
  File "./helpers/test/test_traceback.py", line 105, in _main
    configs = cdtfut.get_configs_from_command_line(args)
  File "$GIT_ROOT/./helpers/test/test_traceback.py", line 228, in get_configs_from_command_line
    "config_builder": args.config_builder,
        """
        self._parse_traceback_helper(
            txt, purify_from_client, exp_cfile, exp_traceback
        )

    def _parse_traceback_helper(
        self,
        txt: str,
        purify_from_client: bool,
        exp_cfile: str,
        exp_traceback: str,
    ) -> None:
        dbg.dassert_isinstance(txt, str)
        dbg.dassert_isinstance(exp_cfile, str)
        dbg.dassert_isinstance(exp_traceback, str)
        # Run the function under test.
        act_cfile, act_traceback = htrace.parse_traceback(
            txt, purify_from_client=purify_from_client
        )
        _LOG.debug("act_cfile=\n%s", act_cfile)
        _LOG.debug("act_traceback=\n%s", act_traceback)
        # Compare cfile.
        act_cfile = htrace.cfile_to_str(act_cfile)
        self.assert_equal(act_cfile, exp_cfile, fuzzy_match=True, purify_text=True)
        # Compare traceback.
        # Handle `None`.
        act_traceback = str(act_traceback)
        self.assert_equal(act_traceback, exp_traceback, fuzzy_match=True,
                          purify_text=True)