import logging

import helpers.traceback_helper as htrace
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_Traceback1(hut.TestCase):

    def test_parse1(self) -> None:
        txt = """
    
        TEST
Traceback
    TEST
Traceback (most recent call last):
  File "/app/amp/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
    act = ltasks._get_gh_issue_title(issue_id, repo)
  File "/app/amp/lib_tasks.py", line 1265, in _get_gh_issue_title
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
  File "/app/amp/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
    if repo_short_name == "amp":
NameError: name 'repo_short_name' is not defined
    TEST TEST TEST
"""
        act_cfile, act_traceback = htrace.parse_traceback(txt)
        _LOG.debug("act_cfile=\n%s", act_cfile)
        # pylint: disable=line-too-long
        exp_cfile = [('test/test_lib_tasks.py', 27,
                'test_get_gh_issue_title2:act = ltasks._get_gh_issue_title(issue_id, repo)'),
               ('lib_tasks.py', 1265,
                '_get_gh_issue_title:task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)'),
               ('helpers/git.py', 397,
                'get_task_prefix_from_repo_short_name:if repo_short_name == "amp":')]
        # pylint: enable=line-too-long
        self.assert_equal(htrace.cfile_to_str(act_cfile), htrace.cfile_to_str(exp_cfile))
        #
        exp_traceback = """
Traceback (most recent call last):
  File "/app/amp/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
    act = ltasks._get_gh_issue_title(issue_id, repo)
  File "/app/amp/lib_tasks.py", line 1265, in _get_gh_issue_title
    task_prefix = git.get_task_prefix_from_repo_short_name(repo_short_name)
  File "/app/amp/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
    if repo_short_name == "amp":
        """.rstrip().lstrip()
        self.assert_equal(act_traceback, exp_traceback)
