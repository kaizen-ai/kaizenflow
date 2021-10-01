import logging
from typing import List

import helpers.dbg as hdbg
import helpers.printing as hprintin
import helpers.traceback_helper as htrhel
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


class Test_Traceback1(huntes.TestCase):
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
            task_prefix = hgit.get_task_prefix_from_repo_short_name(repo_short_name)
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
                "_get_gh_issue_title:task_prefix = hgit.get_task_prefix_from_repo_short_name(repo_short_name)",
            ),
            (
                "helpers/git.py",
                397,
                'get_task_prefix_from_repo_short_name:if repo_short_name == "amp":',
            ),
        ]
        exp_cfile = htrhel.cfile_to_str(exp_cfile)
        # pylint: enable=line-too-long
        exp_traceback = """
        Traceback (most recent call last):
          File "$GIT_ROOT/helpers/test/test_lib_tasks.py", line 27, in test_get_gh_issue_title2
            act = ltasks._get_gh_issue_title(issue_id, repo)
          File "$GIT_ROOT/helpers/lib_tasks.py", line 1265, in _get_gh_issue_title
            task_prefix = hgit.get_task_prefix_from_repo_short_name(repo_short_name)
          File "$GIT_ROOT/helpers/git.py", line 397, in get_task_prefix_from_repo_short_name
            if repo_short_name == "amp":
        """
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
        exp_cfile: List[htrhel.CFILE_ROW] = []
        exp_cfile = htrhel.cfile_to_str(exp_cfile)
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
        # pylint: disable=line-too-long
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
        # pylint: enable=line-too-long
        self._parse_traceback_helper(
            txt, purify_from_client, exp_cfile, exp_traceback
        )

    def test_parse3(self) -> None:
        """
        Parse a traceback file with both files from Docker and local files.
        """
        # Use references to this file so that we are independent from the file
        # layout.
        # pylint: disable=line-too-long
        txt = """
        collected 6 items

        helpers/test/test_lib_tasks.py::Test_pytest_failed1::test_classes1 (0.02 s) FAILED [ 16%]

        =================================== FAILURES ===================================
        ______________________ Test_pytest_failed1.test_classes1 _______________________
        Traceback (most recent call last):
          File "/app/amp/helpers/test/test_lib_tasks.py", line 1460, in test_classes1
            self._helper(file_name, target_type, exp)
          File "/app/amp/helpers/test/test_lib_tasks.py", line 1440, in _helper
            act = ltasks.pytest_failed(ctx, use_frozen_list=use_frozen_list,
          File "/venv/lib/python3.8/site-packages/invoke/tasks.py", line 127, in __call__
            result = self.body(*args, **kwargs)
          File "/app/amp/helpers/lib_tasks.py", line 2140, in pytest_failed
            hdbg.dassert(m, "Invalid test='%s'", test)
          File "/app/amp/helpers/dbg.py", line 129, in dassert
            _dfatal(txt, msg, *args)
          File "/app/amp/helpers/dbg.py", line 117, in _dfatal
            dfatal(dfatal_txt)
          File "/app/amp/helpers/dbg.py", line 63, in dfatal
            raise assertion_type(ret)
        kAssertionError:
        # #####################################################################
        * Failed assertion *
        cond=None
        Invalid test='dev_scripts/testing/test/test_run_tests.py'
        """
        # pylint: enable=line-too-long
        purify_from_client = False
        exp_cfile = """
        $GIT_ROOT/helpers/test/test_lib_tasks.py:1460:test_classes1:self._helper(file_name, target_type, exp)
        $GIT_ROOT/helpers/test/test_lib_tasks.py:1440:_helper:act = ltasks.pytest_failed(ctx, use_frozen_list=use_frozen_list,
        /venv/lib/python3.8/site-packages/invoke/tasks.py:127:__call__:result = self.body(*args, **kwargs)
        $GIT_ROOT/helpers/lib_tasks.py:2140:pytest_failed:hdbg.dassert(m, "Invalid test='%s'", test)
        $GIT_ROOT/helpers/dbg.py:129:dassert:_dfatal(txt, msg, *args)
        $GIT_ROOT/helpers/dbg.py:117:_dfatal:dfatal(dfatal_txt)
        $GIT_ROOT/helpers/dbg.py:63:dfatal:raise assertion_type(ret)"""
        exp_traceback = r"""
        Traceback (most recent call last):
          File "$GIT_ROOT/helpers/test/test_lib_tasks.py", line 1460, in test_classes1
            self._helper(file_name, target_type, exp)
          File "$GIT_ROOT/helpers/test/test_lib_tasks.py", line 1440, in _helper
            act = ltasks.pytest_failed(ctx, use_frozen_list=use_frozen_list,
          File "/venv/lib/python3.8/site-packages/invoke/tasks.py", line 127, in __call__
            result = self.body(*args, **kwargs)
          File "$GIT_ROOT/helpers/lib_tasks.py", line 2140, in pytest_failed
            hdbg.dassert(m, "Invalid test='%s'", test)
          File "$GIT_ROOT/helpers/dbg.py", line 129, in dassert
            _dfatal(txt, msg, *args)
          File "$GIT_ROOT/helpers/dbg.py", line 117, in _dfatal
            dfatal(dfatal_txt)
          File "$GIT_ROOT/helpers/dbg.py", line 63, in dfatal
            raise assertion_type(ret)"""
        self._parse_traceback_helper(
            txt, purify_from_client, exp_cfile, exp_traceback
        )

    # pylint: disable=line-too-long
    # TODO(gp): Add test and fix for the following traceback:
    # Traceback (most recent call last):
    #   File "/Users/saggese/src/venv/amp.client_venv/bin/invoke", line 8, in <module>
    #     sys.exit(program.run())
    #   File "/Users/saggese/src/venv/amp.client_venv/lib/python3.9/site-packages/invoke/program.py", line 373, in run
    #     self.parse_collection()
    #   File "/Users/saggese/src/venv/amp.client_venv/lib/python3.9/site-packages/invoke/program.py", line 465, in parse_collection
    #     self.load_collection()
    #   File "/Users/saggese/src/venv/amp.client_venv/lib/python3.9/site-packages/invoke/program.py", line 696, in load_collection
    #     module, parent = loader.load(coll_name)
    #   File "/Users/saggese/src/venv/amp.client_venv/lib/python3.9/site-packages/invoke/loader.py", line 76, in load
    #     module = imp.load_module(name, fd, path, desc)
    #   File "/usr/local/Cellar/python@3.9/3.9.5/Frameworks/Python.framework/Versions/3.9/lib/python3.9/imp.py", line 234, in load_module
    #     return load_source(name, filename, file)
    #   File "/usr/local/Cellar/python@3.9/3.9.5/Frameworks/Python.framework/Versions/3.9/lib/python3.9/imp.py", line 171, in load_source
    #     module = _load(spec)
    #   File "<frozen importlib._bootstrap>", line 711, in _load
    #   File "<frozen importlib._bootstrap>", line 680, in _load_unlocked
    #   File "<frozen importlib._bootstrap_external>", line 855, in exec_module
    #   File "<frozen importlib._bootstrap>", line 228, in _call_with_frames_removed
    #   File "/Users/saggese/src/lem1/amp/tasks.py", line 8, in <module>
    #     from helpers.lib_tasks import set_default_params  # This is not an invoke target.
    #   File "/Users/saggese/src/lem1/amp/helpers/lib_tasks.py", line 23, in <module>
    #     import helpers.git as hgit
    #   File "/Users/saggese/src/lem1/amp/helpers/git.py", line 16, in <module>
    #     import helpers.system_interaction as hsyint
    #   File "/Users/saggese/src/lem1/amp/helpers/system_interaction.py", line 529
    #     signature2 = _compute_file_signature(file_name, dir_depth)
    #     ^
    # SyntaxError: invalid syntax
    # Traceback (most recent call last):
    #   File "/Users/saggese/src/lem1/amp/dev_scripts/tg.py", line 21, in <module>
    #     import helpers.system_interaction as hsyint
    #   File "/Users/saggese/src/lem1/amp/helpers/system_interaction.py", line 529
    #     signature2 = _compute_file_signature(file_name, dir_depth)
    #     ^
    # SyntaxError: invalid syntax
    # pylint: enable=line-too-long


    def _parse_traceback_helper(
        self,
        txt: str,
        purify_from_client: bool,
        exp_cfile: str,
        exp_traceback: str,
    ) -> None:
        hdbg.dassert_isinstance(txt, str)
        hdbg.dassert_isinstance(exp_cfile, str)
        hdbg.dassert_isinstance(exp_traceback, str)
        txt = hprintin.dedent(txt)
        # Run the function under test.
        act_cfile, act_traceback = htrhel.parse_traceback(
            txt, purify_from_client=purify_from_client
        )
        _LOG.debug("act_cfile=\n%s", act_cfile)
        _LOG.debug("act_traceback=\n%s", act_traceback)
        # Compare cfile.
        act_cfile = htrhel.cfile_to_str(act_cfile)
        exp_cfile = hprintin.dedent(exp_cfile)
        self.assert_equal(
            act_cfile, exp_cfile, fuzzy_match=True, purify_text=True
        )
        # Compare traceback.
        # Handle `None`.
        act_traceback = str(act_traceback)
        exp_traceback = hprintin.dedent(exp_traceback)
        self.assert_equal(
            act_traceback, exp_traceback, fuzzy_match=True, purify_text=True
        )
