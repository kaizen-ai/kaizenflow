# This should only test helper functions from `lib_tasks.py`.
# `test_tasks.py` associated to `tasks.py` should test specific task targets.

import logging
import os

import pytest

import helpers.git as git
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut
import lib_tasks as ltasks

_LOG = logging.getLogger(__name__)


class TestLibTasks1(hut.TestCase):
    def test_get_build_tag1(self) -> None:
        code_ver = "amp-1.0.0"
        build_tag = ltasks._get_build_tag(code_ver)
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


class TestLibRunTests1(hut.TestCase):
    def test_run_fast_tests1(self) -> None:
        """
        Basic run fast tests.
        """
        pytest_opts = ""
        pytest_mark = ""
        dir_name = ""
        skip_submodules = False
        coverage = False
        collect_only = False
        #
        skipped_tests = "not slow and not superslow"
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = 'pytest -m "not slow and not superslow"'
        self.assert_equal(act, exp)

    def test_run_fast_tests2(self) -> None:
        """
        Coverage and collect-only.
        """
        pytest_opts = ""
        pytest_mark = ""
        dir_name = ""
        skip_submodules = False
        coverage = True
        collect_only = True
        #
        skipped_tests = "not slow and not superslow"
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = r'pytest -m "not slow and not superslow" --cov=. --cov-branch --cov-report term-missing --cov-report html --collect-only'
        self.assert_equal(act, exp)

    @pytest.mark.skipif(
        not git.has_submodules(),
        reason="Run only if this repo has are submodules",
    )
    def test_run_fast_tests3(self) -> None:
        """
        Skip submodules.
        """
        pytest_opts = ""
        pytest_mark = ""
        dir_name = ""
        skip_submodules = True
        coverage = False
        collect_only = False
        #
        skipped_tests = ""
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = r""
        self.assert_equal(act, exp)

    def test_run_fast_tests4(self) -> None:
        """
        Select pytest_mark.
        """
        scratch_space = self.get_scratch_space(use_absolute_path=False)
        dir_name = os.path.join(scratch_space, "test")
        file_dict = {
            "test_this.py": hprint.dedent(
                """
                    foo

                    class TestHelloWorld(hut.TestCase):
                        bar
                    """
            ),
            "test_that.py": hprint.dedent(
                """
                    foo
                    baz

                    @pytest.mark.no_container
                    class TestHello_World(hut.):
                        bar
                    """
            ),
        }
        incremental = True
        hut.create_test_dir(dir_name, incremental, file_dict)

        pytest_opts = ""
        pytest_mark = "no_container"
        dir_name = scratch_space
        skip_submodules = True
        coverage = False
        collect_only = False
        #
        skipped_tests = ""
        act = ltasks._build_run_command_line(
            pytest_opts,
            pytest_mark,
            dir_name,
            skip_submodules,
            coverage,
            collect_only,
            skipped_tests,
        )
        exp = "pytest TestLibRunTests1.test_run_fast_tests4/tmp.scratch/test/test_that.py"
        self.assert_equal(act, exp)


class TestLibTasksRunTests1(hut.TestCase):
    def test_find_test_files1(self) -> None:
        """
        Find all the test files in the current dir.
        """
        files = ltasks._find_test_files()
        # For sure there are more than 1 test files: at least this one.
        self.assertGreater(len(files), 1)

    def test_find_test_files2(self) -> None:
        """
        Find all the test files from the top of the super module root.
        """
        git_root = git.get_client_root(super_module=True)
        files = ltasks._find_test_files(git_root)
        # For sure there are more than 1 test files: at least this one.
        self.assertGreater(len(files), 1)

    def test_find_test_class1(self) -> None:
        """
        Find the current test class.
        """
        git_root = git.get_client_root(super_module=True)
        file_names = ltasks._find_test_files(git_root)
        #
        file_names = ltasks._find_test_class("TestLibTasksRunTests1", file_names)
        act = hut.purify_file_names(file_names)
        exp = ["test/test_lib_tasks.py::TestLibTasksRunTests1"]
        self.assert_equal(str(act), str(exp))

    def test_find_test_class2(self) -> None:
        """
        Find the current test class.
        """
        file_names = [__file__]
        #
        file_names = ltasks._find_test_class("TestLibTasksRunTests1", file_names)
        act = hut.purify_file_names(file_names)
        exp = ["test/test_lib_tasks.py::TestLibTasksRunTests1"]
        self.assert_equal(str(act), str(exp))

    def test_find_test_class3(self) -> None:
        """
        Create synthetic code and look for a class.
        """
        scratch_space = self.get_scratch_space()
        dir_name = os.path.join(scratch_space, "test")
        file_dict = {
            "test_this.py": hprint.dedent(
                """
                    foo

                    class TestHelloWorld(hut.TestCase):
                        bar
                    """
            ),
            "test_that.py": hprint.dedent(
                """
                    foo
                    baz

                    class TestHello_World(hut.):
                        bar
                    """
            ),
        }
        incremental = True
        hut.create_test_dir(dir_name, incremental, file_dict)
        #
        file_names = ltasks._find_test_files(dir_name)
        act_file_names = [os.path.relpath(d, scratch_space) for d in file_names]
        exp_file_names = ["test/test_that.py", "test/test_this.py"]
        self.assert_equal(str(act_file_names), str(exp_file_names))
        #
        act = ltasks._find_test_class("TestHelloWorld", file_names)
        act = hut.purify_file_names(act)
        exp = [
            "test/TestLibTasksRunTests1.test_find_test_class3/tmp.scratch/"
            "test/test_this.py::TestHelloWorld"
        ]
        self.assert_equal(str(act), str(exp))

    def test_find_test_decorator1(self) -> None:
        """
        Find test functions in the "no_container" in synthetic code.
        """
        scratch_space = self.get_scratch_space()
        dir_name = os.path.join(scratch_space, "test")
        file_dict = {
            "test_this.py": hprint.dedent(
                """
                    foo

                    class TestHelloWorld(hut.TestCase):
                        bar
                    """
            ),
            "test_that.py": hprint.dedent(
                """
                    foo
                    baz

                    @pytest.mark.no_container
                    class TestHello_World(hut.):
                        bar
                    """
            ),
        }
        incremental = True
        hut.create_test_dir(dir_name, incremental, file_dict)
        #
        file_names = ltasks._find_test_files(dir_name)
        act = ltasks._find_test_decorator("no_container", file_names)
        act = hut.purify_file_names(act)
        exp = [
            "test/TestLibTasksRunTests1.test_find_test_decorator1/tmp.scratch/"
            "test/test_that.py"
        ]
        self.assert_equal(str(act), str(exp))

    def test_find_test_decorator2(self) -> None:
        """
        Find test functions in the "no_container" test list.
        """
        file_names = ["test/test_tasks.py"]
        act = ltasks._find_test_decorator("no_container", file_names)
        act = hut.purify_file_names(act)
        exp = ["test/test_tasks.py"]
        self.assert_equal(str(act), str(exp))
