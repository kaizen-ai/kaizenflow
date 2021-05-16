# This should only test helper functions from `lib_tasks.py`.
# `test_tasks.py` associated to `tasks.py` should test specific task targets.

import logging
import os
import re
from typing import Dict

import invoke
import pytest

import helpers.git as git
import helpers.lib_tasks as ltasks
import helpers.printing as hprint
import helpers.system_interaction as hsinte
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


def _get_default_params() -> Dict[str, str]:
    """
    Get fake params pointing to a different image so we can test the code
    without affecting the official images.
    """
    ecr_base_path = "665840871993.dkr.ecr.us-east-1.amazonaws.com"
    default_params = {
        "ECR_BASE_PATH": ecr_base_path,
        "BASE_IMAGE": "amp_test",
        "DEV_TOOLS_IMAGE_PROD": f"{ecr_base_path}/dev_tools:prod",
    }
    return default_params


class _TestClassHelper(hut.TestCase):
    def setUp(self) -> None:
        super().setUp()
        params = _get_default_params()
        ltasks.set_default_params(params)

    def tearDown(self) -> None:
        ltasks.reset_default_params()
        super().tearDown()


def _build_mock_context_returning_ok() -> invoke.MockContext:
    """
    Build a MockContext catching any command and returning rc=0.
    """
    ctx = invoke.MockContext(
        repeat=True, run={re.compile(".*"): invoke.Result(exited=0)}
    )
    return ctx


def _gh_login() -> None:
    """
    Log in inside GitHub.

    This is needed by GitHub action.
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


# #############################################################################


# TODO(gp): We should introspect lib_tasks.py and find all the functions decorated
#  with `@tasks`, instead of maintaining a (incomplete) list of tasks.
class TestDryRunTasks1(hut.TestCase):
    """
    - Run invoke in dry-run mode from command line
    - Compare the output to the golden outcomes
    """

    def test_print_setup(self) -> None:
        target = "print_setup"
        self._dry_run(target)

    def test_git_pull(self) -> None:
        target = "git_pull"
        self._dry_run(target)

    def test_git_pull_master(self) -> None:
        target = "git_pull_master"
        self._dry_run(target)

    def test_git_clean(self) -> None:
        target = "git_clean"
        self._dry_run(target)

    @pytest.mark.skipif(
        hsinte.is_inside_ci(), reason="In CI the output is different"
    )
    def test_docker_images_ls_repo(self) -> None:
        target = "docker_images_ls_repo"
        self._dry_run(target)

    def test_docker_ps(self) -> None:
        target = "docker_ps"
        self._dry_run(target)

    def test_docker_stats(self) -> None:
        target = "docker_stats"
        self._dry_run(target)

    def test_docker_kill_last(self) -> None:
        target = "docker_kill_last"
        self._dry_run(target)

    def test_docker_kill_all(self) -> None:
        target = "docker_kill_all"
        self._dry_run(target)

    def _dry_run(self, target: str) -> None:
        """
        Invoke the given target with dry run.

        This is used to test the commands that we can't actually
        execute.
        """
        cmd = f"invoke --dry {target} | grep -v INFO | grep -v 'code_version='"
        _, act = hsinte.system_to_string(cmd)
        act = hprint.remove_non_printable_chars(act)
        self.check_string(act)


# #############################################################################


class TestDryRunTasks2(_TestClassHelper):
    """
    - Call the invoke task directly from Python
    - `check_string()` that the sequence of commands issued by the target is the
      expected one using mocks to return ok for every system call.
    """

    def test_print_setup(self) -> None:
        target = "print_setup(ctx)"
        self._check_output(target)

    def test_git_pull(self) -> None:
        target = "git_pull(ctx)"
        self._check_output(target)

    def test_git_pull_master(self) -> None:
        target = "git_pull_master(ctx)"
        self._check_output(target)

    def test_git_clean(self) -> None:
        target = "git_clean(ctx)"
        self._check_output(target)

    def test_docker_images_ls_repo(self) -> None:
        target = "docker_images_ls_repo(ctx)"
        self._check_output(target, check=False)

    def test_docker_kill_all(self) -> None:
        target = "docker_kill_all(ctx)"
        self._check_output(target)

    def test_docker_kill_last(self) -> None:
        target = "docker_kill_last(ctx)"
        self._check_output(target)

    def test_docker_ps(self) -> None:
        target = "docker_ps(ctx)"
        self._check_output(target)

    def test_docker_pull(self) -> None:
        target = "docker_pull(ctx)"
        self._check_output(target, check=False)

    def test_docker_stats(self) -> None:
        target = "docker_stats(ctx)"
        self._check_output(target)

    def test_gh_create_pr1(self) -> None:
        _gh_login()
        target = "gh_create_pr(ctx, repo='amp', title='test')"
        self._check_output(target)

    def test_gh_create_pr2(self) -> None:
        _gh_login()
        target = (
            "gh_create_pr(ctx, body='hello_world', repo='amp', title='test')"
        )
        self._check_output(target)

    def test_gh_create_pr3(self) -> None:
        _gh_login()
        target = "gh_create_pr(ctx, draft=False, repo='amp', title='test')"
        self._check_output(target)

    def test_gh_issue_title(self) -> None:
        _gh_login()
        target = "gh_issue_title(ctx, 1)"
        self._check_output(target)

    def test_gh_workflow_list(self) -> None:
        _gh_login()
        target = "gh_workflow_list(ctx, branch='master')"
        self._check_output(target)

    # This is an action with side effects so we can't test it.
    # def test_gh_workflow_run(self) -> None:
    #     target = "gh_workflow_run(ctx)"
    #     self._check_output(target)

    def test_git_branch_files(self) -> None:
        # This test needs a reference to Git master branch.
        git.fetch_origin_master_if_needed()
        #
        target = "git_branch_files(ctx)"
        self._check_output(target)

    def test_git_clean(self) -> None:
        target = "git_clean(ctx)"
        self._check_output(target)

    def test_git_create_branch1(self) -> None:
        target = (
            "git_create_branch(ctx, branch_name='test', "
            "only_branch_from_master=False)"
        )
        self._check_output(target)

    def test_git_create_branch2(self) -> None:
        target = (
            "git_create_branch(ctx, issue_id=1, repo='amp', "
            "only_branch_from_master=False)"
        )
        self._check_output(target)

    def test_git_create_branch3(self) -> None:
        with self.assertRaises(AssertionError):
            target = (
                "git_create_branch(ctx, branch_name='test', issue_id=1, "
                "only_branch_from_master=False)"
            )
            self._check_output(target, check=False)

    # This is an action with side effects so we can't test it.
    # def test_git_delete_merged_branches(self) -> None:
    #     target = "git_delete_merged_branches(ctx)"
    #     self._check_output(target)

    def test_git_merge_master(self) -> None:
        target = "git_merge_master(ctx)"
        self._check_output(target)

    def test_lint1(self) -> None:
        target = "lint(ctx, modified=True)"
        # The output depends on the client, so don't check it.
        self._check_output(target, check=False)

    def test_lint2(self) -> None:
        target = "lint(ctx, branch=True)"
        # The output depends on the client, so don't check it.
        self._check_output(target, check=False)

    def test_lint3(self) -> None:
        file = __file__
        target = f"lint(ctx, files='{file}')"
        # The output depends on the client, so don't check it.
        self._check_output(target, check=False)

    def test_find_test_class1(self) -> None:
        class_name = self.__class__.__name__
        target = f"find_test_class(ctx, class_name='{class_name}')"
        self._check_output(target)

    # #########################################################################

    @pytest.mark.skipif(
        hsinte.is_inside_ci(), reason="In CI the output is different"
    )
    def test_docker_login(self) -> None:
        """
        Instead of using _build_mock_context_returning_ok(), set the return
        values more explicitly.
        """
        stdout = "aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49\n"
        ctx = invoke.MockContext(
            run={
                "aws --version": invoke.Result(stdout),
                re.compile("^docker login"): invoke.Result(exited=0),
                re.compile("^eval"): invoke.Result(exited=0),
            }
        )
        ltasks.docker_login(ctx)
        # Check the outcome.
        self._check_calls(ctx)

    # #########################################################################

    def _check_calls(self, ctx: invoke.MockContext) -> None:
        """
        check_string() the sequence of commands issued in the context.
        """
        act = "\n".join(map(str, ctx.run.mock_calls))
        act = hprint.remove_non_printable_chars(act)
        self.check_string(act)

    def _check_output(self, target: str, check: bool = True) -> None:
        """
        Dry run target checking that the sequence of commands issued is the
        expected one.
        """
        ctx = _build_mock_context_returning_ok()
        exec(f"ltasks.{target}")
        # Check the outcome.
        if check:
            self._check_calls(ctx)


# #############################################################################


class TestLibTasks1(hut.TestCase):
    """
    Test some auxiliary functions, e.g., `_get_build_tag`,
    `_get_gh_issue_title()`.
    """

    def test_get_build_tag1(self) -> None:
        code_ver = "amp-1.0.0"
        build_tag = ltasks._get_build_tag(code_ver)
        _LOG.debug("build_tag=%s", build_tag)

    def test_get_gh_issue_title1(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "amp"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "AmpTask1_Bridge_Python_and_R"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title2(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "lem"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "LemTask1_Adapt_the_usual_infra_from_my_codebase"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title3(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "dev_tools"
        act = ltasks._get_gh_issue_title(issue_id, repo)
        exp = "DevToolsTask1_Migration_from_amp"
        self.assert_equal(act, exp)

    def test_get_gh_issue_title4(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "current"
        _ = ltasks._get_gh_issue_title(issue_id, repo)


# #############################################################################


class TestLibTasksRemoveSpaces1(hut.TestCase):
    def test1(self) -> None:
        txt = r"""
            IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
                docker-compose \
                --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
                run \
                --rm \
                -l user=$USER_NAME \
                --entrypoint bash \
                user_space
            """
        act = ltasks._to_single_line_cmd(txt)
        exp = (
            "IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev"
            " docker-compose --file"
            " $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml"
            " run --rm -l user=$USER_NAME --entrypoint bash user_space"
        )
        self.assert_equal(act, exp, fuzzy_match=False)


# #############################################################################


class TestLibTasksGetDockerCmd1(_TestClassHelper):
    """
    Test `_get_docker_cmd()`.
    """

    @pytest.mark.skipif(
        not git.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash1(self) -> None:
        """
        Command for docker_bash target.
        """
        stage = "dev"
        base_image = ""
        cmd = "bash"
        service_name = "app"
        entrypoint = False
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            service_name=service_name,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            --entrypoint bash \
            app
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(
        not git.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash2(self) -> None:
        """
        Command for docker_bash with entrypoint.
        """
        stage = "local"
        base_image = ""
        cmd = "bash"
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage, base_image, cmd, print_docker_config=print_docker_config
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:local \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            app \
            bash
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(
        not git.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash3(self) -> None:
        """
        Command for docker_bash with some env vars.
        """
        stage = "local"
        base_image = ""
        cmd = "bash"
        extra_env_vars = ["PORT=9999", "SKIP_RUN=1"]
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            extra_env_vars=extra_env_vars,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:local \
        PORT=9999 \
        SKIP_RUN=1 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            app \
            bash
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(
        not git.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_docker_bash4(self) -> None:
        stage = "dev"
        base_image = ""
        cmd = "bash"
        entrypoint = False
        print_docker_config = False
        act = ltasks._get_docker_cmd(
            stage,
            base_image,
            cmd,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            --entrypoint bash \
            app
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @pytest.mark.skipif(
        not git.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_jupyter1(self) -> None:
        stage = "dev"
        base_image = ""
        port = 9999
        self_test = True
        print_docker_config = False
        act = ltasks._get_docker_jupyter_cmd(
            stage,
            base_image,
            port,
            self_test,
            print_docker_config=print_docker_config,
        )
        act = hut.purify_txt_from_client(act)
        exp = r"""
        IMAGE=665840871993.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
        PORT=9999 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            -l user=$USER_NAME \
            --service-ports \
            jupyter_server_test
        """
        self.assert_equal(act, exp, fuzzy_match=True)


# #############################################################################


class TestLibRunTests1(hut.TestCase):
    """
    Test `_build_run_command_line()`.
    """

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
        exp = (
            r'pytest -m "not slow and not superslow" --cov=. --cov-branch'
            r" --cov-report term-missing --cov-report html --collect-only"
        )
        self.assert_equal(act, exp)

    @pytest.mark.skipif(not git.is_lem(), reason="Only run in lem")
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
        exp = r"pytest --ignore amp"
        self.assert_equal(act, exp)

    @pytest.mark.skipif(not git.is_amp(), reason="Only run in amp")
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
        #
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
        exp = (
            "pytest TestLibRunTests1.test_run_fast_tests4/tmp.scratch/"
            "test/test_that.py"
        )
        self.assert_equal(act, exp)


# #############################################################################


class TestLibTasksRunTests1(hut.TestCase):
    """
    Test `_find_test_files()`, `_find_test_decorator()`.
    """

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
        exp = ["helpers/test/test_lib_tasks.py::TestLibTasksRunTests1"]
        self.assert_equal(str(act), str(exp))

    def test_find_test_class2(self) -> None:
        """
        Find the current test class.
        """
        file_names = [__file__]
        #
        file_names = ltasks._find_test_class("TestLibTasksRunTests1", file_names)
        act = hut.purify_file_names(file_names)
        exp = ["helpers/test/test_lib_tasks.py::TestLibTasksRunTests1"]
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
            "helpers/test/TestLibTasksRunTests1.test_find_test_class3/tmp.scratch/"
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
            "helpers/test/TestLibTasksRunTests1.test_find_test_decorator1/"
            "tmp.scratch/test/test_that.py"
        ]
        self.assert_equal(str(act), str(exp))

    @pytest.mark.skipif(not git.is_amp(), reason="Only run in amp")
    def test_find_test_decorator2(self) -> None:
        """
        Find test functions in the "no_container" test list.
        """
        file_names = ["test/test_tasks.py"]
        act = ltasks._find_test_decorator("no_container", file_names)
        act = hut.purify_file_names(act)
        exp = ["test/test_tasks.py"]
        self.assert_equal(str(act), str(exp))


# #############################################################################


class TestLibTasksGitCreatePatch1(hut.TestCase):
    """
    Test `git_create_patch()`.
    """

    def test_tar_modified1(self) -> None:
        """
        Exercise the code for:

        > invoke git_create_patch --mode="tar" --branch
        """
        # This test needs a reference to Git master branch.
        git.fetch_origin_master_if_needed()
        #
        modified = True
        branch = False
        files = ""
        self._helper(modified, branch, files)

    def test_tar_branch1(self) -> None:
        """
        Exercise the code for:

        > invoke git_create_patch --mode="tar" --modified
        """
        modified = False
        branch = True
        files = ""
        self._helper(modified, branch, files)

    def test_tar_files1(self) -> None:
        """
        Exercise the code for:

        > invoke git_create_patch --mode="tar" --files "this file"
        """
        # This test needs a reference to Git master branch.
        git.fetch_origin_master_if_needed()
        #
        ctx = _build_mock_context_returning_ok()
        mode = "tar"
        modified = False
        branch = False
        files = __file__
        ltasks.git_create_patch(ctx, mode, modified, branch, files)

    def test_tar_files2(self) -> None:
        """
        Exercise the code for:

        > invoke git_create_patch --mode="diff" --files "this file"

        In this case one needs to specify --branch or --modified.
        """
        # This test needs a reference to Git master branch.
        git.fetch_origin_master_if_needed()
        #
        ctx = _build_mock_context_returning_ok()
        mode = "diff"
        modified = False
        branch = False
        files = __file__
        with self.assertRaises(AssertionError):
            ltasks.git_create_patch(ctx, mode, modified, branch, files)

    @staticmethod
    def _helper(modified: bool, branch: bool, files: str) -> None:
        ctx = _build_mock_context_returning_ok()
        #
        mode = "tar"
        ltasks.git_create_patch(ctx, mode, modified, branch, files)
        #
        mode = "diff"
        ltasks.git_create_patch(ctx, mode, modified, branch, files)
