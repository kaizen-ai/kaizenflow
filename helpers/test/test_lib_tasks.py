# This should only test helper functions from `lib_tasks.py`.
# `test_tasks.py` associated to `tasks.py` should test specific task targets.

import logging
import os
import re
from typing import Dict, Optional

import invoke
import pytest

import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import helpers.lib_tasks as hlibtask

_LOG = logging.getLogger(__name__)


def _get_default_params() -> Dict[str, str]:
    """
    Get fake params pointing to a different image so we can test the code
    without affecting the official images.
    """
    ecr_base_path = os.environ["AM_ECR_BASE_PATH"]
    default_params = {
        "AM_ECR_BASE_PATH": ecr_base_path,
        "BASE_IMAGE": "amp_test",
        "DEV_TOOLS_IMAGE_PROD": f"{ecr_base_path}/dev_tools:prod",
    }
    return default_params


class _LibTasksTestCase(hunitest.TestCase):
    """
    Test class injecting default parameters in the `lib_tasks` singleton on
    `setUp()` and cleaning up the singleton on `tearDown()`.
    """

    def setUp(self) -> None:
        super().setUp()
        params = _get_default_params()
        hlibtask.set_default_params(params)

    def tearDown(self) -> None:
        hlibtask.reset_default_params()
        super().tearDown()


# #############################################################################


def _build_mock_context_returning_ok() -> invoke.MockContext:
    """
    Build a MockContext catching any command and returning rc=0.
    """
    ctx = invoke.MockContext(
        repeat=True, run={re.compile(".*"): invoke.Result(exited=0)}
    )
    return ctx


class _CheckDryRunTestCase(hunitest.TestCase):
    """
    Test class running an invoke target with/without dry-run and checking that
    the issued commands are what is expected.
    """

    def _check_calls(self, ctx: invoke.MockContext) -> None:
        """
        `check_string()` the sequence of commands issued in the context.
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
        # pylint: disable=exec-used
        exec(f"hlibtask.{target}")
        # pylint: enable=exec-used
        # Check the outcome.
        if check:
            self._check_calls(ctx)


# #############################################################################


def _gh_login() -> None:
    """
    Log in inside GitHub.

    This is needed by GitHub actions.
    """
    env_var = "GH_ACTION_ACCESS_TOKEN"
    if os.environ.get(env_var, None):
        # If the env var exists and it's not None.
        _LOG.warning("Using env var '%s' to log in GitHub", env_var)
        # For debugging only (see AmpTask1864).
        if False:

            def _cmd(cmd):
                hsystem.system(
                    cmd,
                    suppress_output=False,
                    log_level="echo",
                    abort_on_error=False,
                )

            for cmd in [
                "ls -l $HOME/.config",
                "ls -l $HOME/.config/gh",
                "ls -l $HOME/.config/gh/config.yml",
                "touch $HOME/.config/gh/config.yml",
                "ls -l $HOME/.config/gh/config.yml",
            ]:
                _cmd(cmd)
        cmd = "echo $GH_ACTION_ACCESS_TOKEN | gh auth login --with-token"
        hsystem.system(cmd)
    # Check that we are logged in.
    cmd = "gh auth status"
    hsystem.system(cmd)


class TestGhLogin1(hunitest.TestCase):
    def test_gh_login(self) -> None:
        _gh_login()


# #############################################################################


# TODO(gp): We should group the tests by what is tested and not how it's
# tested. E.g. TestDryRunTasks1::test_print_setup and
# TestDryRunTasks2::test_print_setup should go together in a class.


class TestDryRunTasks1(hunitest.TestCase):
    """
    - Run invoke in dry-run mode from command line
    - Compare the output to the golden outcomes
    """

    # TODO(gp): -> TestGitCommands1

    def dry_run(
        self, target: str, dry_run: bool = True, check_string: bool = True
    ) -> None:
        """
        Invoke the given target with dry run.

        This is used to test the commands that we can't actually
        execute.
        """
        opts = "--dry" if dry_run else ""
        # TODO(vitalii): While deploying the container versioning
        # we disable the check in the unit tests. Remove `SKIP_VERSION_CHECK=1`
        # after CmampTask570 is fixed.
        cmd = f"SKIP_VERSION_CHECK=1 invoke {opts} {target} | grep -v INFO | grep -v '>>ENV<<:'"
        _, act = hsystem.system_to_string(cmd)
        act = hprint.remove_non_printable_chars(act)
        if check_string:
            self.check_string(act)

    # #########################################################################

    # TODO(gp): We can't test this since amp and cmamp have now different base image.
    # def test_print_setup(self) -> None:
    #     target = "print_setup"
    #     self.dry_run(target)

    def test_git_pull(self) -> None:
        target = "git_pull"
        self.dry_run(target)

    def test_git_fetch_master(self) -> None:
        target = "git_fetch_master"
        self.dry_run(target)

    def test_git_clean(self) -> None:
        target = "git_clean"
        self.dry_run(target)

    # #########################################################################
    # TODO(gp): -> TestDockerCommands1

    @pytest.mark.skipif(
        hsystem.is_inside_ci(), reason="In CI the output is different"
    )
    def test_docker_images_ls_repo(self) -> None:
        target = "docker_images_ls_repo"
        # TODO(gp): amp and cmamp have different version of aws cli and so the
        # output is different.
        check_string = False
        self.dry_run(target, check_string=check_string)

    def test_docker_ps(self) -> None:
        target = "docker_ps"
        self.dry_run(target)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_docker_stats(self) -> None:
        target = "docker_stats"
        self.dry_run(target)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_docker_kill_last(self) -> None:
        target = "docker_kill"
        self.dry_run(target)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_docker_kill_all(self) -> None:
        target = "docker_kill --all"
        self.dry_run(target)


# #############################################################################


@pytest.mark.slow(reason="Around 7s")
@pytest.mark.skipif(
    not hgit.is_in_amp_as_supermodule(),
    reason="Run only in amp as super-module",
)
class TestDryRunTasks2(_LibTasksTestCase, _CheckDryRunTestCase):
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

    def test_git_fetch_master(self) -> None:
        target = "git_fetch_master(ctx)"
        self._check_output(target)

    def test_git_clean(self) -> None:
        target = "git_clean(ctx)"
        self._check_output(target)

    def test_git_clean2(self) -> None:
        target = "git_clean(ctx, dry_run=False)"
        self._check_output(target)

    # #########################################################################

    def test_docker_images_ls_repo(self) -> None:
        target = "docker_images_ls_repo(ctx)"
        self._check_output(target, check=False)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_docker_kill_all(self) -> None:
        target = "docker_kill(ctx, all=True)"
        self._check_output(target)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_docker_kill_last(self) -> None:
        target = "docker_kill(ctx)"
        self._check_output(target)

    def test_docker_ps(self) -> None:
        target = "docker_ps(ctx)"
        self._check_output(target)

    def test_docker_pull(self) -> None:
        target = "docker_pull(ctx)"
        self._check_output(target, check=False)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_docker_stats(self) -> None:
        target = "docker_stats(ctx)"
        self._check_output(target)

    # #########################################################################
    # TODO(gp): -> TestGhCommands1

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_gh_create_pr1(self) -> None:
        _gh_login()
        target = "gh_create_pr(ctx, repo_short_name='amp', title='test')"
        self._check_output(target)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_gh_create_pr2(self) -> None:
        _gh_login()
        target = "gh_create_pr(ctx, body='hello_world', repo_short_name='amp', title='test')"
        self._check_output(target)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_gh_create_pr3(self) -> None:
        _gh_login()
        target = (
            "gh_create_pr(ctx, draft=False, repo_short_name='amp', title='test')"
        )
        self._check_output(target)

    def test_gh_issue_title(self) -> None:
        _gh_login()
        target = "gh_issue_title(ctx, 1)"
        self._check_output(target)

    @pytest.mark.skipif(not hgit.is_amp(), reason="Only run in amp")
    def test_gh_workflow_list(self) -> None:
        _gh_login()
        target = "gh_workflow_list(ctx, filter_by_branch='master')"
        self._check_output(target)

    # This is an action with side effects so we can't test it.
    # def test_gh_workflow_run(self) -> None:
    #     target = "gh_workflow_run(ctx)"
    #     self._check_output(target)

    # #########################################################################
    # TODO(gp): -> TestGitCommands1
    def test_git_branch_files(self) -> None:
        # This test needs a reference to Git master branch.
        hgit.fetch_origin_master_if_needed()
        #
        target = "git_branch_files(ctx)"
        self._check_output(target)

    def test_git_create_branch1(self) -> None:
        _gh_login()
        target = (
            "git_create_branch(ctx, branch_name='AmpTask123_test', "
            "only_branch_from_master=False)"
        )
        self._check_output(target)

    def test_git_create_branch2(self) -> None:
        _gh_login()
        target = (
            "git_create_branch(ctx, issue_id=1, repo_short_name='amp', "
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
        target = "git_merge_master(ctx, abort_if_not_clean=False)"
        self._check_output(target)

    # #########################################################################
    # TODO(gp): -> TestLintCommands1

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_lint1(self) -> None:
        target = "lint(ctx, modified=True)"
        # The output depends on the client, so don't check it.
        self._check_output(target, check=False)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
    def test_lint2(self) -> None:
        target = "lint(ctx, branch=True)"
        # The output depends on the client, so don't check it.
        self._check_output(target, check=False)

    @pytest.mark.skip(
        reason="AmpTask1347: Add support for mocking `system*()` "
        "functions to unit test"
    )
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
        hsystem.is_inside_ci(), reason="In CI the output is different"
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
        hlibtask.docker_login(ctx)
        # Check the outcome.
        # self._check_calls(ctx)


# #############################################################################

# TODO(gp): Run test coverage with
# > i run_fast_slow_tests \
#       --pytest-opts="helpers/test/test_lib_tasks.py test/test_tasks.py" \
#       --coverage

# TODO(gp): Add tests for:
# - print_tasks
# - git_files
# - git_last_commit_files
# - check_python_files
# - docker_stats
# - traceback (with checked in file)
# - lint

# #############################################################################


class Test_generate_compose_file1(hunitest.TestCase):
    def helper(
        self,
        use_privileged_mode: bool = False,
        use_sibling_container: bool = False,
        shared_data_dirs: Optional[Dict[str, str]] = None,
        mount_as_submodule: bool = False,
        use_network_mode_host: bool = True,
        use_main_network: bool = False,
    ) -> None:
        txt = []
        #
        params = [
            "use_privileged_mode",
            "use_sibling_container",
            "shared_data_dirs",
            "mount_as_submodule",
            "use_network_mode_host",
        ]
        txt_tmp = hprint.to_str(" ".join(params))
        txt.append(txt_tmp)
        #
        file_name = None
        txt_tmp = hlibtask._generate_docker_compose_file(
            use_privileged_mode,
            use_sibling_container,
            shared_data_dirs,
            mount_as_submodule,
            use_network_mode_host,
            use_main_network,
            file_name,
        )
        txt_tmp = hunitest.filter_text("AM_HOST_NAME|AM_HOST_OS_NAME", txt_tmp)
        txt.append(txt_tmp)
        #
        txt = "\n".join(txt)
        txt = hunitest.filter_text(r"working_dir", txt)
        self.check_string(txt)

    def test1(self) -> None:
        self.helper(use_privileged_mode=True)

    def test2(self) -> None:
        self.helper(shared_data_dirs={"/data/shared": "/shared_data"})

    def test3(self) -> None:
        self.helper(use_main_network=True)


# #############################################################################


class TestLibTasks1(hunitest.TestCase):
    """
    Test some auxiliary functions, e.g., `_get_gh_issue_title()`.
    """

    def test_get_gh_issue_title1(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "amp"
        act = hlibtask._get_gh_issue_title(issue_id, repo)
        exp = (
            "AmpTask1_Bridge_Python_and_R",
            "https://github.com/alphamatic/amp/issues/1",
        )
        self.assert_equal(str(act), str(exp))

    @pytest.mark.skipif(
        not hgit.is_cmamp(),
        reason="CmampTask #683.",
    )
    def test_get_gh_issue_title3(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "dev_tools"
        act = hlibtask._get_gh_issue_title(issue_id, repo)
        exp = (
            "DevToolsTask1_Migration_from_amp",
            "https://github.com/alphamatic/dev_tools/issues/1",
        )
        self.assert_equal(str(act), str(exp))

    def test_get_gh_issue_title4(self) -> None:
        _gh_login()
        issue_id = 1
        repo = "current"
        _ = hlibtask._get_gh_issue_title(issue_id, repo)


# #############################################################################


class TestLibTasksRemoveSpaces1(hunitest.TestCase):
    def test1(self) -> None:
        txt = r"""
            IMAGE=*****.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev \
                docker-compose \
                --file $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml \
                run \
                --rm \
                -l user=$USER_NAME \
                --entrypoint bash \
                user_space
            """
        act = hlibtask._to_single_line_cmd(txt)
        exp = (
            "IMAGE=*****.dkr.ecr.us-east-1.amazonaws.com/amp_test:dev"
            " docker-compose --file"
            " $GIT_ROOT/devops/compose/docker-compose_as_submodule.yml"
            " run --rm -l user=$USER_NAME --entrypoint bash user_space"
        )
        self.assert_equal(act, exp, fuzzy_match=False)


# #############################################################################


class TestLibTasksGetDockerCmd1(_LibTasksTestCase):
    """
    Test `_get_docker_compose_cmd()`.
    """

    # TODO(gp): After using a single docker file as part of AmpTask2308
    #  "Update_amp_container" we can probably run these tests in any repo, so
    #  we should be able to remove this `skipif`.
    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash1(self) -> None:
        """
        Command for docker_bash target.
        """
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        cmd = "bash"
        service_name = "app"
        entrypoint = False
        print_docker_config = False
        act = hlibtask._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            service_name=service_name,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            --entrypoint bash \
            app
        """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash2(self) -> None:
        """
        Command for docker_bash with entrypoint.
        """
        base_image = ""
        stage = "local"
        version = "1.0.0"
        cmd = "bash"
        print_docker_config = False
        act = hlibtask._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            print_docker_config=print_docker_config,
        )
        exp = r"""IMAGE=$AM_ECR_BASE_PATH/amp_test:local-$USER_NAME-1.0.0 \
                docker-compose \
                --file $GIT_ROOT/devops/compose/docker-compose.yml \
                --env-file devops/env/default.env \
                run \
                --rm \
                --name $USER_NAME.amp_test.app.app \
                app \
                bash """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_bash3(self) -> None:
        """
        Command for docker_bash with some env vars.
        """
        base_image = ""
        stage = "local"
        version = "1.0.0"
        cmd = "bash"
        extra_env_vars = ["PORT=9999", "SKIP_RUN=1"]
        print_docker_config = False
        act = hlibtask._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            extra_env_vars=extra_env_vars,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:local-$USER_NAME-1.0.0 \
        PORT=9999 \
        SKIP_RUN=1 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            app \
            bash
        """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_supermodule(),
        reason="Only run in amp as supermodule",
    )
    def test_docker_bash4(self) -> None:
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        cmd = "bash"
        entrypoint = False
        print_docker_config = False
        act = hlibtask._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            --entrypoint bash \
            app
        """
        self._check(act, exp)

    def test_docker_bash5(self) -> None:
        """
        Command for running through a shell.
        """
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        cmd = "ls && cd .."
        entrypoint = True
        print_docker_config = False
        use_bash = True
        act = hlibtask._get_docker_compose_cmd(
            base_image,
            stage,
            version,
            cmd,
            entrypoint=entrypoint,
            print_docker_config=print_docker_config,
            use_bash=use_bash,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.app.app \
            app \
            bash -c 'ls && cd ..'
        """
        self._check(act, exp)

    @pytest.mark.skipif(
        not hgit.is_in_amp_as_submodule(), reason="Only run in amp as submodule"
    )
    def test_docker_jupyter1(self) -> None:
        base_image = ""
        stage = "dev"
        version = "1.0.0"
        port = 9999
        self_test = True
        print_docker_config = False
        act = hlibtask._get_docker_jupyter_cmd(
            base_image,
            stage,
            version,
            port,
            self_test,
            print_docker_config=print_docker_config,
        )
        exp = r"""
        IMAGE=$AM_ECR_BASE_PATH/amp_test:dev-1.0.0 \
        PORT=9999 \
            docker-compose \
            --file $GIT_ROOT/devops/compose/docker-compose.yml \
            --env-file devops/env/default.env \
            run \
            --rm \
            --name $USER_NAME.amp_test.jupyter_server_test.app \
            --service-ports \
            jupyter_server_test
        """
        self._check(act, exp)

    def _check(self, act: str, exp: str) -> None:
        act = hunitest.purify_txt_from_client(act)
        # This is required when different repos run Docker with user vs root / remap.
        act = hunitest.filter_text("--user", act)
        # Remove current timestamp (e.g., `20220317_232120``) from the `--name`
        # so that the tests pass.
        timestamp_regex = r"\.\d{8}_\d{6}"
        act = re.sub(timestamp_regex, "", act)
        self.assert_equal(act, exp, fuzzy_match=True)


# #############################################################################


class Test_parse_linter_output1(hunitest.TestCase):
    """
    Test `_parse_linter_output()`.
    """

    def test1(self) -> None:
        # pylint: disable=line-too-long
        txt = r"""
Tabs remover.............................................................Passed
autoflake................................................................Passed
isort....................................................................Failed
- hook id: isort
- files were modified by this hook

INFO: > cmd='/app/linters/amp_isort.py core/dataflow/builders.py core/test/test_core.py core/dataflow/nodes.py helpers/printing.py helpers/lib_tasks.py core/data_adapters.py helpers/test/test_dbg.py helpers/unit_test.py core/dataflow/core.py core/finance.py core/dataflow/models.py core/dataflow/visualization.py helpers/dbg.py helpers/test/test_git.py core/dataflow/runners.py core/dataflow/test/test_visualization.py helpers/test/test_lib_tasks.py'
Fixing /src/amp/core/dataflow/builders.py
Fixing /src/amp/core/data_adapters.py
Fixing /src/amp/core/dataflow/models.py

black....................................................................Passed
flake8...................................................................Failed
- hook id: flake8
- exit code: 6

INFO: > cmd='/app/linters/amp_flake8.py core/dataflow/builders.py core/test/test_core.py core/dataflow/nodes.py helpers/printing.py'
core/dataflow/nodes.py:601:9: F821 undefined name '_check_col_names'
core/dataflow/nodes.py:608:9: F821 undefined name '_check_col_names'
INFO: > cmd='/app/linters/amp_flake8.py helpers/lib_tasks.py core/data_adapters.py helpers/test/test_dbg.py helpers/unit_test.py'
helpers/lib_tasks.py:302:12: W605 invalid escape sequence '\g'
helpers/lib_tasks.py:309:27: W605 invalid escape sequence '\s'
helpers/lib_tasks.py:309:31: W605 invalid escape sequence '\S'
helpers/lib_tasks.py:309:35: W605 invalid escape sequence '\('
helpers/lib_tasks.py:2184:9: F541 f-string is missing placeholders
helpers/test/test_dbg.py:71:25: F821 undefined name 'y'
INFO: > cmd='/app/linters/amp_flake8.py core/dataflow/core.py core/finance.py core/dataflow/models.py core/dataflow/visualization.py'
core/finance.py:250:5: E301 expected 1 blank line, found 0
INFO: > cmd='/app/linters/amp_flake8.py helpers/dbg.py helpers/test/test_git.py core/dataflow/runners.py core/dataflow/test/test_visualization.py'

INFO: > cmd='/app/linters/amp_flake8.py helpers/test/test_lib_tasks.py'
helpers/test/test_lib_tasks.py:225:5: F811 redefinition of unused 'test_git_clean' from line 158

doc_formatter............................................................Passed
pylint...................................................................
"""
        # pylint: enable=line-too-long
        act = hlibtask._parse_linter_output(txt)
        # pylint: disable=line-too-long
        exp = r"""
core/dataflow/nodes.py:601:9:[flake8] F821 undefined name '_check_col_names'
core/dataflow/nodes.py:608:9:[flake8] F821 undefined name '_check_col_names'
core/finance.py:250:5:[flake8] E301 expected 1 blank line, found 0
helpers/lib_tasks.py:2184:9:[flake8] F541 f-string is missing placeholders
helpers/lib_tasks.py:302:12:[flake8] W605 invalid escape sequence '\g'
helpers/lib_tasks.py:309:27:[flake8] W605 invalid escape sequence '\s'
helpers/lib_tasks.py:309:31:[flake8] W605 invalid escape sequence '\S'
helpers/lib_tasks.py:309:35:[flake8] W605 invalid escape sequence '\('
helpers/test/test_dbg.py:71:25:[flake8] F821 undefined name 'y'
helpers/test/test_lib_tasks.py:225:5:[flake8] F811 redefinition of unused 'test_git_clean' from line 158
"""
        # pylint: enable=line-too-long
        self.assert_equal(act, exp, fuzzy_match=True)

    def test2(self) -> None:
        # pylint: disable=line-too-long
        txt = """
BUILD_TAG='dev_tools-1.0.0-20210513-DevTools196_Switch_from_makefile_to_invoke-e9ee441a1144883d2e8835740cba4dc09e7d4f2a'
PRE_COMMIT_HOME=/root/.cache/pre-commit
total 44
-rw-r--r-- 1 root root   109 May 13 12:54 README
-rw------- 1 root root 20480 May 13 12:54 db.db
drwx------ 4 root root  4096 May 13 12:54 repo0a8anbxn
drwx------ 8 root root  4096 May 13 12:54 repodla_5o4g
drwx------ 9 root root  4096 May 13 12:54 repoeydyv2ho
drwx------ 7 root root  4096 May 13 12:54 repokrr14v6z
drwx------ 4 root root  4096 May 13 12:54 repozqvepk0i
5
which python: /venv/bin/python
check pandas package: <module 'pandas' from '/venv/lib/python3.8/site-packages/pandas/__init__.py'>
PATH=/app/linters:/app/dev_scripts/eotebooks:/app/documentation/scripts:/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
PYTHONPATH=/app:
cmd: 'pre-commit run -c /app/.pre-commit-config.yaml --files ./core/dataflow/builders.py'
Don't commit to branch...................................................^[[42mPassed^[[m
Check for merge conflicts................................................^[[42mPassed^[[m
Trim Trailing Whitespace.................................................^[[42mPassed^[[m
Fix End of Files.........................................................^[[42mPassed^[[m
Check for added large files..............................................^[[42mPassed^[[m
CRLF end-lines remover...................................................^[[42mPassed^[[m
Tabs remover.............................................................^[[42mPassed^[[m
autoflake................................................................^[[42mPassed^[[m
isort....................................................................^[[42mPassed^[[m
black....................................................................^[[42mPassed^[[m
flake8...................................................................^[[42mPassed^[[m
doc_formatter............................................................^[[42mPassed^[[m
pylint...................................................................^[[41mFailed^[[m
^[[2m- hook id: pylint^[[m
^[[2m- exit code: 3^[[m

^[[33mWARNING^[[0m: No module named 'pandas'
^[[33mWARNING^[[0m: No module named 'matplotlib'
^[[0m^[[36mINFO^[[0m: > cmd='/app/linters/amp_pylint.py core/dataflow/builders.py'
************* Module core.dataflow.builders
core/dataflow/builders.py:104: [R1711(useless-return), DagBuilder.get_column_to_tags_mapping] Useless return at end of function or method
core/dataflow/builders.py:195: [W0221(arguments-differ), ArmaReturnsBuilder.get_dag] Parameters differ from overridden 'get_dag' method

mypy.....................................................................^[[41mFailed^[[m
^[[2m- hook id: mypy^[[m
^[[2m- exit code: 3^[[m

^[[0m^[[36mINFO^[[0m: > cmd='/app/linters/amp_mypy.py core/dataflow/builders.py'
core/dataflow/builders.py:125: error: Returning Any from function declared to return "str"  [no-any-return]
core/dataflow/builders.py:195: error: Argument 2 of "get_dag" is incompatible with supertype "DagBuilder"; supertype defines the argument type as "Optional[Any]"  [override]
core/dataflow/builders.py:195: note: This violates the Liskov substitution principle
        """
        # pylint: enable=line-too-long
        act = hlibtask._parse_linter_output(txt)
        # pylint: disable=line-too-long
        exp = r"""
core/dataflow/builders.py:104:[pylint] [R1711(useless-return), DagBuilder.get_column_to_tags_mapping] Useless return at end of function or method
core/dataflow/builders.py:125:[mypy] error: Returning Any from function declared to return "str"  [no-any-return]
core/dataflow/builders.py:195:[mypy] error: Argument 2 of "get_dag" is incompatible with supertype "DagBuilder"; supertype defines the argument type as "Optional[Any]"  [override]
core/dataflow/builders.py:195:[mypy] note: This violates the Liskov substitution principle
core/dataflow/builders.py:195:[pylint] [W0221(arguments-differ), ArmaReturnsBuilder.get_dag] Parameters differ from overridden 'get_dag' method
"""
        # pylint: enable=line-too-long
        self.assert_equal(act, exp, fuzzy_match=True)


# #############################################################################


class Test_get_files_to_process1(hunitest.TestCase):
    """
    We can't check the outcome so we just execute the code.
    """

    def test_modified1(self) -> None:
        """
        Retrieve files modified in this client.
        """
        modified = True
        branch = False
        last_commit = False
        all_ = False
        files_from_user = ""
        mutually_exclusive = True
        remove_dirs = True
        _ = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files_from_user,
            mutually_exclusive,
            remove_dirs,
        )

    @pytest.mark.skipif(
        hgit.get_branch_name() != "master",
        reason="This test makes sense for a branch",
    )
    def test_branch1(self) -> None:
        """
        Retrieved files modified in this client.
        """
        # This test needs a reference to Git master branch.
        hgit.fetch_origin_master_if_needed()
        #
        modified = False
        branch = True
        last_commit = False
        all_ = False
        files_from_user = ""
        mutually_exclusive = True
        remove_dirs = True
        _ = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files_from_user,
            mutually_exclusive,
            remove_dirs,
        )

    def test_last_commit1(self) -> None:
        """
        Retrieved files modified in the last commit.
        """
        modified = False
        branch = False
        last_commit = True
        all_ = False
        files_from_user = ""
        mutually_exclusive = True
        remove_dirs = True
        _ = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files_from_user,
            mutually_exclusive,
            remove_dirs,
        )

    def test_files1(self) -> None:
        """
        Pass through files from user.
        """
        modified = False
        branch = False
        last_commit = False
        all_ = False
        files_from_user = __file__
        mutually_exclusive = True
        remove_dirs = True
        files = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files_from_user,
            mutually_exclusive,
            remove_dirs,
        )
        self.assertEqual(files, [__file__])

    def test_files2(self) -> None:
        """
        Pass through files from user.

        Use two types of paths we don't want to process:
          - non-existent python file
          - pattern "/*" that matches no files
        """
        modified = False
        branch = False
        last_commit = False
        all_ = False
        files_from_user = "testfile1.py testfiles1/*"
        mutually_exclusive = True
        remove_dirs = True
        files = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files_from_user,
            mutually_exclusive,
            remove_dirs,
        )
        self.assertEqual(files, [])

    def test_files3(self) -> None:
        """
        Pass through files from user.

        Use the sequence of paths separated by newlines.
        """
        modified = False
        branch = False
        last_commit = False
        all_ = False
        # Specify the number of toy files.
        n_toy_files = 4
        files_from_user = []
        # Get root directory.
        root_dir = hgit.get_client_root(super_module=False)
        # Generate toy files and store their paths.
        for file_num in range(n_toy_files):
            # Build the name of the test file.
            file_name = f"test_toy{str(file_num)}.tmp.py"
            # Build the path to the test file.
            test_path = os.path.join(root_dir, file_name)
            # Create the empty toy file.
            hio.to_file(test_path, "")
            files_from_user.append(test_path)
        mutually_exclusive = True
        remove_dirs = True
        # Join the names with `\n` separator.
        joined_files_from_user = "\n".join(files_from_user)
        files = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            joined_files_from_user,
            mutually_exclusive,
            remove_dirs,
        )
        # Remove the toy files.
        for path in files_from_user:
            hio.delete_file(path)
        self.assertEqual(files, files_from_user)

    def test_assert1(self) -> None:
        """
        Test that --modified and --branch together cause an assertion.
        """
        modified = True
        branch = True
        last_commit = False
        all_ = True
        files_from_user = ""
        mutually_exclusive = True
        remove_dirs = True
        with self.assertRaises(AssertionError) as cm:
            hlibtask._get_files_to_process(
                modified,
                branch,
                last_commit,
                all_,
                files_from_user,
                mutually_exclusive,
                remove_dirs,
            )
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        '3'
        ==
        '1'
        Specify only one among --modified, --branch, --last-commit, --all_files, and --files
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_assert2(self) -> None:
        """
        Test that --modified and --files together cause an assertion if
        `mutually_exclusive=True`.
        """
        modified = True
        branch = False
        last_commit = False
        all_ = False
        files_from_user = __file__
        mutually_exclusive = True
        remove_dirs = True
        with self.assertRaises(AssertionError) as cm:
            hlibtask._get_files_to_process(
                modified,
                branch,
                last_commit,
                all_,
                files_from_user,
                mutually_exclusive,
                remove_dirs,
            )
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        '2'
        ==
        '1'
        Specify only one among --modified, --branch, --last-commit, --all_files, and --files
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_assert3(self) -> None:
        """
        Test that --modified and --files together don't cause an assertion if
        `mutually_exclusive=False`.
        """
        modified = True
        branch = False
        last_commit = False
        all_ = False
        files_from_user = __file__
        mutually_exclusive = False
        remove_dirs = True
        files = hlibtask._get_files_to_process(
            modified,
            branch,
            last_commit,
            all_,
            files_from_user,
            mutually_exclusive,
            remove_dirs,
        )
        self.assertEqual(files, [__file__])


# #############################################################################


class TestFailing(hunitest.TestCase):
    """
    Run a test that fails based on AM_FORCE_TEST_FAIL environment variable.
    """

    def test_failing(self) -> None:
        if os.environ.get("AM_FORCE_TEST_FAIL", "") == "1":
            self.fail("test failed succesfully")
