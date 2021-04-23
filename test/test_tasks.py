import logging
import re
from typing import Dict

import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsi
import helpers.unit_test as hut

dbg.init_logger()
_LOG = logging.getLogger(__name__)

try:
    import invoke

    import tasks
except ModuleNotFoundError as e:
    print("Can't find invoke: %s" % str(e))

    class Dummy:
        def __init__(self) -> None:
            self.MockContext = None

    invoke = Dummy()


class TestDryRunTasks1(hut.TestCase):
    def test_print_setup1(self) -> None:
        target = "print_setup"
        self._dry_run(target)

    def test_print_setup2(self) -> None:
        target = "print_setup"
        self._check_output(target)

    def test_git_pull1(self) -> None:
        target = "git_pull"
        self._dry_run(target)

    def test_git_pull2(self) -> None:
        target = "git_pull"
        self._check_output(target)

    def test_git_pull_master1(self) -> None:
        target = "git_pull_master"
        self._dry_run(target)

    def test_git_pull_master2(self) -> None:
        target = "git_pull_master"
        self._check_output(target)

    def test_git_clean1(self) -> None:
        target = "git_clean"
        self._dry_run(target)

    def test_git_clean2(self) -> None:
        target = "git_clean"
        self._check_output(target)

    def test_docker_login(self) -> None:
        stdout = "aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49\n"
        ctx = invoke.MockContext(
            run={
                "aws --version": invoke.Result(stdout),
                re.compile("^docker login"): invoke.Result(exited=0),
                re.compile("^eval"): invoke.Result(exited=0),
            }
        )
        tasks.docker_login(ctx)
        # Check the outcome.
        self._check_calls(ctx)

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
        cmd = "invoke --dry " + target
        _, act = hsi.system_to_string(cmd)
        # TODO(gp): Unclear why pylint can't find this function.
        # pylint: disable=no-member
        act = hprint.remove_non_printable_chars(act)
        # pylint: enable=no-member
        self.check_string(act)

    def _build_mock_context_returning_ok(self) -> invoke.MockContext:
        """
        Build a MockContext catching any command and returning rc=0.
        """
        ctx = invoke.MockContext(
            repeat=True, run={re.compile(".*"): invoke.Result(exited=0)}
        )
        return ctx

    def _check_calls(self, ctx: invoke.MockContext) -> None:
        """
        check_string() the sequence of commands issued in the context.
        """
        act = "\n".join(map(str, ctx.run.mock_calls))
        # TODO(gp): Unclear why pylint can't find this function.
        # pylint: disable=no-member
        act = hprint.remove_non_printable_chars(act)
        # pylint: enable=no-member
        self.check_string(act)

    def _check_output(self, target: str) -> None:
        """
        Dry run target checking that the sequence of commands issued is the
        expected one.
        """
        ctx = self._build_mock_context_returning_ok()
        func = eval(f"tasks.{target}")
        func(ctx)
        # Check the outcome.
        self._check_calls(ctx)


def _get_default_params() -> Dict[str, str]:
    ecr_base_path = "665840871993.dkr.ecr.us-east-1.amazonaws.com"
    default_params = {
        "ECR_BASE_PATH": ecr_base_path,
        "BASE_IMAGE": "amp_test",
        "DEV_TOOLS_IMAGE_PROD": f"{ecr_base_path}/dev_tools:prod",
    }
    return default_params


@pytest.mark.skipif(hsi.is_inside_docker(), reason="AmpTask165")
class TestExecuteTasks1(hut.TestCase):
    """
    Execute tasks that don't change state of the system (e.g., commit images).
    """

    def test_list(self) -> None:
        cmd = "invoke --list"
        hsi.system(cmd)

    def test_print_setup1(self) -> None:
        cmd = "invoke print_setup"
        hsi.system(cmd)

    def test_docker_images_ls_repo1(self) -> None:
        cmd = "invoke docker_images_ls_repo"
        hsi.system(cmd)

    def test_docker_ps(self) -> None:
        cmd = "invoke docker_ps"
        hsi.system(cmd)

    def test_docker_stats(self) -> None:
        cmd = "invoke docker_stats"
        hsi.system(cmd)

    def test_docker_login1(self) -> None:
        cmd = "invoke docker_login"
        hsi.system(cmd)

    def test_docker_cmd1(self) -> None:
        cmd = 'invoke docker_cmd --cmd="ls"'
        hsi.system(cmd)

    def test_docker_jupyter1(self) -> None:
        cmd = "invoke docker_jupyter --self-test"
        hsi.system(cmd)


@pytest.mark.skipif(hsi.is_inside_docker(), reason="AmpTask165")
class TestExecuteTasks2(hut.TestCase):
    """
    Execute tasks that change the state of the system but not using the.
    """

    def test_docker_jupyter1(self) -> None:
        cmd = "invoke docker_jupyter --self-test"
        hsi.system(cmd)

    def test_docker_pull1(self) -> None:
        cmd = "invoke docker_pull"
        hsi.system(cmd)

    # Images workflows.

    def test_docker_build_local_image(self) -> None:
        params = _get_default_params()
        base_image = params["ECR_BASE_PATH"] + "/" + params["BASE_IMAGE"]
        cmd = f"invoke docker_build_local_image --cache --base-image={base_image}"
        hsi.system(cmd)

    @pytest.mark.skip("No prod image for amp yet")
    def test_docker_build_prod_image(self) -> None:
        params = _get_default_params()
        base_image = params["ECR_BASE_PATH"] + "/" + params["BASE_IMAGE"]
        cmd = f"invoke docker_build_prod_image --cache --base-image={base_image}"
        hsi.system(cmd)

    # Run tests.
    def test_run_blank_tests1(self) -> None:
        cmd = "invoke run_blank_tests"
        hsi.system(cmd)

    @pytest.mark.skip
    @pytest.mark.slow("Around 30 secs")
    def test_collect_only1(self) -> None:
        cmd = "invoke docker_cmd --cmd='pytest --collect-only'"
        hsi.system(cmd)

    def test_collect_only2(self) -> None:
        # We need to specify the dir independently of the git root since this will
        # run inside a container.
        dir_name = '$(dirname $(find . -name "test_dbg.py" -type f))'
        cmd = f"invoke docker_cmd --cmd='pytest {dir_name} --collect-only'"
        hsi.system(cmd)

    def test_run_fast_tests(self) -> None:
        file_name = '$(find . -name "test_dbg.py" -type f)'
        cmd = f"invoke run_fast_tests --pytest-opts='{file_name}'"
        hsi.system(cmd)

    # Linter.
    def test_lint_docker_pull1(self) -> None:
        cmd = "invoke lint_docker_pull"
        hsi.system(cmd)

    def test_lint1(self) -> None:
        # Get the pointer to amp.
        file_name = '$(find . -name "dbg.py" -type f)'
        cmd = f"invoke lint --files='{file_name}' --phases='black'"
        hsi.system(cmd)

    # def test_(self):
    #     cmd = "invoke "
    #     hsi.system(cmd)


# slow_self_tests:
# make docker_build_image_with_cache.rc
# make run_blank_tests.rc
# make run_fast_tests.rc
# make docker_build_image.prod
# make run_slow_tests.rc
