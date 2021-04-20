import logging
import re

import pytest

import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.system_interaction as hsi
import helpers.unit_test as hut

dbg.init_logger()
_LOG = logging.getLogger(__name__)

# TODO(*): remove after images update
try:
    import invoke

    import tasks
except ModuleNotFoundError as e:
    _LOG.error(e)

    class Dummy:
        def __init__(self):
            self.MockContext = None

    invoke = Dummy()


@pytest.mark.skip(reason="Update image required. PTask8226")
class TestTasks(hut.TestCase):
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

    def test_docker_ps(self) -> None:
        target = "docker_kill_last"
        self._dry_run(target)

    def test_docker_stats(self) -> None:
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
        act = hprint.remove_non_printable_chars(act)
        self.check_string(act)

    def _build_mock_context_returning_ok(self):
        ctx = invoke.MockContext(
            repeat=True, run={re.compile(".*"): invoke.Result(exited=0)}
        )
        return ctx

    def _check_calls(self, ctx: invoke.MockContext) -> None:
        act = "\n".join(map(str, ctx.run.mock_calls))
        act = hprint.remove_non_printable_chars(act)
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
