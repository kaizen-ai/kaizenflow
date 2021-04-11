import os
import re
import unittest

import pytest

import invoke
from invoke import MockContext, Result

import helpers.unit_test as hut
import helpers.printing as hprint
import helpers.system_interaction as hsi
import tasks
from tasks import get_platform, replace


class TestTasks(hut.TestCase):

    def _dry_run(self, target:str) -> None:
        """
        Invoke with dry run the given target.
        """
        cmd = "invoke --dry " + target
        _, txt = hsi.system_to_string(cmd)
        self.check_string(txt)

    def _build_mock_context_returning_ok(self):
        ctx = MockContext(
            repeat=True,
            run={
            re.compile(".*"): Result(exited=0)
        })
        return ctx

    def _check_calls(self, ctx: invoke.MockContext) -> None:
        act = "\n".join(map(str, ctx.run.mock_calls))
        act = hprint.remove_non_printable_chars(act)
        self.check_string(act)

    def _check_output(self, target: str) -> None:
        """
        Dry run target checking that the sequence of commands issued is the expected
        one.
        """
        ctx = self._build_mock_context_returning_ok()
        func = eval(f"tasks.{target}")
        func(ctx)
        # Check the outcome.
        self._check_calls(ctx)

    def test_print_setup1(self) -> None:
        target = "print_setup"
        self._dry_run(target)
        self._check_output(target)

    def test_git_pull1(self) -> None:
        target = "git_pull"
        self._dry_run(target)
        self._check_output(target)

    def test_git_pull_master1(self) -> None:
        target = "git_pull_master"
        self._dry_run(target)
        self._check_output(target)

    def test_git_clean1(self) -> None:
        target = "git_clean"
        self._dry_run(target)
        self._check_output(target)

    def test_docker_login(self) -> None:
        stdout = "aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49\n"
        ctx = MockContext(run={
            "aws --version": Result(stdout),
            re.compile("^docker login"): Result(exited=0)
        })
        tasks.docker_login(ctx)
        # Check the outcome.
        self._check_calls(ctx)

    # def test_get_platform(self):
    #     c = MockContext(run=Result("Darwin\n"))
    #     print(get_platform(c))
    #     assert 0
    #     assert "Apple" in get_platform(c)
    #     c = MockContext(run=Result("Linux\n"))
    #     assert "desktop" in get_platform(c)
    #
    # def test_regular_sed(self):
    #     expected_sed = "sed -e s/foo/bar/g file.txt"
    #     c = MockContext(run={
    #         "which gsed": Result(exited=1),
    #         #expected_sed: Result(),
    #     })
    #     replace(c, 'file.txt', 'foo', 'bar')
    #     c.run.assert_called_with(expected_sed)