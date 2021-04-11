import os

import pytest

from invoke import MockContext, Result

import helpers.unit_test as hut
import helpers.system_interaction as hsi
import tasks
from tasks import get_platform, replace


class TestTasks(hut.TestCase):

    def _helper(self, target:str) -> None:
        cmd = "invoke --dry " + target
        _, txt = hsi.system_to_string(cmd)
        self.check_string(txt)

    def test_print_setup(self) -> None:
        target = "print_setup"
        self._helper(target)

    def test_git_pull(self) -> None:
        target = "git_pull"
        self._helper(target)

    def test_git_pull_master(self) -> None:
        target = "git_pull_master"
        self._helper(target)

    def test_git_clean(self) -> None:
        target = "git_clean"
        self._helper(target)

    def test_docker_login(self) -> None:
        #stdout = "aws-cli/1.19.49 Python/3.7.6 Darwin/19.6.0 botocore/1.20.49\n"
        #ctx = MockContext(run=Result(stdout))
        #tasks.docker_login(ctx).run()
        pass

    def test_get_platform(self):
        c = MockContext(run=Result("Darwin\n"))
        print(get_platform(c))
        assert 0
        assert "Apple" in get_platform(c)
        c = MockContext(run=Result("Linux\n"))
        assert "desktop" in get_platform(c)

    def test_regular_sed(self):
        expected_sed = "sed -e s/foo/bar/g file.txt"
        c = MockContext(run={
            "which gsed": Result(exited=1),
            #expected_sed: Result(),
        })
        replace(c, 'file.txt', 'foo', 'bar')
        c.run.assert_called_with(expected_sed)