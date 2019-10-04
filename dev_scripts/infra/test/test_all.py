import logging

import pytest

import helpers.git as git
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################


# TODO(gp): Find a way to test this. Maybe we can run it in Jenkins.
# @pytest.mark.skipif('si.get_user_name() != "jenkins"')
@pytest.mark.skip
class Test_ssh_tunnel(ut.TestCase):
    def test1(self):
        exec_name = git.find_file_in_git_tree("ssh_tunnels.py")
        _LOG.debug("exec_name=%s", exec_name)
        # We execute all the phases in multiple rounds to make sure things
        # are fine.
        # TODO(gp): Add some automatic checking.
        actions = [
            #
            "start",
            "check",
            "stop",
            "check",
            #
            "kill",
            "check",
        ]
        for action in actions:
            _LOG.debug("action=%s", action)
            cmd = "%s %s -v INFO" % (exec_name, action)
            si.system(cmd, suppress_output="ON_DEBUG_LEVEL")
