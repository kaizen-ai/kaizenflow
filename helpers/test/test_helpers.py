import logging

import helpers.git as git
import helpers.unit_test as ut
import helpers.system_interaction as si
import helpers.user_credentials as usc

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_git1(ut.TestCase):
    """
    Unfortunately we can't check the outcome of some of these functions since we
    don't know in which dir we are running. Thus we test that the function
    completes and visually inspect the outcome, if needed.
    TODO(gp): If we have Jenkins on AM side we could test for the outcome at
    least in that set-up.
    """
    def _helper(self, func_name):
        func = eval("git.%s" % func_name)
        act = func()
        _LOG.debug("%s()=%s", func_name, act)

    def test_is_inside_submodule1(self):
        func_name = "is_inside_submodule"
        self._helper(func_name)

    def test_get_client_root1(self):
        val = git.get_client_root(super_module=True)
        _LOG.debug("git.get_client_root(super_module=True)=%s", val)

    def test_get_client_root2(self):
        val = git.get_client_root(super_module=False)
        _LOG.debug("git.get_client_root(super_module=False)=%s", val)

    def test_get_path_from_git_root1(self):
        file_name = "helpers/test/test_helpers.py"
        act = git.get_path_from_git_root(file_name, super_module=False)
        _LOG.debug("get_path_from_git_root()=%s", act)

    def test_get_modified_files1(self):
        func_name = "get_modified_files"
        self._helper(func_name)

    def test_get_previous_committed_files1(self):
        func_name = "get_previous_committed_files"
        self._helper(func_name)

    def test_get_git_name1(self):
        func_name = "get_repo_symbolic_name"
        self._helper(func_name)

    def test_get_repo_symbolic_name1(self):
        func_name = "get_repo_symbolic_name"
        self._helper(func_name)

# #############################################################################

class Test_user_credentials(ut.TestCase):
    def test_get_credentials1(self):
        user_name = si.get_user_name()
        server_name = si.get_server_name()
        data = usc.get_credentials(user_name, server_name)
        _LOG.debug("data=%s", data)


# #############################################################################


class Test_numba_1(ut.TestCase):
    def test1(self):
        pass
