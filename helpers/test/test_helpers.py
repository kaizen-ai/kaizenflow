import logging

import helpers.env as env
import helpers.git as git
import helpers.unit_test as ut
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

    def _helper(self, func_call):
        # pylint: disable=W0123
        # [W0123(eval-used), Test_git1._helper] Use of eval
        act = eval(func_call)
        _LOG.debug("%s=%s", func_call, act)

    def test_get_git_name1(self):
        func_call = "git.get_repo_symbolic_name(super_module=True)"
        self._helper(func_call)

    def test_is_inside_submodule1(self):
        func_call = "git.is_inside_submodule()"
        self._helper(func_call)

    def test_get_client_root1(self):
        func_call = "git.get_client_root(super_module=True)"
        self._helper(func_call)

    def test_get_client_root2(self):
        func_call = "git.get_client_root(super_module=False)"
        self._helper(func_call)

    def test_get_path_from_git_root1(self):
        file_name = "helpers/test/test_helpers.py"
        act = git.get_path_from_git_root(file_name, super_module=False)
        _LOG.debug("get_path_from_git_root()=%s", act)

    def test_get_repo_symbolic_name1(self):
        func_call = "git.get_repo_symbolic_name(super_module=True)"
        self._helper(func_call)

    def test_get_repo_symbolic_name2(self):
        func_call = "git.get_repo_symbolic_name(super_module=False)"
        self._helper(func_call)

    def test_get_modified_files1(self):
        func_call = "git.get_modified_files()"
        self._helper(func_call)

    def test_get_previous_committed_files1(self):
        func_call = "git.get_previous_committed_files()"
        self._helper(func_call)

    def test_git_log1(self):
        _LOG.debug("git_log=\n%s", git.git_log())

    def test_git_log2(self):
        _LOG.debug("git_log=\n%s", git.git_log(my_commits=True))


# #############################################################################


class Test_env1(ut.TestCase):
    def test_get_system_signature1(self):
        txt = env.get_system_signature()
        _LOG.debug(txt)


# #############################################################################


class Test_user_credentials1(ut.TestCase):
    def test_get_credentials1(self):
        data = usc.get_credentials()
        _LOG.debug("data=%s", data)


# #############################################################################


class Test_numba_1(ut.TestCase):
    def test1(self):
        pass
