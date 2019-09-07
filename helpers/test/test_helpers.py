import logging

import helpers.git as git
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)

# #############################################################################


class Test_git1(ut.TestCase):
    def test_is_inside_submodule1(self):
        ret = git.is_inside_submodule()
        _LOG.debug("git.is_inside_submodule()=%s", ret)
        self.assertEqual(ret, True)

    def test_get_client_root1(self):
        val = git.get_client_root(super_module=True)
        _LOG.debug("git.get_client_root(super_module=True)=%s", val)
        # We can check since we don't know in which dir we are running:
        # visually inspect the outcome.

    def test_get_client_root2(self):
        val = git.get_client_root(super_module=False)
        _LOG.debug("git.get_client_root(super_module=False)=%s", val)
        # We can check since we don't know in which dir we are running:
        # visually inspect the outcome.

    def test_get_path_from_git_root1(self):
        file_name = "helpers/test/test_helpers.py"
        act = git.get_path_from_git_root(file_name, super_module=False)
        _LOG.debug("get_path_from_git_root()=%s", act)
        # We can check since we don't know in which dir we are running:
        # visually inspect the outcome.
