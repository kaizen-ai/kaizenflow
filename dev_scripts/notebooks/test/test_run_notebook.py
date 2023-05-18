"""
Import as:

import dev_scripts.notebooks.test_run_notebook as dsntruno
"""

import logging
import os

import pytest

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


def config_builder(fail: bool) -> cconfig.Config:
    """
    Simple config builder for the test.
    """
    # Build a simple config for test.
    config = {"fail": fail}
    config = cconfig.Config().from_dict(config)
    config_list = cconfig.ConfigList([config])
    return config_list


# TODO(Grisha): re-use `Test_Run_Notebook_TestCase`.
@pytest.mark.slow("~15 sec.")
class TestRunNotebook(hunitest.TestCase):
    def helper(self, fail: bool, allow_errors: bool) -> int:
        """
        Run the test notebook.

        :param fail: the notebook breaks if True otherwise it does not break
        :param allow_errors: if `True`, run the notebook until the end
            regardless of any error in it
        :return: return code as int
        """
        # Get notebook path.
        amp_dir = hgit.get_amp_abs_path()
        input_dir = self.get_input_dir(use_only_test_class=True)
        notebook_name = "simple_notebook.ipynb"
        notebook_path = os.path.join(amp_dir, input_dir, notebook_name)
        #
        dst_dir = self.get_scratch_space()
        script_path = os.path.join(
            amp_dir, "dev_scripts/notebooks", "run_notebook.py"
        )
        # Build a command to run the notebook.
        opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1"
        config_builder = (
            f"dev_scripts.notebooks.test.test_run_notebook.config_builder({fail})"
        )
        cmd_run_txt = [
            f"{script_path}",
            f"--notebook {notebook_path}",
            f"--config_builder '{config_builder}'",
            f"--dst_dir {dst_dir}",
            f"{opts}",
        ]
        if allow_errors:
            cmd_run_txt.insert(4, "--allow_errors")
        cmd_run_txt = " ".join(cmd_run_txt)
        cmd_txt = []
        cmd_txt.append(cmd_run_txt)
        cmd_txt = "\n".join(cmd_txt)
        _LOG.debug("cmd=%s", cmd_txt)
        # Exucute.
        rc = hsystem.system(cmd_txt, abort_on_error=False, log_level="echo")
        _LOG.debug("rc=%s", rc)
        # Check if notebook is published.
        cmd = f"find {dst_dir} -name '*.html'"
        _, file_path = hsystem.system_to_string(cmd)
        _LOG.debug("file_path=%s", file_path)
        hdbg.dassert_file_exists(file_path)
        return rc

    def test1(self) -> None:
        """
        The broken notebook is executed successfully and is published regardless
        of errors.
        """
        fail = True
        allow_errors = True
        actual = self.helper(fail, allow_errors)
        expected = 0
        self.assertEqual(actual, expected)

    def test2(self) -> None:
        """
        The broken notebook fails but it is published regardless of errors.
        """
        fail = True
        allow_errors = False
        actual = self.helper(fail, allow_errors)
        expected = 0
        self.assertNotEqual(actual, expected)

    def test3(self) -> None:
        """
        The notebook that is not broken is executed successfully and is
        published.
        """
        fail = False
        allow_errors = False
        actual = self.helper(fail, allow_errors)
        expected = 0
        self.assertEqual(actual, expected)