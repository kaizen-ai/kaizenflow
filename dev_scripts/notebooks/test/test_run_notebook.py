"""
Import as:

import dev_scripts.notebooks.test_run_notebook as dsntruno
"""

import logging
import os
from typing import Tuple

import pytest

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


def _config_builder(fail: bool) -> cconfig.Config:
    """
    Simple config builder for the test.
    """
    config = {"fail": fail}
    config = cconfig.Config().from_dict(config)
    config_list = cconfig.ConfigList([config])
    return config_list


# TODO(Grisha): re-use `Test_Run_Notebook_TestCase`.
@pytest.mark.slow("~15 sec.")
class TestRunNotebook(hunitest.TestCase):
    def helper(
        self,
        fail: bool,
        allow_errors: bool,
        tee: bool,
    ) -> Tuple[int, str]:
        """
        Run the test notebook and get its log file.

        :param fail: the notebook breaks if `True` otherwise it does not break
        :param allow_errors: if `True`, run the notebook until the end
            regardless of any error in it.
            Note: if `True`, disables effects of `tee` in case it is enabled.
        :param tee: whether to fill log file with notebook errors or not
        :return: return code as int and log file path
        """
        # Get notebook path.
        amp_dir = hgit.get_amp_abs_path()
        input_dir = self.get_input_dir(use_only_test_class=True)
        notebook_name = "simple_notebook.ipynb"
        notebook_path = os.path.join(amp_dir, input_dir, notebook_name)
        # Build scratch dir and a path to the log file.
        dst_dir = self.get_scratch_space()
        log_file_path = os.path.join(dst_dir, "result_0", "run_notebook.0.log")
        # Get path to the tested script.
        script_path = os.path.join(
            amp_dir, "dev_scripts/notebooks", "run_notebook.py"
        )
        # Build a command to run the notebook.
        opts = "--num_threads 'serial' --publish_notebook -v DEBUG 2>&1"
        config_builder = f"dev_scripts.notebooks.test.test_run_notebook._config_builder({fail})"
        cmd_run_txt = [
            f"{script_path}",
            f"--notebook {notebook_path}",
            f"--config_builder '{config_builder}'",
            f"--dst_dir {dst_dir}",
            f"{opts}",
        ]
        if tee:
            cmd_run_txt.insert(4, "--tee")
        if allow_errors:
            cmd_run_txt.insert(4, "--allow_errors")
        cmd_run_txt = " ".join(cmd_run_txt)
        cmd_txt = []
        cmd_txt.append(cmd_run_txt)
        cmd_txt = "\n".join(cmd_txt)
        _LOG.debug("cmd=%s", cmd_txt)
        # Exucute.
        rc = hsystem.system(
            cmd_txt,
            abort_on_error=False,
            suppress_output=False,
            log_level="echo",
        )
        _LOG.debug("rc=%s", rc)
        # Check if notebook is published.
        cmd = f"find {dst_dir} -name '*.html'"
        _, file_path = hsystem.system_to_string(cmd)
        _LOG.debug("file_path=%s", file_path)
        hdbg.dassert_file_exists(file_path)
        return rc, log_file_path

    def test1(self) -> None:
        """
        Test the notebook run with a broken cell.

        - Errors in the notebook are allowed
        - The log file does not contain the errors, because they are allowed
        - Notebook is published
        """
        fail = True
        allow_errors = True
        tee = False
        # Check that script has passed without any errors.
        actual_rc, _ = self.helper(fail, allow_errors, tee)
        expected_rc = 0
        self.assertEqual(actual_rc, expected_rc)

    def test2(self) -> None:
        """
        Test the notebook run with a broken cell.

        - Errors in the notebook are not allowed
        - The log file does not contain the errors
        - Notebook is published
        """
        fail = True
        allow_errors = False
        tee = False
        # Check that script has passed with detected error.
        actual_rc, _ = self.helper(fail, allow_errors, tee)
        expected_rc = 1
        self.assertEqual(actual_rc, expected_rc)

    def test3(self) -> None:
        """
        Test the notebook run with a broken cell.

        - Errors in the notebook are not allowed
        - The log file contains the notebook errors
        - Notebook is published
        """
        fail = True
        allow_errors = False
        tee = True
        # Check that script has passed with detected error.
        actual_rc, log_file_path = self.helper(fail, allow_errors, tee)
        expected_rc = 1
        self.assertEqual(actual_rc, expected_rc)
        # Check that log file contains error message.
        log_content = hio.from_file(log_file_path)
        self.check_string(log_content)

    def test4(self) -> None:
        """
        Test the notebook run with no broken cells.

        - Errors in the notebook are not allowed
        - The log file does not contain the notebook errors
        - Notebook is published
        """
        fail = False
        allow_errors = False
        tee = False
        # Check that script has passed without any errors.
        actual_rc, _ = self.helper(fail, allow_errors, tee)
        expected_rc = 0
        self.assertEqual(actual_rc, expected_rc)
