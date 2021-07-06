import logging
import os
from typing import Any, List

import pytest

import dev_scripts.test.test_run_notebook as trnot
import helpers.dbg as dbg
import helpers.git as git
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# TODO(gp): We could factor out more common code between here and the corresponding
#  unit tests in TestRuNotebook*. The difference is only in the command lines.
class TestRunExperiment1(hut.TestCase):
    """
    Run experiments without failures.

    These tests are equivalent to `TestRunNotebook1` but using the
    `run_experiment.py` flow instead of `run_notebook.py`.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_experiment.0.log
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_experiment.1.log
        $SCRATCH_SPACE/result_1/success.txt"""

    def test_serial1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - serially
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 'serial'",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - with 2 threads
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 2",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################


class TestRunExperiment2(hut.TestCase):
    """
    Run experiments that fail.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_experiment.0.log
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_experiment.1.log
        $SCRATCH_SPACE/result_1/success.txt
        $SCRATCH_SPACE/result_2
        $SCRATCH_SPACE/result_2/config.pkl
        $SCRATCH_SPACE/result_2/config.txt
        $SCRATCH_SPACE/result_2/run_experiment.2.log"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - serially
        - aborting on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads serial",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_serial2(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - serially
        - skipping on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--skip_on_error",
            "--num_threads serial",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - with 2 threads
        - aborting on error

        Same as `test_serial1` but using 2 threads.
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads 2",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel2(self) -> None:
        """
        Execute:
        - 3 experiments with one failing
        - with 2 threads
        - skipping on error

        Same as `test_serial1` but using 2 threads.
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--skip_on_error",
            "--num_threads 2",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################


def _run_experiment_helper(
    self: Any, cmd_opts: List[str], exp_pass: bool, exp: str
) -> None:
    amp_path = git.get_amp_abs_path()
    # Get the executable.
    exec_file = os.path.join(amp_path, "core/dataflow_model/run_experiment.py")
    dbg.dassert_file_exists(exec_file)
    # Build command line.
    dst_dir = self.get_scratch_space()
    cmd = [
        f"{exec_file}",
        "--experiment_builder core.dataflow_model.test.simple_experiment.run_experiment",
        f"--dst_dir {dst_dir}",
    ]
    trnot.run_cmd_line(self, cmd, cmd_opts, dst_dir, exp, exp_pass)
