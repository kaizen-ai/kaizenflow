import logging
import os
from typing import List

import pytest

import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as si
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestRunExperiment1(hut.TestCase):
    """
    These tests are equivalent to `TestRunNotebook1` but using the
    `run_experiment.py` flow instead of `run_notebook.py`.
    """

    def test1(self) -> None:
        """
        Run two experiments (without any failure) serially.
        """
        cmd = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--skip_on_error",
            "--num_threads 'serial'",
        ]
        # pylint: enable=line-too-long
        exp = r"""# Dir structure
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_0
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_0/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_0/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_0/run_experiment.0.log
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_0/success.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_1
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_1/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_1/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_1/run_experiment.1.log
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test1/tmp.scratch/result_1/success.txt"""
        # pylint: disable=line-too-long
        rc = self._run_experiment_helper(cmd, exp)
        self.assertEqual(rc, 0)

    @pytest.mark.slow
    def test2(self) -> None:
        """
        Run two experiments (without any failure) with 2 threads.
        """
        cmd = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 2",
        ]
        # pylint: enable=line-too-long
        exp = r"""# Dir structure
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_0
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_0/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_0/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_0/run_experiment.0.log
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_0/success.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_1
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_1/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_1/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_1/run_experiment.1.log
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test2/tmp.scratch/result_1/success.txt"""
        # pylint: disable=line-too-long
        rc = self._run_experiment_helper(cmd, exp)
        self.assertEqual(rc, 0)

    @pytest.mark.slow
    def test3(self) -> None:
        """
        Run an experiment with 3 notebooks (with one failing) using 3 threads.
        """
        cmd = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads 3",
        ]
        _LOG.warning("This command is supposed to fail")
        # pylint: enable=line-too-long
        exp = r"""# Dir structure
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_0
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_0/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_0/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_0/run_experiment.0.log
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_0/success.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_1
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_1/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_1/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_1/run_experiment.1.log
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_1/success.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_2
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_2/config.pkl
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_2/config.txt
$GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch/result_2/run_experiment.2.log"""
        # pylint: disable=line-too-long
        rc = self._run_experiment_helper(cmd, exp)
        self.assertNotEqual(rc, 0)

    def _run_experiment_helper(self, cmd: List[str], exp: str) -> int:
        amp_path = git.get_amp_abs_path()
        # Get the executable.
        exec_file = os.path.join(
            amp_path, "core/dataflow_model/run_experiment.py"
        )
        dbg.dassert_file_exists(exec_file)
        # Build command line.
        dst_dir = self.get_scratch_space()
        cmd_tmp = [
            f"{exec_file}",
            "--experiment_builder core.dataflow_model.test.simple_experiment.run_experiment",
            f"--dst_dir {dst_dir}",
        ]
        cmd_tmp.extend(cmd)
        cmd = " ".join(cmd_tmp)
        # Run command.
        rc = si.system(cmd, abort_on_error=False)
        # Compute and compare the dir signature.
        act = hut.get_dir_signature(
            dst_dir, include_file_content=False, num_lines=None
        )
        act = hut.purify_txt_from_client(act)
        self.assert_equal(act, exp, fuzzy_match=True)
        return rc
