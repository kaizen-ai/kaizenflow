import logging
import os
from typing import Any, List

import pytest

import dev_scripts.test.test_run_notebook as trnot
import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.git as git
import helpers.s3 as hs3
import helpers.system_interaction as hsyste
import helpers.unit_test as hut
import helpers.parser as hparse

_LOG = logging.getLogger(__name__)


# TODO(gp): We could factor out more common code between here and the corresponding
#  unit tests in TestRuNotebook*. The difference is only in the command lines.
class TestRunExperimentSuccess1(hut.TestCase):
    """
    Run experiments that succeed.

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


class TestRunExperimentFail2(hut.TestCase):
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
            "--skip_archive_on_S3",
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
            "--skip_archive_on_S3",
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
            "--skip_archive_on_S3",
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
            "--skip_archive_on_S3",
        ]
        #
        exp_pass = True
        _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################


class TestRunExperimentArchiveOnS3(hut.TestCase):
    """
    Run experiments that succeed and archive the results on S3.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/output_metadata.json
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_experiment.0.log
        $SCRATCH_SPACE/result_0/success.txt"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - two experiments (without any failure)
        - serially
        """
        scratch_dir = self.get_scratch_space()
        aws_profile = "am"
        # Actions.
        create_s3_archive = True
        check_s3_archive = True
        clean_up_s3_archive = False
        # Create archive on S3.
        if create_s3_archive:
            output_metadata_file = f"{scratch_dir}/output_metadata.json"
            cmd_opts = [
                "--config_builder 'dev_scripts.test.test_run_notebook.build_configs3()'",
                "--num_threads 'serial'",
                f"--aws_profile '{aws_profile}'",
                "--s3_path s3://alphamatic-data/tmp",
                f"--json_output_metadata {output_metadata_file}"
            ]
            #
            exp_pass = True
            dst_dir = _run_experiment_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)
            _ = dst_dir
            # Read the metadata back.
            output_metadata = hparse.read_output_metadata(output_metadata_file)
            s3_path = output_metadata["s3_path"]
            _LOG.debug("s3_path=%s", s3_path)
        if check_s3_archive:
            #s3_path = "s3://alphamatic-data/tmp/tmp.20210802-121908.scratch.tgz"
            tgz_dst_dir = hs3.retrieve_archived_data_from_s3(s3_path, scratch_dir, aws_profile)
            _LOG.info("Retrieved to %s", tgz_dst_dir)
            # Check the content.
            cmd = f"ls -1 {tgz_dst_dir}"
            files = hsyste.system_to_files(cmd)
            _LOG.debug("Files are:\n%s", files)
            # TODO(gp): We should check that the output looks like:
            # EXPECTED_OUTCOME = r"""# Dir structure
            #     $SCRATCH_SPACE/result_0
            #     $SCRATCH_SPACE/result_0/config.pkl
            #     $SCRATCH_SPACE/result_0/config.txt
            #     $SCRATCH_SPACE/result_0/run_experiment.0.log
            #     $SCRATCH_SPACE/result_0/success.txt"""
        if clean_up_s3_archive:
            # Clean up S3.
            s3fs_ = hs3.get_s3fs(aws_profile)
            hs3.dassert_s3_exists(s3_path, s3fs_)
            s3fs_.rm(s3_path)
            hs3.dassert_s3_not_exist(s3_path, s3fs_)


# #############################################################################


def _run_experiment_helper(
    self: Any, cmd_opts: List[str], exp_pass: bool, exp: str
) -> str:
    """
    Build, run, and check a `run_experiment` command line.

    :return: destination dir with the results
    """
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
    dst_dir = trnot.run_cmd_line(self, cmd, cmd_opts, dst_dir, exp, exp_pass)
    return dst_dir
