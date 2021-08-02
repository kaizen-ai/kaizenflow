import logging
import os
from typing import Any, List, Tuple, cast

import pytest

import core.config as cconfig
import helpers.dbg as dbg
import helpers.git as git
import helpers.printing as hprint
import helpers.system_interaction as si
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestRunNotebook1(hut.TestCase):
    """
    Run notebooks without failures.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_notebook.0.log
        $SCRATCH_SPACE/result_0/simple_notebook.0.ipynb
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_notebook.1.log
        $SCRATCH_SPACE/result_1/simple_notebook.1.ipynb
        $SCRATCH_SPACE/result_1/success.txt"""

    def test_serial1(self) -> None:
        """
        Execute:
        - two notebooks (without any failure)
        - serially
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 'serial'",
        ]
        #
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

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
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


# #############################################################################


class TestRunNotebook2(hut.TestCase):
    """
    Run experiments that fail.
    """

    EXPECTED_OUTCOME = r"""# Dir structure
        $SCRATCH_SPACE
        $SCRATCH_SPACE/result_0
        $SCRATCH_SPACE/result_0/config.pkl
        $SCRATCH_SPACE/result_0/config.txt
        $SCRATCH_SPACE/result_0/run_notebook.0.log
        $SCRATCH_SPACE/result_0/simple_notebook.0.ipynb
        $SCRATCH_SPACE/result_0/success.txt
        $SCRATCH_SPACE/result_1
        $SCRATCH_SPACE/result_1/config.pkl
        $SCRATCH_SPACE/result_1/config.txt
        $SCRATCH_SPACE/result_1/run_notebook.1.log
        $SCRATCH_SPACE/result_1/simple_notebook.1.ipynb
        $SCRATCH_SPACE/result_1/success.txt
        $SCRATCH_SPACE/result_2
        $SCRATCH_SPACE/result_2/config.pkl
        $SCRATCH_SPACE/result_2/config.txt
        $SCRATCH_SPACE/result_2/run_notebook.2.log"""

    @pytest.mark.slow
    def test_serial1(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - serially
        - aborting on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads 3",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_serial2(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - serially
        - skipping on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--skip_on_error",
            "--num_threads 3",
        ]
        #
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel1(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - with 2 threads
        - aborting on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--num_threads 2",
        ]
        #
        exp_pass = False
        _LOG.warning("This command is supposed to fail")
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)

    @pytest.mark.slow
    def test_parallel2(self) -> None:
        """
        Execute:
        - an experiment with 3 notebooks (with one failing)
        - with 2 threads
        - skipping on error
        """
        cmd_opts = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs2()'",
            "--skip_on_error",
            "--num_threads 2",
        ]
        #
        exp_pass = True
        _run_notebook_helper(self, cmd_opts, exp_pass, self.EXPECTED_OUTCOME)


def _get_files() -> Tuple[str, str]:
    amp_path = git.get_amp_abs_path()
    #
    exec_file = os.path.join(amp_path, "dev_scripts/notebooks/run_notebook.py")
    dbg.dassert_file_exists(exec_file)
    # This notebook fails/succeeds depending on the return code stored inside
    # each config.
    notebook_file = os.path.join(
        amp_path, "dev_scripts/notebooks/test/simple_notebook.ipynb"
    )
    dbg.dassert_file_exists(notebook_file)
    return exec_file, notebook_file


def _run_notebook_helper(
    self: Any, cmd_opts: List[str], exp_pass: bool, exp: str
) -> None:
    # Build command line.
    dst_dir = self.get_scratch_space()
    exec_file, notebook_file = _get_files()
    cmd = [
        f"{exec_file}",
        f"--dst_dir {dst_dir}",
        f"--notebook {notebook_file}",
    ]
    run_cmd_line(self, cmd, cmd_opts, dst_dir, exp, exp_pass)


# #############################################################################


# These are fake config builders used for testing.


def _build_config(values: List[bool]) -> List[cconfig.Config]:
    config_template = cconfig.Config()
    # TODO(gp): -> fail_param
    config_template["fail"] = None
    configs = cconfig.build_multiple_configs(config_template, {("fail",): values})
    # Duplicated configs are not allowed, so we need to add identifiers to make
    # each config unique.
    for i, config in enumerate(configs):
        config["id"] = str(i)
    configs = cast(List[cconfig.Config], configs)
    return configs


def build_configs1() -> List[cconfig.Config]:
    """
    Build 2 configs that won't make the notebook to fail.
    """
    values = [False, False]
    configs = _build_config(values)
    return configs


def build_configs2() -> List[cconfig.Config]:
    """
    Build 3 configs with one failing.
    """
    values = [False, False, True]
    configs = _build_config(values)
    return configs


def build_configs3() -> List[cconfig.Config]:
    """
    Build 1 config that won't make the notebook to fail.
    """
    values = [False]
    configs = _build_config(values)
    return configs


# #############################################################################


def run_cmd_line(
    self: Any,
    cmd: List[str],
    cmd_opts: List[str],
    dst_dir: str,
    exp: str,
    exp_pass: bool,
) -> str:
    """
    Run run_experiment / run_notebook command line and check return code and output.

    :return: destination dir with the results
    """
    # Assemble the command line.
    cmd.extend(cmd_opts)
    cmd = " ".join(cmd)
    # Run command.
    abort_on_error = exp_pass
    _LOG.debug("exp_pass=%s abort_on_error=%s", exp_pass, abort_on_error)
    rc = si.system(cmd, abort_on_error=abort_on_error, suppress_output=False)
    if exp_pass:
        self.assertEqual(rc, 0)
    else:
        self.assertNotEqual(rc, 0)
    # Compute and compare the dir signature.
    act = hut.get_dir_signature(
        dst_dir, include_file_content=False, num_lines=None
    )
    # Remove references like:
    # $GIT_ROOT/core/dataflow_model/test/TestRunExperiment1.test3/tmp.scratch
    act = act.replace(self.get_scratch_space(), "$SCRATCH_SPACE")
    act = hut.purify_txt_from_client(act)
    # Remove lines like:
    # $GIT_ROOT/core/dataflow_model/.../log.20210705_100612.txt
    act = hut.filter_text(r"^.*/log\.\S+\.txt$", act)
    #
    exp = hprint.dedent(exp)
    self.assert_equal(act, exp, fuzzy_match=True)
    return dst_dir
