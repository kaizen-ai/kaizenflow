import logging
import os
from typing import List, Tuple

import pytest

import core.config as cfg
import core.config_builders as cfgb
import helpers.dbg as dbg
import helpers.git as git
import helpers.system_interaction as si
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestRunNotebook1(hut.TestCase):
    def test1(self) -> None:
        """
        Run an experiment with 2 notebooks (without any failure) serially.
        """
        cmd = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--skip_on_error",
            "--num_threads 'serial'",
        ]
        # pylint: enable=line-too-long
        exp = r"""# Dir structure
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_0
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_0/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_0/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_0/run_notebook.0.log
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_0/simple_notebook.0.ipynb
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_0/success.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_1
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_1/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_1/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_1/run_notebook.1.log
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_1/simple_notebook.1.ipynb
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test1/tmp.scratch/result_1/success.txt"""
        # pylint: disable=line-too-long
        rc = self._run_notebook_helper(cmd, exp)
        self.assertEqual(rc, 0)

    @pytest.mark.slow
    def test2(self) -> None:
        """
        Run an experiment with 2 notebooks (without any failure) with 2
        threads.
        """
        cmd = [
            "--config_builder 'dev_scripts.test.test_run_notebook.build_configs1()'",
            "--num_threads 2",
        ]
        # pylint: enable=line-too-long
        exp = r"""# Dir structure
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_0
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_0/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_0/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_0/run_notebook.0.log
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_0/simple_notebook.0.ipynb
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_0/success.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_1
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_1/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_1/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_1/run_notebook.1.log
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_1/simple_notebook.1.ipynb
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test2/tmp.scratch/result_1/success.txt"""
        # pylint: disable=line-too-long
        rc = self._run_notebook_helper(cmd, exp)
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
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_0
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_0/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_0/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_0/run_notebook.0.log
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_0/simple_notebook.0.ipynb
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_0/success.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_1
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_1/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_1/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_1/run_notebook.1.log
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_1/simple_notebook.1.ipynb
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_1/success.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_2
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_2/config.pkl
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_2/config.txt
$GIT_ROOT/dev_scripts/test/TestRunNotebook1.test3/tmp.scratch/result_2/run_notebook.2.log"""
        # pylint: disable=line-too-long
        rc = self._run_notebook_helper(cmd, exp)
        self.assertNotEqual(rc, 0)

    @staticmethod
    def _get_files() -> Tuple[str, str]:
        amp_path = git.get_amp_abs_path()
        #
        exec_file = os.path.join(
            amp_path, "dev_scripts/notebooks/run_notebook.py"
        )
        dbg.dassert_file_exists(exec_file)
        # This notebook fails/succeeds depending on the return code stored inside
        # each config.
        notebook_file = os.path.join(
            amp_path, "dev_scripts/notebooks/test/simple_notebook.ipynb"
        )
        dbg.dassert_file_exists(notebook_file)
        return exec_file, notebook_file

    def _run_notebook_helper(self, cmd: List[str], exp: str) -> int:
        # Build command line.
        dst_dir = self.get_scratch_space()
        exec_file, notebook_file = self._get_files()
        cmd_tmp = [
            f"{exec_file}",
            f"--dst_dir {dst_dir}",
            f"--notebook {notebook_file}",
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


def _build_config(values: List[bool]) -> List[cfg.Config]:
    config_template = cfg.Config()
    config_template["fail"] = None
    configs = cfgb.build_multiple_configs(config_template, {("fail",): values})
    # Duplicate configs are not allowed, so we need to add identifiers to make
    # each config unique.
    for i, config in enumerate(configs):
        config["id"] = i
    return configs


def build_configs1() -> List[cfg.Config]:
    """
    Build 2 configs that won't make the notebook to fail.
    """
    values = (False, False)
    configs = _build_config(values)
    return configs


def build_configs2() -> List[cfg.Config]:
    """
    Build 3 configs with one failing.
    """
    values = (False, False, True)
    configs = _build_config(values)
    return configs
