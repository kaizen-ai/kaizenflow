import os
from typing import Any

import pytest

import core.config as cconfig
import dev_scripts.notebooks.run_notebook_test_case as dsnrnteca
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hsystem as hsystem


def build_config() -> cconfig.ConfigList:
    """
    Simple config builder for the test.
    """
    # We want to execute the notebook as it is, but config builder needs
    # a config from the caller, which we ignore for now.
    config = {}
    config = cconfig.Config()
    config_list = cconfig.ConfigList([config])
    return config_list


def _test_save_data(self: Any) -> None:
    """
    Save test data.
    """
    # Specify params to run the script.
    amp_dir = hgit.get_amp_abs_path()
    script_path = "dataflow/backtest/run_config_list.py"
    script_path = os.path.join(amp_dir, script_path)
    backtest_config = "mock1_v1-top2.5T.2000-01-01_2000-01-02"
    config_builder = f"'dataflow_amp.system.mock1.mock1_tile_config_builders.build_Mock1_tile_config_list(\"{backtest_config}\")'"
    # Save results to a scratch dir to copy only `tiled_results` dir
    # further.
    scratch_dir = self.get_scratch_space()
    cmd = [
        script_path,
        "--experiment_builder 'dataflow.backtest.master_backtest.run_in_sample_tiled_backtest'",
        f"--config_builder {config_builder}",
        f"--dst_dir {scratch_dir}",
        "--num_threads 'serial'",
    ]
    cmd = " ".join(cmd)
    # Run tiled flow to get data.
    hsystem.system(cmd, suppress_output=False)
    # Copy `tiled_results` dir to the input dir.
    tiled_results_dir = os.path.join(scratch_dir, "tiled_results")
    use_only_test_class = True
    dst_dir = self.get_input_dir(use_only_test_class)
    hio.create_dir(dst_dir, incremental=True)
    cmd = f"cp -r {tiled_results_dir} {dst_dir}"
    hsystem.system(cmd, suppress_output=False)


def _test_run_notebook(self: Any, notebook_name: str) -> None:
    """
    Run notebook end-to-end without errors.
    """
    amp_dir = hgit.get_amp_abs_path()
    notebook_path = os.path.join(
        amp_dir,
        "dataflow",
        "model",
        "notebooks",
        f"{notebook_name}.ipynb",
    )
    config_builder = "dataflow.model.test.test_run_notebooks.build_config()"
    self._test_run_notebook(notebook_path, config_builder)


class Test_run_master_feature_analyzer(dsnrnteca.Test_Run_Notebook_TestCase):
    @pytest.mark.superslow("~45 sec.")
    def test_run_notebook(self) -> None:
        """
        Test that notebook runs end-to-end without errors.
        """
        notebook_name = "Master_feature_analyzer"
        _test_run_notebook(self, notebook_name)

    @pytest.mark.skip("Run manually.")
    @pytest.mark.slow("~15 sec.")
    def test_save_data(self) -> None:
        """
        Save test data.
        """
        _test_save_data(self)


class Test_run_master_research_backtest_analyzer(
    dsnrnteca.Test_Run_Notebook_TestCase
):
    @pytest.mark.superslow("~45 sec.")
    def test_run_notebook(self) -> None:
        """
        Test that notebook runs end-to-end without errors.
        """
        notebook_name = "Master_research_backtest_analyzer"
        _test_run_notebook(self, notebook_name)

    @pytest.mark.skip("Run manually.")
    @pytest.mark.slow("~17 sec.")
    def test_save_data(self) -> None:
        """
        Save test data.
        """
        _test_save_data(self)
