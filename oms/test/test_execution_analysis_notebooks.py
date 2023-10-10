"""
Tests OMS execution notebooks.

  - Master_execution_analysis.ipynb


Import as:

import oms.test.test_execution_analysis_notebooks as ottean
"""

import logging
import os

import pytest

import core.config as cconfig
import dev_scripts.notebooks as dsnrn
import helpers.hgit as hgit
import helpers.hserver as hserver
import helpers.hsystem as hsystem
import oms.execution_analysis_configs as oexancon

_LOG = logging.getLogger(__name__)


def build_test_master_execution_analysis_config(
    system_log_dir: str,
) -> cconfig.ConfigList:
    """
    Default config builder for testing the Master_execution_analysis notebook.
    """
    id_col = "asset_id"
    universe_version = "v7.1"
    vendor = "CCXT"
    mode = "trade"
    test_asset_id = 1467591036
    bar_duration = "5T"
    expected_num_child_orders = [0, 5]
    use_historical = True
    config_list = oexancon.build_execution_analysis_configs(
        system_log_dir,
        id_col,
        universe_version,
        vendor,
        mode,
        test_asset_id,
        bar_duration,
        expected_num_child_orders,
        use_historical,
    )
    return config_list


@pytest.mark.skipif(
    hserver.is_inside_ci(),
    reason="The prod database is not available via GH actions",
)
class Test_run_master_execution_analysis_notebook(
    dsnrn.Test_Run_Notebook_TestCase
):
    @pytest.mark.superslow("~70 seconds.")
    def test1(self) -> None:
        """
        Run Master_execution_analysis notebook end-to-end.
        """
        amp_dir = hgit.get_amp_abs_path()
        # Copy test data from S3 to scratch space.
        s3_input_dir = self.get_s3_input_dir("ck")
        scratch_space_path = self.get_scratch_space()
        self._copy_data_from_s3_to_scratch(s3_input_dir, scratch_space_path)
        # Run the notebook.
        notebook_path = "oms/notebooks/Master_execution_analysis.ipynb"
        notebook_path = os.path.join(amp_dir, notebook_path)
        # Build config function and use scratch space path as an argument.
        config_builder = f'oms.test.test_execution_analysis_notebooks.build_test_master_execution_analysis_config("{scratch_space_path}")'
        self._test_run_notebook(notebook_path, config_builder)

    def _copy_data_from_s3_to_scratch(
        self, s3_dir_path: str, scratch_space_path: str, aws_profile: str = "ck"
    ) -> None:
        """
        Copy data from S3 to scratch space.
        """
        _LOG.debug(
            "Copying input data from %s to %s",
            s3_dir_path,
            scratch_space_path,
        )
        cmd = f"aws s3 cp {s3_dir_path} {scratch_space_path} --recursive --profile {aws_profile}"
        hsystem.system(cmd, suppress_output=False, log_level="echo")
        return
