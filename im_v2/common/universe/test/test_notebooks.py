import logging
import os

import pandas as pd
import pytest

import core.config as cconfig
import core.finance.bid_ask as cfibiask
import dev_scripts.notebooks as dsnrn
import helpers.hgit as hgit
import helpers.hserver as hserver

_LOG = logging.getLogger(__name__)


def build_test_universe_analysis_config() -> cconfig.ConfigList:
    """
    Default config builder for testing the Master_universe_analysis notebook.
    """
    universe_version = "v7.1"
    config = {
        "universe": {
            "vendor": "CCXT",
            "mode": "trade",
            "version": universe_version,
        },
        "start_timestamp": pd.Timestamp("2023-11-01 00:00:00+00:00", tz="UTC"),
        "end_timestamp": pd.Timestamp("2023-11-03T00:00:00", tz="UTC"),
        "bid_ask_data": {
            "im_client_config": {
                "universe_version": universe_version,
                # Data currently residing in the test bucket
                "root_dir": "s3://cryptokaizen-unit-test/v3",
                "partition_mode": "by_year_month",
                "dataset": "bid_ask",
                "contract_type": "futures",
                # Data snapshot is not applicable for data version = "v3".
                "data_snapshot": "",
                "version": "v1_0_0",
                "download_universe_version": "v7",
                "tag": "resampled_1min",
                "aws_profile": "ck",
            },
            # TODO(Grisha): for some reason the current filtering mechanism filters out `asset_ids` which
            # makes it impossible to stitch the 2 market data dfs. So adding the necessary columns manually.
            "columns": cfibiask.get_bid_ask_columns_by_level(1)
            + ["asset_id", "full_symbol", "start_ts", "knowledge_timestamp"],
            "column_remap": None,
            "filter_data_mode": "assert",
        },
        "ohlcv_data": {
            "resampling_rule": "D",
        },
        "column_names": {
            "timestamp": "timestamp",
            "full_symbol_column": "full_symbol",
            "close": "close",
            "volume": "volume",
            "volume_notional": "volume_notional",
        },
        "bar_duration": "5T",
    }
    config = cconfig.Config().from_dict(config)
    config_list = cconfig.ConfigList([config])
    return config_list


@pytest.mark.skipif(
    hserver.is_inside_ci(),
    reason="The prod database is not available via GH actions",
)
class Test_run_master_universe_analysis_notebook(
    dsnrn.Test_Run_Notebook_TestCase
):
    @pytest.mark.superslow("~67 seconds.")
    def test1(self) -> None:
        """
        Run `im_v2/common/universe/notebooks/Master_universe_analysis.ipynb`
        notebook end-to-end.
        """
        amp_dir = hgit.get_amp_abs_path()
        notebook_path = (
            "im_v2/common/universe/notebooks/Master_universe_analysis.ipynb"
        )
        notebook_path = os.path.join(amp_dir, notebook_path)
        #
        config_builder = "im_v2.common.universe.test.test_notebooks.build_test_universe_analysis_config()"
        self._test_run_notebook(notebook_path, config_builder)
