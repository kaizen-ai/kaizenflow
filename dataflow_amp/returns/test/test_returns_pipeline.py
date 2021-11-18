import logging
import os
from typing import Any, Dict

import pandas as pd
import pytest

import core.config as cconfig
import core.dataflow as dtf
import core.dataflow_source_nodes as cdtfsonod
import dataflow_amp.returns.pipeline as dtfamrepip
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestReturnsBuilder(hunitest.TestCase):
    """
    Test the ReturnsBuilder pipeline.
    """

    @pytest.mark.slow
    def test_equities1(self) -> None:
        # TODO(gp): This node doesn't work with the rest of the pipeline since
        #  it seems that it is multi-index.
        # source_node_kwargs = {
        #     "source_node_name": "kibot_equities",
        #     "source_node_kwargs": {
        #         "frequency": "T",
        #         "symbols": ["AAPL"],
        #         "start_date": "2019-01-04 09:00:00",
        #         "end_date": "2019-01-04 16:30:00",
        #     }
        # }
        from im.kibot.data.config import S3_PREFIX

        # TODO(gp): We could use directly a DiskDataSource here.
        ticker = "AAPL"
        config = {
            "func": dtf.load_data_from_disk,
            "func_kwargs": {
                "file_path": os.path.join(
                    S3_PREFIX, "pq/sp_500_1min", ticker + ".pq"
                ),
                "aws_profile": "am",
                "start_date": pd.to_datetime("2010-01-04 9:30:00"),
                "end_date": pd.to_datetime("2010-01-04 16:05:00"),
            },
        }
        self._helper(config)

    @pytest.mark.slow
    def test_futures1(self) -> None:
        source_node_kwargs = {
            "func": cdtfsonod.load_kibot_data,
            "func_kwargs": {
                "frequency": "T",
                "contract_type": "continuous",
                "symbol": "ES",
                "start_date": "2010-01-04 09:00:00",
                "end_date": "2010-01-04 16:30:00",
            },
        }
        self._helper(source_node_kwargs)

    def _helper(self, source_node_kwargs: Dict[str, Any]) -> None:
        dag_builder = dtfamrepip.ReturnsPipeline()
        config = dag_builder.get_config_template()
        # Inject the node.
        config["load_prices"] = cconfig.get_config_from_nested_dict(
            {
                "source_node_name": "DataLoader",
                "source_node_kwargs": source_node_kwargs,
            }
        )
        #
        dag_runner = dtf.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        str_output = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)
