"""
Import as:

import dataflow.system.example_pipeline1_system_runner as dtfsepsyru
"""

import logging
from typing import List, Optional, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.pipelines.examples.example1_pipeline as dtfpexexpi
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import dataflow.system.system_runner as dtfsysyrun
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


# TODO(gp): -> system_runner_example.py for symmetry with the other _example?


class Example1_ForecastSystem(dtfsysyrun.ForecastSystem):
    """
    Create a system with:
    - a ReplayedMarketData
    - an Example1 DAG
    """
    def __init__(self, asset_ids: List[int], event_loop=None):
        self._asset_ids = asset_ids
        self._event_loop = event_loop

    def get_market_data(
        self,
        data: pd.DataFrame,
        initial_replayed_delay: int = 5,
    ):
        market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
            self._event_loop,
            initial_replayed_delay,
            data,
        )
        return market_data

    def get_dag(
        self,
        *,
        prediction_col: str = "feature1",
        volatility_col: str = "vwap.ret_0.vol",
        returns_col: str = "vwap.ret_0",
        spread_col: Optional[str] = None,
        timedelta: pd.Timedelta = pd.Timedelta("7D"),
        asset_id_col: str = "asset_id",
        log_dir: Optional[str] = None,
    ) -> Tuple[cconfig.Config, dtfcore.DagBuilder]:
        # # TODO(gp): We should replace the DagAdapter with the new approach.
        # base_dag_builder = dtfpexexpi.Example1_DagBuilder()
        # # dag_builder = dtfsrtdaad.RealTimeDagAdapter(
        # #     base_dag_builder,
        # #     portfolio,
        # #     prediction_col,
        # #     volatility_col,
        # #     returns_col,
        # #     spread_col,
        # #     timedelta,
        # #     asset_id_col,
        # #     log_dir=log_dir,
        # # )
        # #_LOG.debug("dag_builder=\n%s", dag_builder)
        # config = dag_builder.get_config_template()
        # return config, dag_builder

        # TODO(gp): -> config
        backtest_config = cconfig.Config()
        # Save the `DagBuilder` and the `DagConfig` in the config.
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        dag_config = dag_builder.get_config_template()
        backtest_config["DAG"] = dag_config
        backtest_config["meta", "dag_builder"] = dag_builder
        return backtest_config

    def get_dag_runner(config: cconfig.Config,
                       market_data
                       ) -> dtfcore.AbstractDagRunner:
        """
        Build a DAG runner from a config.
        """
        # # TODO(gp): In the previous code asset_ids was coming from:
        # #  `asset_ids = dtfuniver.get_universe(universe_str)`.
        # asset_ids = [3303714233, 1467591036]
        # columns: List[str] = []
        # columns_remap = None
        # market_data = mdata.get_ImClientMarketData_example2(
        #     asset_ids, columns, columns_remap
        # )
        # # Create HistoricalDataSource.
        # stage = "read_data"
        # asset_id_col = "asset_id"
        # # TODO(gp): This in the original code was
        # #  `ts_col_name = "timestamp_db"`.
        # ts_col_name = "end_ts"
        # multiindex_output = True
        # # col_names_to_remove = ["start_datetime", "timestamp_db"]
        # col_names_to_remove = ["start_ts"]
        # node = dtfsysonod.HistoricalDataSource(
        #     stage,
        #     market_data,
        #     asset_id_col,
        #     ts_col_name,
        #     multiindex_output,
        #     col_names_to_remove=col_names_to_remove,
        # )
        # Build the DAG.
        dag_builder = config["meta", "dag_builder"]
        dag = dag_builder.get_dag(config["DAG"])
        # This is for debugging. It saves the output of each node in a `csv` file.
        # dag.set_debug_mode("df_as_csv", False, "crypto_forever")
        if False:
            dag.force_freeing_nodes = True
        # Add the data source node.
        dag.insert_at_head(stage, node)

# #############################################################################


# class Example1_Dataframe_SystemRunner(Example1_SystemRunner):
#     def get_portfolio(
#         self,
#         market_data: mdata.MarketData,
#     ) -> oms.AbstractPortfolio:
#         portfolio = oms.get_DataFramePortfolio_example1(
#             self._event_loop,
#             market_data=market_data,
#             mark_to_market_col="close",
#             pricing_method="twap.5T",
#             asset_ids=self._asset_ids,
#         )
#         portfolio.broker._column_remap = {
#             "bid": "bid",
#             "ask": "ask",
#             "midpoint": "midpoint",
#             "price": "close",
#         }
#         return portfolio
#
#
# # #############################################################################
#
#
# class Example1_Database_SystemRunner(
#     dtfsysyrun.SystemWithSimulatedOmsRunner, Example1_SystemRunner
# ):
#     def get_portfolio(
#         self,
#         market_data: mdata.MarketData,
#     ) -> oms.AbstractPortfolio:
#         table_name = oms.CURRENT_POSITIONS_TABLE_NAME
#         portfolio = oms.get_mocked_portfolio_example1(
#             self._event_loop,
#             self._db_connection,
#             table_name,
#             market_data=market_data,
#             mark_to_market_col="close",
#             pricing_method="twap.5T",
#             asset_ids=self._asset_ids,
#         )
#         portfolio.broker._column_remap = {
#             "bid": "bid",
#             "ask": "ask",
#             "midpoint": "midpoint",
#             "price": "close",
#         }
#         return portfolio
