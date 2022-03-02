"""
Import as:

import dataflow.system.example_pipeline1_system_runner as dtfsepsyru
"""


import logging
from typing import List, Optional, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.pipelines.examples.pipeline1 as dtfpiexpip
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import dataflow.system.system_runner as dtfsysyrun
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


class ExamplePipeline1_SystemRunner(dtfsysyrun.SystemRunner):
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
        portfolio: oms.AbstractPortfolio,
        *,
        prediction_col: str = "feature1",
        volatility_col: str = "vwap.ret_0.vol",
        returns_col: str = "vwap.ret_0",
        timedelta: pd.Timedelta = pd.Timedelta("7D"),
        asset_id_col: str = "asset_id",
        log_dir: Optional[str] = None,
    ) -> Tuple[cconfig.Config, dtfcore.DagBuilder]:
        base_dag_builder = dtfpiexpip.ExamplePipeline1_ModelBuilder()
        dag_builder = dtfsrtdaad.RealTimeDagAdapter(
            base_dag_builder,
            portfolio,
            prediction_col,
            volatility_col,
            returns_col,
            timedelta,
            asset_id_col,
            log_dir=log_dir,
        )
        _LOG.debug("dag_builder=\n%s", dag_builder)
        config = dag_builder.get_config_template()
        return config, dag_builder


class ExamplePipeline1_Dataframe_SystemRunner(ExamplePipeline1_SystemRunner):
    def get_portfolio(
        self,
        market_data: mdata.MarketData,
    ) -> oms.AbstractPortfolio:
        portfolio = oms.get_DataFramePortfolio_example1(
            self._event_loop,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=self._asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio


class ExamplePipeline1_Database_SystemRunner(
    dtfsysyrun.SystemWithSimulatedOmsRunner, ExamplePipeline1_SystemRunner
):
    def get_portfolio(
        self,
        market_data: mdata.MarketData,
    ) -> oms.AbstractPortfolio:
        table_name = oms.CURRENT_POSITIONS_TABLE_NAME
        portfolio = oms.get_mocked_portfolio_example1(
            self._event_loop,
            self._db_connection,
            table_name,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=self._asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio
