"""
Import as:

import dataflow_amp.system.mock1.mock1_forecast_system as dtfasmmfosy
"""

import logging
from typing import Coroutine

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import dataflow_amp.pipelines as dtfapi
import dataflow_amp.system.mock1.mock1_builders as dtfasmmobu
import im_v2.common.data.client as icdc
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


# #############################################################################
# Mock1_ForecastSystem
# #############################################################################


class Mock1_ForecastSystem(dtfsys.ForecastSystem):
    """
    Create a System with:

    - a ReplayedMarketData
    - a non-timed Mock1 DAG

    This is used to run an historical simulation of a Mock1 system.
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfapi.Mock1_DagBuilder()
        system_config = dtfsys.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ImClientMarketData:
        market_data = dtfasmmobu.get_Mock1_MarketData_example2(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfasmmobu.get_Mock1_HistoricalDag_example1(self)
        return dag

    def _get_dag_runner(self) -> dtfcore.DagRunner:
        dag_runner = dtfcore.FitPredictDagRunner(self.dag)
        return dag_runner


def get_Mock1_ForecastSystem_for_simulation_example1(
    backtest_config: str,
) -> dtfsys.ForecastSystem:
    """
    Get Mock1_ForecastSystem object for backtest simulation.
    """
    system = Mock1_ForecastSystem()
    system = dtfsys.apply_backtest_config(system, backtest_config)
    # Fill pipeline-specific backtest config parameters.
    system.config["backtest_config", "freq_as_pd_str"] = "M"
    system.config["backtest_config", "lookback_as_pd_str"] = "10D"
    # Fill `MarketData` related config.
    system.config[
        "market_data_config", "im_client_ctor"
    ] = icdc.get_DataFrameImClient_example1
    system.config["market_data_config", "im_client_config"] = cconfig.Config()
    # Set the research PNL parameters.
    forecast_evaluator_from_prices_dict = {
        "style": "cross_sectional",
        "init": {
            "price_col": "vwap",
            "volatility_col": "vwap.ret_0.vol",
            "prediction_col": "vwap.ret_0.vol_adj.c",
        },
        "kwargs": {
            "target_gmv": 1e5,
            "liquidate_at_end_of_day": False,
        },
    }
    system.config[
        "research_forecast_evaluator_from_prices"
    ] = cconfig.Config.from_dict(forecast_evaluator_from_prices_dict)
    system = dtfsys.apply_market_data_config(system)
    return system


# #############################################################################
# Mock1_Time_ForecastSystem
# #############################################################################


class Mock1_Time_ForecastSystem(dtfsys.Time_ForecastSystem):
    """
    Create a System with:

    - a ReplayedMarketData
    - a timed Mock1 DAG
    - a RealTimeDagRunner
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfapi.Mock1_DagBuilder()
        system_config = dtfsys.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfsys.get_EventLoop_MarketData_from_df(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfasmmobu.get_Mock1_RealtimeDag_example2(self)
        return dag

    def _get_dag_runner(self) -> dtfsys.RealTimeDagRunner:
        dag_runner = dtfsys.get_RealTimeDagRunner_from_System(self)
        return dag_runner


# #############################################################################
# Mock1_Time_ForecastSystem_with_DataFramePortfolio
# #############################################################################


class Mock1_Time_ForecastSystem_with_DataFramePortfolio(
    dtfsys.Time_ForecastSystem_with_DataFramePortfolio
):
    """
    Build a system with:

    - a ReplayedTimeMarketData
    - a timed Mock1 DAG
    - a DataFramePortfolio
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfapi.Mock1_DagBuilder()
        system_config = dtfsys.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfsys.get_EventLoop_MarketData_from_df(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfasmmobu.get_Mock1_RealtimeDag_example3(self)
        return dag

    def _get_portfolio(self) -> oms.Portfolio:
        portfolio = dtfsys.get_DataFramePortfolio_from_System(self)
        return portfolio

    def _get_dag_runner(self) -> dtfsys.RealTimeDagRunner:
        dag_runner = dtfsys.get_RealTimeDagRunner_from_System(self)
        return dag_runner


def get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
    market_data_df: pd.DataFrame,
    real_time_loop_time_out_in_secs: int,
) -> dtfsys.System:
    """
    The System is used for the corresponding unit tests.
    """
    system = Mock1_Time_ForecastSystem_with_DataFramePortfolio()
    # Market data config.
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    system.config["market_data_config", "delay_in_secs"] = 5
    system.config["market_data_config", "initial_replayed_delay"] = 5
    system.config["market_data_config", "asset_ids"] = [101]
    system.config["market_data_config", "data"] = market_data_df
    # Portfolio config.
    system = dtfsys.apply_Portfolio_config(system)
    # Dag runner config.
    system.config["dag_runner_config", "sleep_interval_in_secs"] = 60 * 5
    system.config[
        "dag_runner_config", "real_time_loop_time_out_in_secs"
    ] = real_time_loop_time_out_in_secs
    # PnL config.
    forecast_evaluator_from_prices_dict = {
        "style": "cross_sectional",
        "init": {
            "price_col": "vwap",
            "volatility_col": "vwap.ret_0.vol",
            "prediction_col": "feature1",
        },
        "kwargs": {
            "target_gmv": 1e5,
            "liquidate_at_end_of_day": False,
        },
    }
    system.config[
        "research_forecast_evaluator_from_prices"
    ] = cconfig.Config.from_dict(forecast_evaluator_from_prices_dict)
    return system


# #############################################################################
# Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
# #############################################################################


class Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor(
    dtfsys.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
):
    """
    A System with:

    - a `ReplayedMarketData`
    - a timed `Mock1` DAG
    - a `DatabasePortfolio` (which includes a `DatabaseBroker`)
    - an `OrderProcessor`
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfapi.Mock1_DagBuilder()
        system_config = dtfsys.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfsys.get_EventLoop_MarketData_from_df(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfasmmobu.get_Mock1_RealtimeDag_example3(self)
        return dag

    def _get_portfolio(self) -> oms.Portfolio:
        portfolio = dtfsys.get_DatabasePortfolio_from_System(self)
        return portfolio

    def _get_order_processor(self) -> Coroutine:
        order_processor_coroutine = (
            dtfsys.get_OrderProcessorCoroutine_from_System(self)
        )
        return order_processor_coroutine

    def _get_dag_runner(self) -> dtfsys.RealTimeDagRunner:
        dag_runner = dtfsys.get_RealTimeDagRunner_from_System(self)
        return dag_runner


# TODO(Grisha): @Dan move all examples to a `_example.py` file.
def get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
    market_data_df: pd.DataFrame,
    real_time_loop_time_out_in_secs: int,
) -> dtfsys.System:
    """
    The System is used for the corresponding unit tests.
    """
    system = Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor()
    # Market data config.
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    system.config["market_data_config", "delay_in_secs"] = 5
    system.config["market_data_config", "initial_replayed_delay"] = 5
    system.config["market_data_config", "asset_ids"] = [101]
    system.config["market_data_config", "data"] = market_data_df
    # Portfolio config.
    system = dtfsys.apply_Portfolio_config(system)
    # Dag runner config.
    system.config["dag_runner_config", "sleep_interval_in_secs"] = 60 * 5
    system.config[
        "dag_runner_config", "real_time_loop_time_out_in_secs"
    ] = real_time_loop_time_out_in_secs
    # PnL config.
    forecast_evaluator_from_prices_dict = {
        "style": "cross_sectional",
        "init": {
            "price_col": "vwap",
            "volatility_col": "vwap.ret_0.vol",
            "prediction_col": "feature1",
        },
        "kwargs": {
            "target_gmv": 1e5,
            "liquidate_at_end_of_day": False,
        },
    }
    system.config[
        "research_forecast_evaluator_from_prices"
    ] = cconfig.Config.from_dict(forecast_evaluator_from_prices_dict)
    # If an order is not placed within a bar, then there is a timeout, so
    # we add extra 5 seconds to `sleep_interval_in_secs` (which represents
    # the length of a trading bar) to make sure that the `OrderProcessor`
    # waits long enough before timing out.
    max_wait_time_for_order_in_secs = (
        system.config["dag_runner_config", "sleep_interval_in_secs"] + 5
    )
    system.config[
        "order_processor_config", "max_wait_time_for_order_in_secs"
    ] = max_wait_time_for_order_in_secs
    # We add extra 5 seconds for the `OrderProcessor` to account for the first bar
    # that the DAG spends in fit mode.
    real_time_loop_time_out_in_secs = (
        system.config["dag_runner_config", "real_time_loop_time_out_in_secs"] + 5
    )
    system.config[
        "order_processor_config", "duration_in_secs"
    ] = real_time_loop_time_out_in_secs
    return system
