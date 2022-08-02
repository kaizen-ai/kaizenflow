"""
Import as:

import dataflow.system.example1.example1_forecast_system as dtfseefosy
"""

import logging

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.pipelines.example1.example1_pipeline as dtfpexexpi
import dataflow.system.example1.example1_builders as dtfsexexbu
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_builder_utils as dtfssybuut
import im_v2.common.data.client as icdc
import market_data as mdata
import oms

_LOG = logging.getLogger(__name__)


# #############################################################################
# Example1_ForecastSystem
# #############################################################################


class Example1_ForecastSystem(dtfsyssyst.ForecastSystem):
    """
    Create a System with:

    - a ReplayedMarketData
    - an non-timed Example1 DAG

    This is used to run an historical simulation of an Example1 system.
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfssybuut.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ImClientMarketData:
        market_data = dtfsexexbu.get_Example1_MarketData_example2(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_HistoricalDag_example1(self)
        return dag

    def _get_dag_runner(self) -> dtfcore.DagRunner:
        dag_runner = dtfcore.FitPredictDagRunner(self.dag)
        return dag_runner


def get_Example1_ForecastSystem_for_simulation_example1(
    backtest_config: str,
) -> dtfsyssyst.ForecastSystem:
    """
    Get Example1_ForecastSystem object for backtest simulation.
    """
    system = Example1_ForecastSystem()
    system = dtfssybuut.apply_backtest_config(system, backtest_config)
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
    ] = cconfig.get_config_from_nested_dict(forecast_evaluator_from_prices_dict)
    system = dtfssybuut.apply_market_data_config(system)
    return system


# #############################################################################
# Example1_Time_ForecastSystem
# #############################################################################


class Example1_Time_ForecastSystem(dtfsyssyst.Time_ForecastSystem):
    """
    Create a System with:

    - a ReplayedMarketData
    - an Example1 DAG
    - a RealTimeDagRunner
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfssybuut.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfssybuut.get_EventLoop_MarketData_from_df(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_RealtimeDag_example2(self)
        return dag

    def _get_dag_runner(self) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = dtfssybuut.get_RealTimeDagRunner_from_System(self)
        return dag_runner


# #############################################################################
# Example1_Time_ForecastSystem_with_DataFramePortfolio
# #############################################################################


class Example1_Time_ForecastSystem_with_DataFramePortfolio(
    dtfsyssyst.Time_ForecastSystem_with_DataFramePortfolio
):
    """
    Build a system with:

    - a ReplayedTimeMarketData
    - a time Example1 DAG
    - a DataFramePortfolio
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfssybuut.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(self) -> mdata.ReplayedMarketData:
        market_data = dtfssybuut.get_EventLoop_MarketData_from_df(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_RealtimeDag_example3(self)
        return dag

    def _get_portfolio(self) -> oms.Portfolio:
        portfolio = dtfssybuut.get_DataFramePortfolio_from_System(self)
        return portfolio

    def _get_dag_runner(self) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = dtfssybuut.get_RealTimeDagRunner_from_System(self)
        return dag_runner


def get_Example1_Time_ForecastSystem_with_DataFramePortfolio_example1(
    market_data_df: pd.DataFrame,
    real_time_loop_time_out_in_secs: int,
) -> dtfsyssyst.System:
    """
    The System is used for the corresponding unit tests.
    """
    system = Example1_Time_ForecastSystem_with_DataFramePortfolio()
    # Market data config.
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    system.config["market_data_config", "delay_in_secs"] = 0
    system.config["market_data_config", "initial_replayed_delay"] = 5
    system.config["market_data_config", "asset_ids"] = [101]
    system.config["market_data_config", "data"] = market_data_df
    # Portfolio config.
    system.config["portfolio_config", "mark_to_market_col"] = "close"
    system.config["portfolio_config", "pricing_method"] = "twap.5T"
    system.config["portfolio_config", "column_remap"] = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
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
    ] = cconfig.get_config_from_nested_dict(forecast_evaluator_from_prices_dict)
    return system


# #############################################################################
# Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
# #############################################################################


class Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor(
    dtfsyssyst.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
):
    """
    A System with:

    - a `ReplayedMarketData`
    - an `Example1` DAG
    - a `DatabasePortfolio` (which includes a `DatabaseBroker`)
    - an `OrderProcessor`
    """

    def _get_system_config_template(self) -> cconfig.Config:
        _ = self
        dag_builder = dtfpexexpi.Example1_DagBuilder()
        system_config = dtfssybuut.get_SystemConfig_template_from_DagBuilder(
            dag_builder
        )
        return system_config

    def _get_market_data(
        self,
    ) -> mdata.ReplayedMarketData:
        market_data = dtfssybuut.get_EventLoop_MarketData_from_df(self)
        return market_data

    def _get_dag(self) -> dtfcore.DAG:
        dag = dtfsexexbu.get_Example1_RealtimeDag_example3(self)
        return dag

    def _get_portfolio(self) -> oms.Portfolio:
        portfolio = dtfssybuut.get_DatabasePortfolio_from_System(self)
        return portfolio

    def _get_dag_runner(self) -> dtfsrtdaru.RealTimeDagRunner:
        dag_runner = dtfssybuut.get_RealTimeDagRunner_from_System(self)
        return dag_runner


# TODO(Grisha): @Dan move all examples to a `_example.py` file.
def get_Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
    market_data_df: pd.DataFrame,
    real_time_loop_time_out_in_secs: int,
) -> dtfsyssyst.System:
    """
    The System is used for the corresponding unit tests.
    """
    system = (
        Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor()
    )
    # Market data config.
    system.config["market_data_config", "asset_id_col_name"] = "asset_id"
    system.config["market_data_config", "delay_in_secs"] = 0
    system.config["market_data_config", "initial_replayed_delay"] = 5
    system.config["market_data_config", "asset_ids"] = [101]
    system.config["market_data_config", "data"] = market_data_df
    # Portfolio config.
    system.config["portfolio_config", "mark_to_market_col"] = "close"
    system.config["portfolio_config", "pricing_method"] = "twap.5T"
    system.config["portfolio_config", "column_remap"] = {
        "bid": "bid",
        "ask": "ask",
        "midpoint": "midpoint",
        "price": "close",
    }
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
    ] = cconfig.get_config_from_nested_dict(forecast_evaluator_from_prices_dict)
    return system
