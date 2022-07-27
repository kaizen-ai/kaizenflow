import asyncio
from distutils.command.sdist import sdist
import logging
from typing import Callable

import pandas as pd
import pytest

import core.finance as cofinanc
import dataflow.system.example1.example1_forecast_system as dtfseefosy
import dataflow.system.system as dtfsyssyst
import dataflow.system.test.system_test_case as dtfsytsytc
import helpers.hasyncio as hasynci
import oms as oms
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)


def _get_test_system_builder_func() -> Callable:
    """
    Get System builder function for unit testing.
    """
    backtest_config = "example1_v1-top2.5T.Jan2000"
    system_builder_func = (
        lambda: dtfseefosy.get_Example1_ForecastSystem_for_simulation_example1(
            backtest_config
        )
    )
    return system_builder_func


# #############################################################################
# Test_Example1_System_CheckConfig
# #############################################################################


class Test_Example1_System_CheckConfig(dtfsytsytc.System_CheckConfig_TestCase1):
    def test_freeze_config1(self) -> None:
        system_builder_func = _get_test_system_builder_func()
        system_builder = system_builder_func()
        self._test_freeze_config1(system_builder)


# #############################################################################
# Test_Example1_ForecastSystem_FitPredict
# #############################################################################


class Test_Example1_ForecastSystem_FitPredict(
    dtfsytsytc.ForecastSystem_FitPredict_TestCase1
):
    def get_system(self) -> dtfsyssyst.System:
        """
        Create the System for testing.
        """
        system_builder_func = _get_test_system_builder_func()
        system = system_builder_func()
        system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ] = pd.Timestamp("2000-01-01 00:00:00+0000", tz="UTC")
        system.config["backtest_config", "end_timestamp"] = pd.Timestamp(
            "2000-01-31 00:00:00+0000", tz="UTC"
        )
        return system

    def test_fit_over_backtest_period1(self) -> None:
        system = self.get_system()
        output_col_name = "vwap.ret_0.vol_adj.c"
        self._test_fit_over_backtest_period1(system, output_col_name)

    def test_fit_over_period1(self) -> None:
        system = self.get_system()
        start_timestamp = pd.Timestamp("2000-01-01 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2000-01-31 00:00:00+0000", tz="UTC")
        output_col_name = "vwap.ret_0.vol_adj.c"
        self._test_fit_over_period1(
            system,
            start_timestamp,
            end_timestamp,
            output_col_name=output_col_name,
        )

    def test_fit_vs_predict1(self) -> None:
        system = self.get_system()
        self._test_fit_vs_predict1(system)


# #############################################################################
# Test_Example1_ForecastSystem_FitInvariance
# #############################################################################


class Test_Example1_ForecastSystem_FitInvariance(
    dtfsytsytc.ForecastSystem_FitInvariance_TestCase1
):
    def test_test_invariance1(self) -> None:
        system_builder_func = _get_test_system_builder_func()
        start_timestamp1 = pd.Timestamp("2000-01-01 00:00:00+0000", tz="UTC")
        start_timestamp2 = pd.Timestamp("2000-01-01 09:40:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2000-01-31 00:00:00+0000", tz="UTC")
        compare_start_timestamp = pd.Timestamp(
            "2000-01-01 09:50:00+0000", tz="UTC"
        )
        self._test_invariance1(
            system_builder_func,
            start_timestamp1,
            start_timestamp2,
            end_timestamp,
            compare_start_timestamp,
        )


# #############################################################################
# Test_Example1_ForecastSystem_CheckPnl
# #############################################################################


class Test_Example1_ForecastSystem_CheckPnl(
    dtfsytsytc.ForecastSystem_CheckPnl_TestCase1
):
    def test_test_fit_run1(self) -> None:
        system_builder_func = _get_test_system_builder_func()
        system = system_builder_func()
        system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ] = pd.Timestamp("2000-01-01 00:00:00+0000", tz="UTC")
        system.config["backtest_config", "end_timestamp"] = pd.Timestamp(
            "2000-01-31 00:00:00+0000", tz="UTC"
        )
        self._test_fit_run1(system)


# #############################################################################
# Test_Example1_Time_ForecastSystem1
# #############################################################################


class Test_Example1_Time_ForecastSystem1(
    dtfsytsytc.Test_Time_ForecastSystem_TestCase1
):
    def test1(self) -> None:
        """
        Verify the contents of DAG prediction.
        """
        system = dtfseefosy.Example1_Time_ForecastSystem()
        # TODO(Dan): Add more data, otherwise volatility is NaN.
        market_data, _ = cofinanc.get_market_data_df1()
        # Since we are reading from a df there is no delay.
        system.config["market_data_config", "delay_in_secs"] = 0
        system.config["market_data_config", "data"] = market_data
        system.config["market_data_config", "initial_replayed_delay"] = 5
        # Exercise the system for multiple 5 minute intervals.
        system.config["dag_runner_config", "real_time_loop_time_out_in_secs"] = (
            60 * 5 * 3
        )
        system.config["dag_runner_config", "sleep_interval_in_secs"] = 60 * 5
        #
        output_col_name = "vwap.ret_0.vol_adj.c"
        self._test1(
            system,
            output_col_name=output_col_name,
        )


# #############################################################################
# Test_Example1_Time_ForecastSystem_with_DataFramePortfolio1
# #############################################################################


def _get_test_System_with_DataFramePortfolio(
    market_data_df: pd.DataFrame,
    real_time_loop_time_out_in_secs: int,
) -> Callable:
    """
    Get a System object with a DataFramePortfolio for unit testing.
    """
    system = dtfseefosy.get_Example1_Time_ForecastSystem_with_DataFramePortfolio_example1(
        market_data_df, real_time_loop_time_out_in_secs
    )
    return system


class Test_Example1_Time_ForecastSystem_with_DataFramePortfolio1(
    dtfsytsytc.Time_ForecastSystem_with_DataFramePortfolio_TestCase1
):
    """
    Test an end-to-end `System`, containing:

    - a `MarketData` using fake data and features
    - a Example1 pipeline
    - a `Portfolio` backed by a dataframe
    """

    @pytest.mark.slow("~7 seconds.")
    def test1(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        #
        system = _get_test_System_with_DataFramePortfolio(data, real_time_loop_time_out_in_secs)
        #
        self._test1(system)


# #############################################################################
# Test_Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1
# #############################################################################


def _get_test_System_with_DatabasePortfolio(
    market_data_df: pd.DataFrame,
    real_time_loop_time_out_in_secs: int,
) -> Callable:
    """
    Get a System object with a DatabasePortfolio for unit testing.
    """
    system = dtfseefosy.get_Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
        market_data_df, real_time_loop_time_out_in_secs
    )
    return system


class Test_Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1(
    dtfsytsytc.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1
):
    """
    See description in the parent class.
    """

    @pytest.mark.slow("~6 seconds.")
    def test_market_data1_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        #
        system = _get_test_System_with_DatabasePortfolio(data, real_time_loop_time_out_in_secs)
        #
        self._test1(system)

    @pytest.mark.slow("~6 seconds.")
    def test_market_data2_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df2()
        #
        system = _get_test_System_with_DatabasePortfolio(data, real_time_loop_time_out_in_secs)
        #
        self._test1(system)

    @pytest.mark.slow("~15 seconds.")
    def test_market_data3_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        #
        system = _get_test_System_with_DatabasePortfolio(data, real_time_loop_time_out_in_secs)
        #
        self._test1(system)


# #########################################################################################
# Test_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio1
# #########################################################################################


class Test_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio1(
    dtfsytsytc.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio_TestCase1
):
    """
    See description in the parent class.
    """
    @pytest.mark.slow("~10 seconds.")
    def test1(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        #
        system_with_dataframe_portfolio = _get_test_System_with_DataFramePortfolio(
            data, real_time_loop_time_out_in_secs
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, real_time_loop_time_out_in_secs
        )
        #
        self._test1(system_with_dataframe_portfolio, system_with_database_portfolio)

    @pytest.mark.slow("~10 seconds.")
    def test2(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df2()
        #
        system_with_dataframe_portfolio = _get_test_System_with_DataFramePortfolio(
            data, real_time_loop_time_out_in_secs
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, real_time_loop_time_out_in_secs
        )
        #
        self._test1(system_with_dataframe_portfolio, system_with_database_portfolio)

    @pytest.mark.superslow("~30 seconds.")
    def test3(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        #
        system_with_dataframe_portfolio = _get_test_System_with_DataFramePortfolio(
            data, real_time_loop_time_out_in_secs
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, real_time_loop_time_out_in_secs
        )
        #
        self._test1(system_with_dataframe_portfolio, system_with_database_portfolio)
