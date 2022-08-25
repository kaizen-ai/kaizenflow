import logging
from typing import Callable, Union

import pandas as pd
import pytest

import core.finance as cofinanc
import dataflow.system as dtfsys
import dataflow.system.test.system_test_case as dtfsytsytc
import dataflow_amp.system.mock1.mock1_forecast_system as dtfasmmfosy
import dataflow_amp.system.mock1.mock1_forecast_system_example as dtfasmmfsex

_LOG = logging.getLogger(__name__)


def _get_test_system_builder_func() -> Callable:
    """
    Get System builder function for unit testing.
    """
    # TODO(Max): In the current system, the time periods are set manually,
    # so the value of `time_interval_str` doesn't affect tests.
    backtest_config = "mock1_v1-top2.5T.Jan2000"
    system_builder_func = (
        lambda: dtfasmmfsex.get_Mock1_ForecastSystem_for_simulation_example1(
            backtest_config
        )
    )
    return system_builder_func


# #############################################################################
# Test_Mock1_System_CheckConfig
# #############################################################################


class Test_Mock1_System_CheckConfig(dtfsytsytc.System_CheckConfig_TestCase1):
    def test_freeze_config1(self) -> None:
        system_builder_func = _get_test_system_builder_func()
        system_builder = system_builder_func()
        self._test_freeze_config1(system_builder)


# #############################################################################
# Test_Mock1_ForecastSystem_FitPredict
# #############################################################################


class Test_Mock1_ForecastSystem_FitPredict(
    dtfsytsytc.ForecastSystem_FitPredict_TestCase1
):
    def get_system(self) -> dtfsys.System:
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
# Test_Mock1_ForecastSystem_FitInvariance
# #############################################################################


class Test_Mock1_ForecastSystem_FitInvariance(
    dtfsytsytc.ForecastSystem_FitInvariance_TestCase1
):
    def test_invariance1(self) -> None:
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
# Test_Mock1_ForecastSystem_CheckPnl
# #############################################################################


class Test_Mock1_ForecastSystem_CheckPnl(
    dtfsytsytc.ForecastSystem_CheckPnl_TestCase1
):
    def test_fit_run1(self) -> None:
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
# Test_Mock1_Time_ForecastSystem1
# #############################################################################


class Test_Mock1_Time_ForecastSystem1(
    dtfsytsytc.Test_Time_ForecastSystem_TestCase1
):
    def test1(self) -> None:
        """
        Verify the contents of DAG prediction.
        """
        system = dtfasmmfosy.Mock1_Time_ForecastSystem()
        market_data, real_time_loop_time_out = cofinanc.get_market_data_df4()
        # Since we are reading from a df there is no delay.
        system.config["market_data_config", "delay_in_secs"] = 0
        system.config["market_data_config", "data"] = market_data
        # We need at least 7 bars to compute volatility.
        system.config[
            "market_data_config", "replayed_delay_in_mins_or_timestamp"
        ] = 35
        # Exercise the system for multiple 5 minute intervals.
        system.config[
            "dag_runner_config", "rt_time_out_in_secs_or_timestamp"
        ] = real_time_loop_time_out
        system.config["dag_runner_config", "bar_duration_in_secs"] = 60 * 5
        #
        output_col_name = "vwap.ret_0.vol_adj.c"
        self._test1(
            system,
            output_col_name=output_col_name,
        )


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DataFramePortfolio1
# #############################################################################


def _get_test_System_with_DataFramePortfolio(
    market_data_df: pd.DataFrame,
    rt_time_out_in_secs_or_timestamp: Union[int, pd.Timestamp],
) -> dtfsys.System:
    """
    Get a System object with a DataFramePortfolio for unit testing.
    """
    system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
        market_data_df, rt_time_out_in_secs_or_timestamp
    )
    return system


class Test_Mock1_Time_ForecastSystem_with_DataFramePortfolio1(
    dtfsytsytc.Time_ForecastSystem_with_DataFramePortfolio_TestCase1
):
    """
    See description in the parent class.
    """

    @pytest.mark.slow("~7 seconds.")
    def test1(self) -> None:
        # Build the system.
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df1()
        system = _get_test_System_with_DataFramePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(system)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1
# #############################################################################


def _get_test_System_with_DatabasePortfolio(
    market_data_df: pd.DataFrame,
    rt_time_out_in_secs_or_timestamp: Union[int, pd.Timestamp],
) -> dtfsys.System:
    """
    Get a System object with a DatabasePortfolio for unit testing.
    """
    system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
        market_data_df, rt_time_out_in_secs_or_timestamp
    )
    return system


class Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1(
    dtfsytsytc.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1
):
    """
    Test a Mock1 system with DatabasePortfolio.

    See description in the parent class.
    """

    @pytest.mark.slow("~6 seconds.")
    def test_market_data1_database_portfolio(self) -> None:
        # Build the system.
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df1()
        system = _get_test_System_with_DatabasePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(system)

    @pytest.mark.slow("~6 seconds.")
    def test_market_data2_database_portfolio(self) -> None:
        # Build the system.
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df2()
        system = _get_test_System_with_DatabasePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(system)

    @pytest.mark.slow("~15 seconds.")
    def test_market_data3_database_portfolio(self) -> None:
        # Build the system.
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df3()
        system = _get_test_System_with_DatabasePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(system)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio1
# #############################################################################


class Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio1(
    dtfsytsytc.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio_TestCase1
):
    """
    Run a Mock1 system with DatabasePortfolio and DataFramePortfolio and verify
    that the output is the same.

    See description in the parent class.
    """

    @pytest.mark.slow("~10 seconds.")
    def test1(self) -> None:
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df1()
        # Build the systems to compare.
        system_with_dataframe_portfolio = (
            _get_test_System_with_DataFramePortfolio(
                data, rt_time_out_in_secs_or_timestamp
            )
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(
            system_with_dataframe_portfolio, system_with_database_portfolio
        )

    @pytest.mark.slow("~10 seconds.")
    def test2(self) -> None:
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df2()
        # Build the systems to compare.
        system_with_dataframe_portfolio = (
            _get_test_System_with_DataFramePortfolio(
                data, rt_time_out_in_secs_or_timestamp
            )
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(
            system_with_dataframe_portfolio, system_with_database_portfolio
        )

    @pytest.mark.superslow("~30 seconds.")
    def test3(self) -> None:
        data, rt_time_out_in_secs_or_timestamp = cofinanc.get_market_data_df3()
        # Build the systems to compare.
        system_with_dataframe_portfolio = (
            _get_test_System_with_DataFramePortfolio(
                data, rt_time_out_in_secs_or_timestamp
            )
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, rt_time_out_in_secs_or_timestamp
        )
        # Run.
        self._test1(
            system_with_dataframe_portfolio, system_with_database_portfolio
        )
