import datetime
import logging
from typing import Callable, Optional, Tuple, Union

import pandas as pd
import pytest

import core.finance as cofinanc
import dataflow.system as dtfsys
import dataflow_amp.system.mock1.mock1_forecast_system as dtfasmmfosy
import dataflow_amp.system.mock1.mock1_forecast_system_example as dtfasmmfsex
import helpers.hpandas as hpandas
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


def _get_test_system_builder_func() -> Callable:
    """
    Get a function building a Mock1 non-time forecast system for unit testing.
    """
    # In this system the time periods are set manually, so the value of
    # `time_interval_str` doesn't affect tests.
    backtest_config = "mock1_v1-top2.5T.Jan2000"
    system_builder_func = lambda: dtfasmmfsex.get_Mock1_NonTime_ForecastSystem_for_simulation_example1(
        backtest_config
    )
    return system_builder_func


# #############################################################################
# Test_Mock1_System_CheckConfig
# #############################################################################


class Test_Mock1_System_CheckConfig(dtfsys.System_CheckConfig_TestCase1):
    def test_freeze_config1(self) -> None:
        system_builder_func = _get_test_system_builder_func()
        system_builder = system_builder_func()
        self._test_freeze_config1(system_builder)


# #############################################################################
# Test_Mock1_NonTime_ForecastSystem_FitPredict
# #############################################################################


class Test_Mock1_NonTime_ForecastSystem_FitPredict(
    dtfsys.NonTime_ForecastSystem_FitPredict_TestCase1
):
    @staticmethod
    def get_system() -> dtfsys.System:
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
        output_col_name = "prediction"
        self._test_fit_over_backtest_period1(system, output_col_name)

    def test_fit_over_period1(self) -> None:
        system = self.get_system()
        start_timestamp = pd.Timestamp("2000-01-01 00:00:00+0000", tz="UTC")
        end_timestamp = pd.Timestamp("2000-01-31 00:00:00+0000", tz="UTC")
        output_col_name = "prediction"
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
# Test_Mock1_NonTime_ForecastSystem_FitInvariance
# #############################################################################


class Test_Mock1_NonTime_ForecastSystem_FitInvariance(
    dtfsys.NonTime_ForecastSystem_FitInvariance_TestCase1
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
# Test_Mock1_NonTime_ForecastSystem_CheckPnl
# #############################################################################


class Test_Mock1_NonTime_ForecastSystem_CheckPnl(
    dtfsys.NonTime_ForecastSystem_CheckPnl_TestCase1
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


class Test_Mock1_Time_ForecastSystem1(dtfsys.Test_Time_ForecastSystem_TestCase1):
    def test1(self) -> None:
        """
        Verify the contents of DAG prediction.
        """
        system = dtfasmmfosy.Mock1_Time_ForecastSystem()
        (
            market_data,
            rt_timeout_in_secs_or_time,
        ) = cofinanc.get_MarketData_df4()
        # Since we are reading from a df there is no delay.
        system.config["market_data_config", "delay_in_secs"] = 0
        system.config["market_data_config", "data"] = market_data
        # We need at least 7 bars to compute volatility.
        system.config[
            "market_data_config", "replayed_delay_in_mins_or_timestamp"
        ] = pd.Timestamp("2000-01-01 10:05:00-05:00", tz="America/New_York")
        # Exercise the system for multiple 5 minute intervals.
        system.config[
            "dag_runner_config", "rt_timeout_in_secs_or_time"
        ] = rt_timeout_in_secs_or_time
        system.config["dag_runner_config", "bar_duration_in_secs"] = 60 * 5
        #
        output_col_name = "prediction"
        self._test1(
            system,
            output_col_name=output_col_name,
        )


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DataFramePortfolio1
# #############################################################################


class Test_Mock1_Time_ForecastSystem_with_DataFramePortfolio1(
    dtfsys.Time_ForecastSystem_with_DataFramePortfolio_TestCase1
):
    """
    See description in the parent class.
    """

    # These 3 tests correspond to the same tests for:
    # - DataFramePortfolio
    # - DatabasePortfolio
    # - DataFrame vs Database

    @pytest.mark.slow("~7 seconds.")
    def test1(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df1()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio.
        expected_last_timestamp = pd.Timestamp("2000-01-01 10:05:06-05:00")
        dtfsys.check_portfolio_state(self, system, expected_last_timestamp)

    @pytest.mark.slow("~7 seconds.")
    def test2(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df2()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio.
        expected_last_timestamp = pd.Timestamp("2000-01-01 10:05:06-05:00")
        dtfsys.check_portfolio_state(self, system, expected_last_timestamp)

    @pytest.mark.slow("~7 seconds.")
    def test3(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df3()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio.
        expected_last_timestamp = pd.Timestamp("2000-01-01 11:25:06-05:00")
        dtfsys.check_portfolio_state(self, system, expected_last_timestamp)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DataFramePortfolio2
# #############################################################################


class Test_Mock1_Time_ForecastSystem_with_DataFramePortfolio2(
    dtfsys.Time_ForecastSystem_with_DataFramePortfolio_TestCase1
):
    """
    Test liquidating the portfolio at the end of the day.
    """

    @pytest.mark.slow("~7 seconds.")
    def test_with_liquidate_at_end_of_day1(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df1()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test_with_liquidate_at_end_of_day1(system)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1
# #############################################################################


class Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1(
    dtfsys.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1
):
    """
    Test a Mock1 system with DatabasePortfolio.

    See description in the parent class.
    """

    # These 3 tests correspond to the same tests for:
    # - DataFramePortfolio
    # - DatabasePortfolio
    # - DataFrame vs Database

    @pytest.mark.slow("~6 seconds.")
    def test1(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df1()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio.
        expected_last_timestamp = pd.Timestamp("2000-01-01 10:05:06-05:00")
        dtfsys.check_portfolio_state(self, system, expected_last_timestamp)

    @pytest.mark.slow("~6 seconds.")
    def test2(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df2()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio.
        expected_last_timestamp = pd.Timestamp("2000-01-01 10:05:06-05:00")
        dtfsys.check_portfolio_state(self, system, expected_last_timestamp)

    @pytest.mark.superslow("~30 seconds.")
    def test3(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df3()
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio.
        expected_last_timestamp = pd.Timestamp("2000-01-01 11:25:06-05:00")
        dtfsys.check_portfolio_state(self, system, expected_last_timestamp)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor2
# #############################################################################


class Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor2(
    dtfsys.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1
):
    """
    Test a Mock1 system with DatabasePortfolio to verify the stopping
    condition.
    """

    @pytest.mark.slow("~6 seconds.")
    def test1(self) -> None:
        # Build the system.
        data, _ = cofinanc.get_MarketData_df1()
        _LOG.debug("data=\n%s", hpandas.df_to_str(data))
        self.assert_equal(str(data.index.min()), "2000-01-01 09:31:00-05:00")
        self.assert_equal(str(data.index.max()), "2000-01-01 10:10:00-05:00")
        # One bar is 5 mins and we want to run for 5 bars.
        rt_timeout_in_secs_or_time = 300 * 5
        system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Test some properties of the system.
        order_processor = system.order_processor
        _LOG.debug(hprint.to_str("order_processor.get_execution_signature()"))
        self.assert_equal(
            str(order_processor.start_timestamp), "2000-01-01 09:35:00-05:00"
        )
        self.assert_equal(
            str(order_processor.end_timestamp), "2000-01-01 10:00:06-05:00"
        )
        self.assertEqual(order_processor.num_filled_orders, 5)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio1
# #############################################################################


class Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio1(
    dtfsys.Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio_TestCase1
):
    """
    Run a Mock1 system with DatabasePortfolio and DataFramePortfolio and verify
    that the output is the same.

    See description in the parent class.
    """

    def run_test(
        self,
        data: pd.DataFrame,
        rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]],
    ) -> Tuple[dtfsys.System, dtfsys.System]:
        """
        Run two systems with DataFrame and Database portfolios and compare.
        """
        # Build the systems to compare.
        system_with_dataframe_portfolio = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
            data, rt_timeout_in_secs_or_time
        )
        system_with_database_portfolio = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(
            system_with_dataframe_portfolio, system_with_database_portfolio
        )
        return system_with_dataframe_portfolio, system_with_database_portfolio

    # These 3 tests correspond to the same tests for:
    # - DataFramePortfolio
    # - DatabasePortfolio,
    # - DataFrame vs Database

    @pytest.mark.slow("~10 seconds.")
    def test1(self) -> None:
        """
        Run with a `cofinanc.get_MarketData_df1()`.
        """
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df1()
        self.run_test(data, rt_timeout_in_secs_or_time)

    @pytest.mark.slow("~10 seconds.")
    def test2(self) -> None:
        """
        Run with a `cofinanc.get_MarketData_df2()`.
        """
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df2()
        self.run_test(data, rt_timeout_in_secs_or_time)

    @pytest.mark.superslow("~30 seconds.")
    def test3(self) -> None:
        """
        Run with a `cofinanc.get_MarketData_df3()`.
        """
        data, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df3()
        self.run_test(data, rt_timeout_in_secs_or_time)


# #############################################################################
# Test_Mock1_NonTime_ForecastSystem_vs_Time_ForecastSystem1
# #############################################################################


class Test_Mock1_NonTime_ForecastSystem_vs_Time_ForecastSystem1(
    dtfsys.NonTime_ForecastSystem_vs_Time_ForecastSystem_TestCase1
):
    """
    See the parent class for description.
    """

    def get_NonTime_ForecastSystem_builder_func(self) -> Callable:
        """
        Get the function building the (non-time) `ForecastSystem`.
        """
        # TODO(Grisha): @Dan Write a function that extracts the latest available universe.
        universe_version = "v1"
        # In the current system, the time periods are set manually,
        # so the value of `time_interval_str` (e.g., "2022-01-01_2022-02-01")
        # doesn't affect tests.
        backtest_config = f"mock1_{universe_version}-all.5T.Jan2000"
        non_time_system_builder_func = (
            lambda: dtfasmmfsex.get_Mock1_NonTime_ForecastSystem_example1(
                backtest_config
            )
        )
        return non_time_system_builder_func

    # TODO(Grisha): @Dan Factor out to `system_test_case.py`.
    def get_NonTime_ForecastSystem_from_Time_ForecastSystem(
        self, time_system: dtfsys.System
    ) -> dtfsys.System:
        """
        See description in the parent test case class.
        """
        # Get wall clock time and history lookback from `Time_Forecast_System`
        # to pass them to `Forecast_System` so that the values are in sync.
        wall_clock_time = time_system.market_data.get_wall_clock_time()
        history_lookback = time_system.config[
            "market_data_config", "history_lookback"
        ]
        # Since time forecast system is run for multiple 5 min intervals,
        # we need to compute the number of minutes the system will go beyond
        # its wall clock time.
        # In this case the wall clock time is `2000-01-01 09:55:00-05:00`,
        # so the system goes 1 min forward from the wall clock time until
        # the first 5 min interval and then goes by the number of the remaining
        # 5 min cycles. This will be the end time for `ForecastSystem`.
        rt_timeout_in_secs_or_time = time_system.config[
            "dag_runner_config", "rt_timeout_in_secs_or_time"
        ]
        n_5min_intervals = rt_timeout_in_secs_or_time / 60 / 5
        intervals_delay_in_mins = 1 + (n_5min_intervals - 1) * 5
        end_timestamp = wall_clock_time + pd.Timedelta(
            intervals_delay_in_mins, "minutes"
        )
        # Get start time for `ForecastSystem` using history lookback
        # and by adding 1 min to exclude the end of 5 min interval.
        start_timestamp = end_timestamp - history_lookback + pd.Timedelta("1T")
        non_time_system_builder_func = (
            self.get_NonTime_ForecastSystem_builder_func()
        )
        non_time_system = non_time_system_builder_func()
        non_time_system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ] = start_timestamp
        non_time_system.config["backtest_config", "end_timestamp"] = end_timestamp
        return non_time_system

    def get_Time_ForecastSystem(self) -> dtfsys.System:
        """
        See description in the parent test case class.
        """
        # Load market data for replaying.
        market_data_df, rt_timeout_in_secs_or_time = cofinanc.get_MarketData_df5()
        time_system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_example1()
        # TODO(Grisha): @Dan Pass "rt_timeout_in_secs_or_time" through kwargs.
        # Make system to run for 3 5-minute intervals.
        time_system.config[
            "dag_runner_config", "rt_timeout_in_secs_or_time"
        ] = rt_timeout_in_secs_or_time
        time_system.config["market_data_config", "data"] = market_data_df
        return time_system

    @pytest.mark.slow("~8 seconds.")
    def test1(self) -> None:
        output_col_name = "prediction"
        self._test1(output_col_name)