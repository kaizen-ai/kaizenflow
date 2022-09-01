import datetime
import logging
from typing import Callable, Optional, Tuple, Union

import pandas as pd
import pytest

import core.config as cconfig
import core.finance as cofinanc
import dataflow.system as dtfsys
import dataflow.system.test.system_test_case as dtfsytsytc
import dataflow_amp.system.mock1.mock1_forecast_system as dtfasmmfosy
import dataflow_amp.system.mock1.mock1_forecast_system_example as dtfasmmfsex
import im_v2.ccxt.data.client as icdcl


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
        (
            market_data,
            rt_timeout_in_secs_or_time,
        ) = cofinanc.get_market_data_df4()
        # Since we are reading from a df there is no delay.
        system.config["market_data_config", "delay_in_secs"] = 0
        system.config["market_data_config", "data"] = market_data
        # We need at least 7 bars to compute volatility.
        system.config[
            "market_data_config", "replayed_delay_in_mins_or_timestamp"
        ] = 35
        # Exercise the system for multiple 5 minute intervals.
        system.config[
            "dag_runner_config", "rt_timeout_in_secs_or_time"
        ] = rt_timeout_in_secs_or_time
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
    rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]],
) -> dtfsys.System:
    """
    Get a System object with a DataFramePortfolio for unit testing.
    """
    system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DataFramePortfolio_example1(
        market_data_df, rt_timeout_in_secs_or_time
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
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df1()
        system = _get_test_System_with_DataFramePortfolio(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)
        # Check some high level property of the Portfolio:
        expected_last_timestamp = pd.Timestamp("2000-01-01 10:05:06-05:00")
        dtfsytsytc.check_portfolio_state(self, system, expected_last_timestamp)

    @pytest.mark.slow("~7 seconds.")
    def test_with_liquidate_at_end_of_day1(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df1()
        system = _get_test_System_with_DataFramePortfolio(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test_with_liquidate_at_end_of_day1(system)


# #############################################################################
# Test_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1
# #############################################################################


def _get_test_System_with_DatabasePortfolio(
    market_data_df: pd.DataFrame,
    rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]],
) -> dtfsys.System:
    """
    Get a System object with a DatabasePortfolio for unit testing.
    """
    system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_example1(
        market_data_df, rt_timeout_in_secs_or_time
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
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df1()
        system = _get_test_System_with_DatabasePortfolio(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)

    @pytest.mark.slow("~6 seconds.")
    def test_market_data2_database_portfolio(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df2()
        system = _get_test_System_with_DatabasePortfolio(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(system)

    @pytest.mark.slow("~15 seconds.")
    def test_market_data3_database_portfolio(self) -> None:
        # Build the system.
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df3()
        system = _get_test_System_with_DatabasePortfolio(
            data, rt_timeout_in_secs_or_time
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

    def run_test(
        self,
        data: pd.DataFrame,
        rt_timeout_in_secs_or_time: Optional[Union[int, datetime.time]],
    ) -> Tuple[dtfsys.System, dtfsys.System]:
        # Build the systems to compare.
        system_with_dataframe_portfolio = (
            _get_test_System_with_DataFramePortfolio(
                data, rt_timeout_in_secs_or_time
            )
        )
        system_with_database_portfolio = _get_test_System_with_DatabasePortfolio(
            data, rt_timeout_in_secs_or_time
        )
        # Run.
        self._test1(
            system_with_dataframe_portfolio, system_with_database_portfolio
        )
        return system_with_dataframe_portfolio, system_with_database_portfolio

    @pytest.mark.slow("~10 seconds.")
    def test1(self) -> None:
        """
        Run with a `cofinanc.get_market_data_df1()`.
        """
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df1()
        self.run_test(data, rt_timeout_in_secs_or_time)

    @pytest.mark.slow("~10 seconds.")
    def test2(self) -> None:
        """
        Run with a `cofinanc.get_market_data_df2()`.
        """
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df2()
        self.run_test(data, rt_timeout_in_secs_or_time)

    @pytest.mark.superslow("~30 seconds.")
    def test3(self) -> None:
        """
        Run with a `cofinanc.get_market_data_df3()`.
        """
        data, rt_timeout_in_secs_or_time = cofinanc.get_market_data_df3()
        self.run_test(data, rt_timeout_in_secs_or_time)


# #############################################################################
# Test_Mock1_NonTime_ForecastSystem_vs_Time_ForecastSystem1
# #############################################################################


class Test_Mock1_NonTime_ForecastSystem_vs_Time_ForecastSystem1(
    dtfsytsytc.NonTime_ForecastSystem_vs_Time_ForecastSystem_TestCase1
):
    """
    See the parent class for description.
    """

    # TODO(Grisha): @Dan Add a test for 1 asset id.
    @pytest.mark.skip("Run manually")
    def test_save_data(self) -> None:
        # pylint: disable=line-too-long
        """
        Dump data from a `MarketData` to feed both non-time and time systems
        to `ReplayedMarketData`.
        ```
          index                    end_ts   asset_id       full_symbol      open       high        low     close   volume              knowledge_timestamp                  start_ts
        0     0 2021-12-19 19:00:00-05:00 1182743717 binance::BTC_BUSD 46681.500 46687.4000 46620.0000 46678.400   47.621 2022-07-09 16:21:44.328375+00:00 2021-12-19 18:59:00-05:00
        1     1 2021-12-19 19:00:00-05:00 1464553467 binance::ETH_USDT  3922.510  3927.4500  3920.1900  3927.060 1473.723 2022-06-24 11:10:10.287766+00:00 2021-12-19 18:59:00-05:00
        2     2 2021-12-19 19:00:00-05:00 1467591036 binance::BTC_USDT 46668.650 46677.2200 46575.0000 46670.340  620.659 2022-07-09 12:07:51.240219+00:00 2021-12-19 18:59:00-05:00
        ```
        """
        # pylint: enable=line-too-long
        universe_version = None
        resample_1min = True
        dataset = "ohlcv"
        contract_type = "futures"
        data_snapshot = None
        im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        full_symbols = im_client.get_universe()
        asset_ids = im_client.get_asset_ids_from_full_symbols(full_symbols)
        columns = None
        columns_remap = None
        wall_clock_time = pd.Timestamp("2022-01-04T00:00:00+00:00")
        # We dump data from an historical market data and then we can replay the
        # data with a ReplayedMarket data.
        market_data_df = mdata.get_HistoricalImClientMarketData_example1(
            im_client,
            asset_ids,
            columns,
            columns_remap,
            wall_clock_time=wall_clock_time,
        )
        file_path = self.get_file_path()
        period = pd.Timedelta("15D")
        mdata.save_market_data(market_data_df, file_path, period)
        _LOG.warning("Updated file '%s'", file_path)

    def get_Mock1_NonTime_ForecastSystem_builder_func(self) -> Callable:
        """
        Get the function building the (non-time) `ForecastSystem`.
        """
        # TODO(Grisha): @Dan Write a function that extracts the latest available universe.
        universe_version = "v1"
        # In the current system, the time periods are set manually,
        # so the value of `time_interval_str` (e.g., "2022-01-01_2022-02-01")
        # doesn't affect tests.
        backtest_config = f"mock1_{universe_version}-all.5T.2022-01-01_2022-02-01"
        non_time_system_builder_func = (
            lambda: dtfasmmfsex.get_Mock1_ForecastSystem_for_simulation_example1(
                backtest_config
            )
        )
        return non_time_system_builder_func

    def get_Mock1_NonTime_ForecastSystem_from_Time_ForecastSystem(
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
        # In this case the wall clock time is `2021-12-26 18:59:00-05:00`,
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
        # TODO(Grisha): @Dan Use different system builder func since the used
        # one is intended to load data for 2 asset ids.
        non_time_system_builder_func = (
            self.get_NonTime_ForecastSystem_builder_func()
        )
        non_time_system = non_time_system_builder_func()
        im_client_config = {
            "universe_version": "v1",
            "resample_1min": True,
            "dataset": "ohlcv",
            "contract_type": "futures",
            "data_snapshot": None,
        }
        non_time_system.config["market_data_config", "im_client_ctor"] = im_client_config
        non_time_system.config["market_data_config", "im_client_config"] = cconfig.Config()
        non_time_system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ] = start_timestamp
        non_time_system.config["backtest_config", "end_timestamp"] = end_timestamp
        return non_time_system

    def get_Mock1_Time_ForecastSystem(self) -> dtfsys.System:
        """
        See description in the parent test case class.
        """
        file_path = self.get_file_path()
        aws_profile = "ck"
        # `get_ReplayedTimeMarketData_from_df()` is looking for "start_datetime"
        # and "end_datetime" columns by default and we do not have a way to
        # change it yet since `get_EventLoop_MarketData_from_df` has no kwargs.
        column_remap = {"start_ts": "start_datetime", "end_ts": "end_datetime"}
        timestamp_db_column = "end_datetime"
        datetime_columns = ["start_datetime", "end_datetime", "timestamp_db"]
        # Load market data for replaying.
        market_data_df = mdata.load_market_data(
            file_path,
            aws_profile=aws_profile,
            column_remap=column_remap,
            timestamp_db_column=timestamp_db_column,
            datetime_columns=datetime_columns,
        )
        # TODO(Grisha): @Dan we should use separate example systems for
        # reconciliation otherwise we should keep the current ones in
        # sync.
        time_system = dtfasmmfsex.get_Mock1_Time_ForecastSystem_example1() 
        # TODO(Grisha): @Dan consider a way to pass the number of 5-minute intervals.
        # Make system to run for 20 5-minute intervals.
        time_system.config["dag_runner_config", "rt_timeout_in_secs_or_time"] = (
            60 * 5 * 20
        )
        time_system.config["market_data_config", "data"] = market_data_df
        return time_system

    # @pytest.mark.skip("Run manually")
    @pytest.mark.superslow("~200 seconds.")
    def test1(self) -> None:
        output_col_name = "vwap.ret_0.vol_adj_2_hat"
        self._test1(output_col_name)
