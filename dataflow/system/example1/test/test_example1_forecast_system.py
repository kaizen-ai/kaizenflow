import asyncio
import logging
from typing import Callable

import pandas as pd
import pytest

import core.finance as cofinanc
import dataflow.system as dtfsys
import dataflow.system.example1.example1_forecast_system as dtfseefosy
import dataflow.system.system_tester as dtfsysytes
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
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


class Test_Example1_System_CheckConfig(dtfsysytes.System_CheckConfig_TestCase1):
    def test_freeze_config1(self) -> None:
        system_builder_func = _get_test_system_builder_func()
        system_builder = system_builder_func()
        self._test_freeze_config1(system_builder)


# #############################################################################
# Test_Example1_ForecastSystem_FitPredict
# #############################################################################


class Test_Example1_ForecastSystem_FitPredict(
    dtfsysytes.ForecastSystem_FitPredict_TestCase1
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
# Test_Example1_ForecastSystem_FitInvariance
# #############################################################################


class Test_Example1_ForecastSystem_FitInvariance(
    dtfsysytes.ForecastSystem_FitInvariance_TestCase1
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
    dtfsysytes.ForecastSystem_CheckPnl_TestCase1
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


class Test_Example1_Time_ForecastSystem1(hunitest.TestCase):
    """
    Test a System composed of:

    - a `ReplayedMarketData` (providing fake data and features)
    - an `Example1` DAG
    """

    @staticmethod
    def run_coroutines() -> str:
        with hasynci.solipsism_context() as event_loop:
            system = dtfseefosy.Example1_Time_ForecastSystem()
            # Complete system config.
            system.config["event_loop_object"] = event_loop
            data, _ = cofinanc.get_market_data_df1()
            system.config["market_data_config", "data"] = data
            system.config["market_data_config", "initial_replayed_delay"] = 5
            system.config[
                "dag_runner_config", "real_time_loop_time_out_in_secs"
            ] = (60 * 5)
            # Create DAG runner.
            dag_runner = system.get_dag_runner()
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            # TODO(gp): Use the signature from system_testing. See below.
            result_bundles: str = result_bundles[0][0]
        return result_bundles

    def test1(self) -> None:
        """
        Verify the contents of DAG prediction.
        """
        actual = self.run_coroutines()
        self.check_string(str(actual), purify_text=True)


# #############################################################################
# Test_Example1_Time_ForecastSystem_with_DataFramePortfolio1
# #############################################################################

# TODO(gp): This should derive from SystemTester.
class Test_Example1_Time_ForecastSystem_with_DataFramePortfolio1(
    hunitest.TestCase
):
    """
    Test an end-to-end `System`, containing:

    - a `MarketData` using fake data and features
    - a Example1 pipeline
    - a `Portfolio` backed by a dataframe
    """

    # TODO(gp): This was copied.
    def run_coroutines(
        self,
        data: pd.DataFrame,
        real_time_loop_time_out_in_secs: int,
    ) -> str:
        """
        Run a system using the desired portfolio based on DB or dataframe.
        """
        with hasynci.solipsism_context() as event_loop:
            system = (
                dtfseefosy.Example1_Time_ForecastSystem_with_DataFramePortfolio()
            )
            # Complete system config.
            system.config["event_loop_object"] = event_loop
            system.config["market_data_config", "data"] = data
            system.config["market_data_config", "initial_replayed_delay"] = 5
            system.config["market_data_config", "asset_ids"] = [101]
            system.config["dag_runner_config", "sleep_interval_in_secs"] = 60 * 5
            system.config[
                "dag_runner_config", "real_time_loop_time_out_in_secs"
            ] = real_time_loop_time_out_in_secs
            # Create DAG runner.
            dag_runner = system.get_dag_runner()
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            # Compute output.
            # TODO(gp): Factor this out to SystemTester.
            system_tester = dtfsysytes.SystemTester()
            result_bundles = result_bundles[0]
            result_bundle = result_bundles[-1]
            _LOG.debug("result_bundle=\n%s", result_bundle)
            # TODO(gp): Extract all of this from System.
            portfolio = system.portfolio
            _LOG.debug("portfolio=\n%s", portfolio)
            price_col = "vwap"
            volatility_col = "vwap.ret_0.vol"
            prediction_col = "feature1"
            actual: str = system_tester.compute_run_signature(
                dag_runner,
                portfolio,
                result_bundle,
                price_col=price_col,
                volatility_col=volatility_col,
                prediction_col=prediction_col,
            )
        return actual

    def test_market_data1_dataframe_portfolio1(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        actual = self.run_coroutines(
            data,
            real_time_loop_time_out_in_secs,
        )
        self.check_string(str(actual))


# #############################################################################
# Test_Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor
# #############################################################################


# TODO(gp): This should derive from SystemTester.
class Test_Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor1(
    otodh.TestOmsDbHelper
):
    """
    Test an end-to-end `System`, containing:

    - Example1 pipeline
    - with a `MarketData` using fake data and features
    - with a `Portfolio` backed by DB or dataframe
    """

    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def run_coroutines(
        self,
        data: pd.DataFrame,
        real_time_loop_time_out_in_secs: int,
        is_database_portfolio: bool,
    ) -> str:
        """
        Run a system using the desired portfolio based on DB or dataframe.
        """
        # TODO(gp): This might come from market_data.asset_id_col
        asset_id_name = "asset_id"
        incremental = False
        oms.create_oms_tables(self.connection, incremental, asset_id_name)
        #
        with hasynci.solipsism_context() as event_loop:
            coroutines = []
            #
            if is_database_portfolio:
                system = (
                    dtfseefosy.Example1_Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor()
                )
            else:
                system = (
                    dtfseefosy.Example1_Time_ForecastSystem_with_DataFramePortfolio()
                )
            # Complete system config.
            system.config["event_loop_object"] = event_loop
            system.config["db_connection_object"] = self.connection
            system.config["market_data_config", "data"] = data
            system.config["market_data_config", "initial_replayed_delay"] = 5
            system.config["market_data_config", "asset_ids"] = [101]
            # TODO(gp): This needs to go to the config.
            system.config["dag_runner_config", "sleep_interval_in_secs"] = 60 * 15
            system.config[
                "dag_runner_config", "real_time_loop_time_out_in_secs"
            ] = real_time_loop_time_out_in_secs
            # Create DAG runner.
            dag_runner = system.get_dag_runner()
            coroutines.append(dag_runner.predict())
            # Create and add order processor.
            portfolio = system.portfolio
            if is_database_portfolio:
                order_processor = oms.get_order_processor_example1(
                    self.connection, portfolio, asset_id_name
                )
                order_processor_coroutine = (
                    oms.get_order_processor_coroutine_example1(
                        order_processor,
                        portfolio,
                        real_time_loop_time_out_in_secs,
                    )
                )
                coroutines.append(order_processor_coroutine)
            #
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            # Compute output.
            system_tester = dtfsysytes.SystemTester()
            result_bundles = result_bundles[0]
            result_bundle = result_bundles[-1]
            _LOG.debug("result_bundle=\n%s", result_bundle)
            # TODO(gp): Extract all of this from System.
            portfolio = system.portfolio
            _LOG.debug("portfolio=\n%s", portfolio)
            price_col = "vwap"
            volatility_col = "vwap.ret_0.vol"
            prediction_col = "feature1"
            actual: str = system_tester.compute_run_signature(
                dag_runner,
                portfolio,
                result_bundle,
                price_col=price_col,
                volatility_col=volatility_col,
                prediction_col=prediction_col,
            )
            return actual

    # ///////////////////////////////////////////////////////////////////////////

    def test_market_data1_dataframe_portfolio(self) -> None:
        """
        Test a dataframe-based Portfolio against the expected behavior.
        """
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.slow
    def test_market_data1_database_portfolio(self) -> None:
        """
        Test a database-based Portfolio against the expected behavior.
        """
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.slow
    def test_market_data2_database_portfolio(self) -> None:
        """
        Test a database-based Portfolio against the expected behavior.
        """
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df2()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.slow
    def test_market_data3_database_portfolio(self) -> None:
        """
        Test a database-based Portfolio against the expected behavior.
        """
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.slow
    def test_market_data1_database_vs_dataframe_portfolio(self) -> None:
        """
        Compare the output between using a DB and dataframe portfolio.
        """
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        expected = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    def test_market_data2_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df2()
        expected = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.superslow("Times out in GH Actions.")
    def test_market_data3_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        expected = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected, fuzzy_match=True)
