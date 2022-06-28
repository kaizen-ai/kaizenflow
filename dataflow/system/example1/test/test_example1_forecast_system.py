import asyncio
import logging

import pandas as pd
import pytest

import core.finance as cofinanc
import dataflow.system.example1.example1_forecast_system as dtfseefosy
import dataflow.system.system_tester as dtfsysytes
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import oms as oms
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)

# TODO(gp): -> test_example1_system.py

# #############################################################################
# Test_Example1_ReplayedForecastSystem
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
            system.config["event_loop"] = event_loop
            data, _ = cofinanc.get_market_data_df1()
            system.config["market_data", "data"] = data
            system.config["market_data", "initial_replayed_delay"] = 5
            system.config["dag_runner", "real_time_loop_time_out_in_secs"] = (
                60 * 5
            )
            # Create DAG runner.
            dag_runner = system.get_dag_runner()
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            # TODO(gp): Use the signature from system_testing. See below.
            result_bundles = result_bundles[0][0]
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
            system.config["event_loop"] = event_loop
            system.config["market_data", "data"] = data
            system.config["market_data", "initial_replayed_delay"] = 5
            system.config["market_data", "asset_ids"] = [101]
            system.config["dag_runner", "sleep_interval_in_secs"] = 60 * 5
            system.config[
                "dag_runner", "real_time_loop_time_out_in_secs"
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
            actual = system_tester.compute_run_signature(
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


# TODO(gp): This should derive from SystemTester.
class Test_Example1_Time_ForecastSystem_with_DatabasePortfolio1(
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
        with hasynci.solipsism_context() as event_loop:
            coroutines = []
            #
            if is_database_portfolio:
                system = dtfseefosy.Example1_Time_ForecastSystem_with_DatabasePortfolio(
                    db_connection=self.connection
                )
            else:
                system = (
                    dtfseefosy.Example1_Time_ForecastSystem_with_DataFramePortfolio()
                )
            # Complete system config.
            system.config["event_loop"] = event_loop
            system.config["market_data", "data"] = data
            system.config["market_data", "initial_replayed_delay"] = 5
            system.config["market_data", "asset_ids"] = [101]
            system.config["dag_runner", "sleep_interval_in_secs"] = 60 * 5
            system.config[
                "dag_runner", "real_time_loop_time_out_in_secs"
            ] = real_time_loop_time_out_in_secs
            # Create DAG runner.
            dag_runner = system.get_dag_runner()
            coroutines.append(dag_runner.predict())
            # Create and add order processor.
            portfolio = system.portfolio
            if is_database_portfolio:
                timeout_in_secs = 60 * (5 + 15)
                order_processor = oms.get_order_processor_example1(
                    self.connection, portfolio, timeout_in_secs
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
            actual = system_tester.compute_run_signature(
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
