import asyncio
import logging

import pandas as pd
import pytest

import core.finance as cofinanc
import dataflow.system.example_pipeline1_system_runner as dtfsepsyru
import dataflow.system.system_tester as dtfsysytes
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)


class Test_Example1_ForecastSystem(hunitest.TestCase):
    """
    Test a System composed of:

    - a `ReplayedMarketData` (providing fake data and features)
    - an `Example1` DAG
    """

    def run_coroutines(
        self,
        data: pd.DataFrame,
    ) -> str:
        """
        Run a system using the desired portfolio based on DB or dataframe.
        """
        with hasynci.solipsism_context() as event_loop:
            asset_ids = [101]
            system = dtfsepsyru.Example1_ForecastSystem(
                asset_ids,
                event_loop,
            )
            config = system.get_dag_config()
            market_data = system.get_market_data(data)
            dag_runner = system.get_dag_runner(
                config,
                market_data,
                real_time_loop_time_out_in_secs=60 * 5,
            )
            coroutines = [dag_runner.predict()]
            #
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            result_bundles = result_bundles[0][0]
        return result_bundles

    # ///////////////////////////////////////////////////////////////////////////

    def test1(self) -> None:
        """
        Verify the contents of DAG prediction.
        """
        data, _ = cofinanc.get_market_data_df1()
        actual = self.run_coroutines(
            data,
        )
        self.check_string(str(actual))


# #############################################################################


# TODO(gp): This should derive from SystemTester.
class Test_Example1_SimulatedOmsSystem(otodh.TestOmsDbHelper):
    """
    Test using fake data and features:

    - `Example1` pipeline
    - end-to-end inside a `System`
    - with a `MarketData`
    - with a `Portfolio` backed by DB or dataframe
    """

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
            asset_ids = [101]
            # TODO(gp): Can we derive `System` from the class?
            if is_database_portfolio:
                system_runner = dtfsepsyru.Example1_Database_ForecastSystem(
                    asset_ids, event_loop, db_connection=self.connection
                )
            else:
                system_runner = dtfsepsyru.Example1_Dataframe_ForecastSystem(
                    asset_ids, event_loop
                )
            #
            market_data = system_runner.get_market_data(data)
            #
            portfolio = system_runner.get_portfolio(market_data)
            #
            returns_col = "vwap.ret_0"
            volatility_col = "vwap.ret_0.vol"
            prediction_col = "feature1"
            config, dag_builder = system_runner.get_dag(
                portfolio,
                prediction_col=prediction_col,
                volatility_col=volatility_col,
                returns_col=returns_col,
            )
            #
            get_wall_clock_time = market_data.get_wall_clock_time
            dag_runner = system_runner.get_dag_runner(
                dag_builder,
                config,
                get_wall_clock_time,
                real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
            )
            coroutines = [dag_runner.predict()]
            #
            if is_database_portfolio:
                order_processor_coroutine = (
                    system_runner.get_order_processor_coroutine(
                        portfolio,
                        real_time_loop_time_out_in_secs,
                    )
                )
                coroutines.append(order_processor_coroutine)
            #
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            system_tester = dtfsysytes.SystemTester()
            result_bundles = result_bundles[0]
            actual = system_tester.compute_run_signature(
                dag_runner,
                portfolio,
                result_bundles[-1],
                returns_col=returns_col,
                volatility_col=volatility_col,
                prediction_col=prediction_col,
            )
            return actual

    # ///////////////////////////////////////////////////////////////////////////

    @pytest.mark.slow
    def test_market_data1_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data2_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df2()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data3_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        self.check_string(actual)

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
        self.assert_equal(actual, expected)

    @pytest.mark.slow
    def test_market_data2_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df2()
        expected = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)

    @pytest.mark.superslow("Times out in GH Actions.")
    def test_market_data3_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        expected = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)
