import asyncio
import logging

import pandas as pd
import pytest

import core.config as cconfig
import core.finance as cofinanc
import dataflow.system.example_pipeline1_system_runner as dtfsepsyru
import dataflow.pipelines.examples.example1_pipeline as dtfpexexpi
import dataflow.system.source_nodes as dtfsysonod
import dataflow.system.system_tester as dtfsysytes
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import helpers.hasyncio as hasynci
import oms.test.oms_db_helper as otodh
import unittest

_LOG = logging.getLogger(__name__)


class Test_Example1_ForecastSystem(unittest.TestCase):
    """
    Test a System composed of:
    - a `ReplayedMarketData` (providing fake data and features)
    - an `Example1` DAG
    """

    def run_coroutines(
        self,
        data: pd.DataFrame,
        real_time_loop_time_out_in_secs: int,
    ) -> str:
        """
        Run a system using the desired portfolio based on DB or dataframe.
        """
        with hasynci.solipsism_context() as event_loop:
            asset_ids = [101]
            # TODO(gp): -> system
            system_runner = dtfsepsyru.Example1_ForecastSystem(
                asset_ids, event_loop,
            )
            #
            market_data = system_runner.get_market_data(data)
            #
            config = cconfig.Config()
            # Save the `DagBuilder` and the `DagConfig` in the config.
            dag_builder = dtfpexexpi.Example1_DagBuilder()
            dag_config = dag_builder.get_config_template()
            config["DAG"] = dag_config
            config["meta", "dag_builder"] = dag_builder

            # Create HistoricalDataSource.
            stage = "read_data"
            asset_id_col = "asset_id"
            # TODO(gp): This in the original code was
            #  `ts_col_name = "timestamp_db"`.
            #ts_col_name = "end_ts"
            multiindex_output = True
            # How much history is needed for the DAG to compute.
            timedelta = pd.Timedelta("5M")
            # col_names_to_remove = ["start_datetime", "timestamp_db"]
            #col_names_to_remove = ["start_ts"]
            get_wall_clock_time = market_data.get_wall_clock_time
            node = dtfsysonod.RealTimeDataSource(
                stage,
                market_data,
                timedelta,
                asset_id_col,
                #ts_col_name,
                multiindex_output,
                #col_names_to_remove=col_names_to_remove,
            )
            # Build the DAG.
            dag_builder = config["meta", "dag_builder"]
            dag = dag_builder.get_dag(config["DAG"])
            # This is for debugging. It saves the output of each node in a `csv` file.
            # dag.set_debug_mode("df_as_csv", False, "crypto_forever")
            if False:
                dag.force_freeing_nodes = True
            # Add the data source node.
            dag.insert_at_head(stage, node)

            #
            # #portfolio = system_runner.get_portfolio(market_data)
            # #
            # returns_col = "vwap.ret_0"
            # volatility_col = "vwap.ret_0.vol"
            # prediction_col = "feature1"
            # config, dag_builder = system_runner.get_dag(
            #     prediction_col=prediction_col,
            #     volatility_col=volatility_col,
            #     returns_col=returns_col,
            # )
            # #
            # get_wall_clock_time = market_data.get_wall_clock_time
            # dag_runner = system_runner.get_dag_runner(
            #     dag_builder,
            #     config,
            #     get_wall_clock_time,
            #     real_time_loop_time_out_in_secs=real_time_loop_time_out_in_secs,
            # )

            # This represents what's the increment of the simulated wall clock.
            # TODO(gp): This is the bar alignment. Horrible name and also it should be
            # a property of the DAG instead of the DagRunner.
            #sleep_interval_in_secs = 60
            sleep_interval_in_secs = 5 * 60

            # Set up the event loop.
            execute_rt_loop_kwargs = {
                "get_wall_clock_time": get_wall_clock_time,
                "sleep_interval_in_secs": sleep_interval_in_secs,
                "time_out_in_secs": real_time_loop_time_out_in_secs,
            }
            dag_runner_kwargs = {
                "config": config,
                "dag_builder": dag,
                "fit_state": None,
                "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
                "dst_dir": None,
            }
            dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)

            coroutines = [dag_runner.predict()]
            #
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
        return str

    # ///////////////////////////////////////////////////////////////////////////

    def test1(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df1()
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs,
        )
        #self.check_string(actual)


# ######################################################


# TODO(gp): This should derive from SystemTester.
# TODO(gp): Rename -> Test_Example1_SimulatedOmsSystem
class Test_Example1_SystemRunner(otodh.TestOmsDbHelper):
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
                system_runner = dtfsepsyru.Example1_Database_SystemRunner(
                    asset_ids, event_loop, db_connection=self.connection
                )
            else:
                system_runner = dtfsepsyru.Example1_Dataframe_SystemRunner(
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
    @pytest.mark.skip("AmpTask2200 Enable after updating Pandas")
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
    @pytest.mark.skip("AmpTask2200 Enable after updating Pandas")
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
    @pytest.mark.skip("AmpTask2200 Enable after updating Pandas")
    def test_market_data3_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = cofinanc.get_market_data_df3()
        expected = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self.run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)
