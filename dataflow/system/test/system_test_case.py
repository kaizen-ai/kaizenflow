"""
Import as:

import dataflow.system.test.system_test_case as dtfsytsytc
"""

import abc
import asyncio
import datetime
import logging
import os
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple, Union

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.model as dtfmod
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_builder_utils as dtfssybuut
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hunit_test as hunitest
import oms as oms
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)


# #############################################################################
# Utils
# #############################################################################


# TODO(gp): What is the difference with _get_signature_from_result_bundle?
#  Can we unify?
def get_signature(
    system_config: cconfig.Config, result_bundle: dtfcore.ResultBundle, col: str
) -> str:
    """
    Compute the signature of a test in terms of:
    
    - system signature
    - result bundle signature
    """
    txt: List[str] = []
    #
    txt.append(hprint.frame("system_config"))
    txt.append(str(system_config))
    #
    txt.append(hprint.frame(col))
    result_df = result_bundle.result_df
    data = result_df[col].dropna(how="all").round(3)
    data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
    txt.append(data_str)
    #
    res = "\n".join(txt)
    return res


def _get_signature_from_result_bundle(
    system: dtfsyssyst.System,
    result_bundles: List[dtfcore.ResultBundle],
    add_system_config: bool,
    add_run_signature: bool,
) -> str:
    """
    Compute the signature of a test in terms of:

    - system signature
    - run signature
    - output dir signature
    """
    portfolio = system.portfolio
    dag_runner = system.dag_runner
    txt = []
    # 1) Compute system signature.
    hdbg.dassert(system.is_fully_built)
    if add_system_config:
        # TODO(gp): Use check_system_config.
        txt.append(hprint.frame("system_config"))
        txt.append(str(system.config))
    # 2) Compute run signature.
    if add_run_signature:
        # TODO(gp): This should be factored out.
        txt.append(hprint.frame("compute_run_signature"))
        hdbg.dassert_isinstance(result_bundles, list)
        result_bundle = result_bundles[-1]
        # result_bundle.result_df = result_bundle.result_df.tail(40)
        system_tester = SystemTester()
        # Check output.
        forecast_evaluator_from_prices_dict = system.config[
            "research_forecast_evaluator_from_prices"
        ].to_dict()
        txt_tmp = system_tester.compute_run_signature(
            dag_runner,
            portfolio,
            result_bundle,
            forecast_evaluator_from_prices_dict,
        )
        txt.append(txt_tmp)
    # 3) Compute the signature of the output dir.
    txt.append(hprint.frame("system_log_dir signature"))
    log_dir = system.config["system_log_dir"]
    txt_tmp = hunitest.get_dir_signature(
        log_dir, include_file_content=False, remove_dir_name=True
    )
    txt.append(txt_tmp)
    #
    actual = "\n".join(txt)
    # Remove the following line:
    # ```
    # db_connection_object: <connection object; dsn: 'user=aljsdalsd
    #   password=xxx dbname=oms_postgres_db_local
    #   host=cf-spm-dev4 port=12056', closed: 0>
    # ```
    actual = hunitest.filter_text("db_connection_object", actual)
    actual = hunitest.filter_text("log_dir:", actual)
    actual = hunitest.filter_text("trade_date:", actual)
    return actual


def run_ForecastSystem_dag_from_backtest_config(
    self: Any, system: dtfsyssyst.System, method: str
) -> dtfcore.ResultBundle:
    """
    Run `ForecastSystem` DAG with the specified fit / predict method and using
    the backtest parameters from `SystemConfig`.

    :param system: system object to extract `DagRunner` from
    :param method: "fit" or "predict"
    :return: result bundle
    """
    hdbg.dassert_in(method, ["fit", "predict"])
    dtfssybuut.apply_unit_test_log_dir(self, system)
    # Build the DAG runner.
    dag_runner = system.dag_runner
    hdbg.dassert_isinstance(dag_runner, dtfcore.DagRunner)
    # Check the system config.
    tag = "forecast_system"
    check_system_config(self, system, tag)
    # Set the time boundaries.
    start_datetime = system.config[
        "backtest_config", "start_timestamp_with_lookback"
    ]
    end_datetime = system.config["backtest_config", "end_timestamp"]
    # Run.
    if method == "fit":
        dag_runner.set_fit_intervals(
            [(start_datetime, end_datetime)],
        )
        result_bundle = dag_runner.fit()
    elif method == "predict":
        dag_runner.set_predict_intervals(
            [(start_datetime, end_datetime)],
        )
        result_bundle = dag_runner.predict()
    else:
        raise ValueError("Invalid method='%s'" % method)
    return result_bundle


# #############################################################################
# System_CheckConfig_TestCase1
# #############################################################################


class System_CheckConfig_TestCase1(hunitest.TestCase):
    """
    Check the config.
    """

    def _test_freeze_config1(self, system: dtfsyssyst.System) -> None:
        """
        Freeze config.
        """
        hdbg.dassert_isinstance(system, dtfsyssyst.System)
        dtfssybuut.apply_unit_test_log_dir(self, system)
        # Build the DAG runner.
        _ = system.dag_runner
        # TODO(gp): Use check_system_config.
        txt = []
        txt.append(hprint.frame("system_config"))
        txt.append(str(system.config))
        # Check.
        txt = "\n".join(txt)
        txt = hunitest.filter_text("trade_date:", txt)
        txt = hunitest.filter_text("log_dir:", txt)
        self.check_string(txt, purify_text=True)


# #############################################################################
# ForecastSystem1_FitPredict_TestCase1
# #############################################################################


class ForecastSystem_FitPredict_TestCase1(hunitest.TestCase):
    """
    Test fit() and predict() methods on a System.
    """

    def _test_fit_over_backtest_period1(
        self,
        system: dtfsyssyst.System,
        output_col_name: str,
    ) -> None:
        """
        - Fit a System over the backtest_config period
        - Save the signature of the system
        """
        result_bundle = run_ForecastSystem_dag_from_backtest_config(
            self, system, "fit"
        )
        # Check outcome.
        actual = get_signature(system.config, result_bundle, output_col_name)
        self.check_string(actual, fuzzy_match=True, purify_text=True)

    def _test_fit_over_period1(
        self,
        system: dtfsyssyst.System,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        output_col_name: str = "prediction",
    ) -> None:
        """
        - Fit a System over the given period
        - Save the signature of the system
        """
        dtfssybuut.apply_unit_test_log_dir(self, system)
        # Build the DAG runner.
        dag_runner = system.dag_runner
        # Set the time boundaries.
        dag_runner.set_fit_intervals(
            [(start_timestamp, end_timestamp)],
        )
        # Run.
        result_bundle = dag_runner.fit()
        # Check outcome.
        actual = get_signature(system.config, result_bundle, output_col_name)
        self.check_string(actual, fuzzy_match=True, purify_text=True)

    # TODO(Paul, gp): This should have the option to burn the last N elements
    #  of the fit/predict dataframes.
    def _test_fit_vs_predict1(
        self,
        system: dtfsyssyst.System,
    ) -> None:
        """
        Check that `predict()` matches `fit()` on the same data, when the model
        is frozen.
        """
        # Fit.
        fit_result_bundle = run_ForecastSystem_dag_from_backtest_config(
            self, system, "fit"
        )
        fit_df = fit_result_bundle.result_df
        # Predict.
        predict_result_bundle = run_ForecastSystem_dag_from_backtest_config(
            self, system, "predict"
        )
        predict_df = predict_result_bundle.result_df
        # Check.
        self.assert_dfs_close(fit_df, predict_df)


# #############################################################################
# ForecastSystem_FitInvariance_TestCase1
# #############################################################################


class ForecastSystem_FitInvariance_TestCase1(hunitest.TestCase):
    """
    Check the behavior of a System for different amount of passed data history.
    """

    def _test_invariance1(
        self,
        system_builder: Callable,
        start_timestamp1: pd.Timestamp,
        start_timestamp2: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        compare_start_timestamp: pd.Timestamp,
    ) -> None:
        """
        Test output invariance over different history lengths.
        """
        # Run dag_runner1.
        system = system_builder()
        dtfssybuut.apply_unit_test_log_dir(self, system)
        dag_runner1 = system.dag_runner
        dag_runner1.set_fit_intervals(
            [(start_timestamp1, end_timestamp)],
        )
        result_bundle1 = dag_runner1.fit()
        result_df1 = result_bundle1.result_df
        # Run dag_runner2.
        system = system_builder()
        dtfssybuut.apply_unit_test_log_dir(self, system)
        dag_runner2 = system.dag_runner
        dag_runner2.set_fit_intervals(
            [(start_timestamp2, end_timestamp)],
        )
        result_bundle2 = dag_runner2.fit()
        result_df2 = result_bundle2.result_df
        # Compare.
        result_df1 = result_df1[compare_start_timestamp:]
        result_df2 = result_df2[compare_start_timestamp:]
        self.assert_equal(str(result_df1), str(result_df2), fuzzy_match=True)


# #############################################################################
# ForecastSystem_CheckPnl_TestCase1
# #############################################################################


class ForecastSystem_CheckPnl_TestCase1(hunitest.TestCase):
    def _test_fit_run1(
        self,
        system: dtfsyssyst.System,
    ) -> None:
        result_bundle = run_ForecastSystem_dag_from_backtest_config(
            self, system, "fit"
        )
        # Check the pnl.
        system_tester = SystemTester()
        forecast_evaluator_from_prices_dict = system.config[
            "research_forecast_evaluator_from_prices"
        ].to_dict()
        signature, _ = system_tester.get_research_pnl_signature(
            result_bundle, forecast_evaluator_from_prices_dict
        )
        self.check_string(signature, fuzzy_match=True, purify_text=True)


# #############################################################################
# Test_Time_ForecastSystem_TestCase1
# #############################################################################


class Test_Time_ForecastSystem_TestCase1(hunitest.TestCase):
    """
    Test a System composed of:

    - a `ReplayedMarketData` (providing fake data and features)
    - a time DAG
    """

    def _test1(
        self,
        system: dtfsyssyst.System,
        *,
        output_col_name: str = "prediction",
    ) -> None:
        dtfssybuut.apply_unit_test_log_dir(self, system)
        with hasynci.solipsism_context() as event_loop:
            # Complete system config.
            system.config["event_loop_object"] = event_loop
            # Create DAG runner.
            dag_runner = system.dag_runner
            # 1) Check the system config.
            tag = "forecast_system"
            check_system_config(self, system, tag)
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
        # 2) Check the signature of the simulation.
        result_bundle = result_bundles[0][-1]
        actual = get_signature(system.config, result_bundle, output_col_name)
        self.check_string(actual, fuzzy_match=True, purify_text=True)


# #############################################################################
# Time_ForecastSystem_with_DataFramePortfolio_TestCase1
# #############################################################################


class Time_ForecastSystem_with_DataFramePortfolio_TestCase1(hunitest.TestCase):
    """
    Run for an extended period of time a system containing:

    - a timed DAG
    - ReplayedMarketData
    - DataFrame portfolio
    - Simulated broker

    Freeze the config and the output of the system.
    """

    # TODO(Grisha): there is some code that is common for
    #  `Time_ForecastSystem_with_DataFramePortfolio_TestCase1`
    #  and
    #  `Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1`
    #  that we should factor out.
    def _test_dataframe_portfolio_helper(
        self,
        system: dtfsyssyst.System,
        *,
        trading_end_time: Optional[datetime.time] = None,
        liquidate_at_trading_end_time: bool = False,
        add_system_config: bool = True,
        add_run_signature: bool = True,
    ) -> str:
        """
        Run a System with a DataframePortfolio.
        """
        _LOG.debug(
            hprint.to_str(
                "trading_end_time liquidate_at_trading_end_time "
                "add_system_config add_run_signature"
            )
        )
        dtfssybuut.apply_unit_test_log_dir(self, system)
        # Set `trading_end_time`.
        if trading_end_time is not None:
            system.config[
                "process_forecasts_node_dict",
                "process_forecasts_dict",
                "trading_end_time",
            ] = trading_end_time
        # Set `liquidate_at_trading_end_time`.
        system.config[
            "process_forecasts_node_dict",
            "process_forecasts_dict",
            "liquidate_at_trading_end_time",
        ] = liquidate_at_trading_end_time
        # Run the system.
        with hasynci.solipsism_context() as event_loop:
            system.config["event_loop_object"] = event_loop
            dag_runner = system.dag_runner
            # 1) Check the system config.
            # TODO(gp): Freeze the config after `dag_runner` in all the tests.
            tag = "dataframe_portfolio"
            check_system_config(self, system, tag)
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
        # 2) Check the run signature.
        # Pick the ResultBundle corresponding to the DagRunner execution.
        result_bundles = result_bundles[0]
        actual = _get_signature_from_result_bundle(
            system, result_bundles, add_system_config, add_run_signature
        )
        # 3) Check the state of the Portfolio after forced liquidation.
        if liquidate_at_trading_end_time:
            # The Portfolio should have no holdings after the end of trading.
            portfolio = system.portfolio
            has_no_holdings = portfolio.has_no_holdings()
            last_portfolio_timestamp = portfolio.get_last_timestamp()
            _LOG.debug(hprint.to_str("last_portfolio_timestamp has_no_holdings"))
            self.assertLess(trading_end_time, last_portfolio_timestamp.time())
            self.assertTrue(has_no_holdings)
        return actual

    def _test1(self, system: dtfsyssyst.System) -> None:
        """
        - Run a system:
            - Using a DataFramePortfolio
            - Freezing the output
        """
        liquidate_at_trading_end_time = False
        actual = self._test_dataframe_portfolio_helper(
            system,
            liquidate_at_trading_end_time=liquidate_at_trading_end_time,
        )
        # TODO(Grisha): @Dan we should also freeze the config for all the tests
        #  with a Portfolio.
        self.check_string(actual, fuzzy_match=True, purify_text=True)

    def _test_with_liquidate_at_end_of_day1(
        self, system: dtfsyssyst.System
    ) -> None:
        """
        - Run a system:
            - Using a DataFramePortfolio
            - Liquidating at 10am
            - Freezing the output

        Like `_test1` but liquidating at 10am.
        """
        # Liquidate at 10am.
        trading_end_time = datetime.time(10, 0)
        liquidate_at_trading_end_time = True
        actual = self._test_dataframe_portfolio_helper(
            system,
            trading_end_time=trading_end_time,
            liquidate_at_trading_end_time=liquidate_at_trading_end_time,
        )
        # TODO(Grisha): @Dan we should also freeze the config for all the tests
        #  with a Portfolio.
        self.check_string(actual, fuzzy_match=True, purify_text=True)


# #############################################################################
# Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1
# #############################################################################


class Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1(
    otodh.TestOmsDbHelper,
):
    """
    Test end-to-end system with:

    - `RealTimeDagRunner`
    - `ReplayedMarketData`
    - `DatabasePortfolio` and `OrderProcessor`
    """

    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def _test_database_portfolio_helper(
        self,
        system: dtfsyssyst.System,
        *,
        add_system_config: bool = True,
        add_run_signature: bool = True,
    ) -> str:
        """
        Run a System with a DatabasePortfolio.
        """
        dtfssybuut.apply_unit_test_log_dir(self, system)
        #
        asset_id_name = system.config["market_data_config", "asset_id_col_name"]
        incremental = False
        oms.create_oms_tables(self.connection, incremental, asset_id_name)
        #
        with hasynci.solipsism_context() as event_loop:
            coroutines = []
            # Complete system config.
            system.config["event_loop_object"] = event_loop
            system.config["db_connection_object"] = self.connection
            # 1) Check the system config.
            tag = "database_portfolio"
            check_system_config(self, system, tag)
            # Create DAG runner.
            dag_runner = system.dag_runner
            coroutines.append(dag_runner.predict())
            # Create and add order processor.
            order_processor_coroutine = system.order_processor
            hdbg.dassert_isinstance(order_processor_coroutine, Coroutine)
            coroutines.append(order_processor_coroutine)
            #
            coro_output = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
        # 2) Check the signature from the result bundle.
        # Pick the result_bundle that corresponds to the DagRunner.
        result_bundles = coro_output[0]
        actual = _get_signature_from_result_bundle(
            system, result_bundles, add_system_config, add_run_signature
        )
        return actual

    def _test1(self, system: dtfsyssyst.System) -> None:
        """
        Run a system using the desired DB portfolio and freeze the output.
        """
        actual = self._test_database_portfolio_helper(system)
        self.check_string(actual, fuzzy_match=True, purify_text=True)


# #############################################################################
# Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio_TestCase1
# #############################################################################


class Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_vs_DataFramePortfolio_TestCase1(
    Time_ForecastSystem_with_DataFramePortfolio_TestCase1,
    Time_ForecastSystem_with_DatabasePortfolio_and_OrderProcessor_TestCase1,
):
    def _test1(
        self,
        system_with_dataframe_portfolio: dtfsyssyst.System,
        system_with_database_portfolio: dtfsyssyst.System,
    ) -> None:
        """
        Test that the outcome is the same when running a System with a
        DataFramePortfolio vs running one with a DatabasePortfolio.
        """
        # The config signature is different (since the systems are different) so
        # we only compare the result of the run.
        add_system_config = False
        add_run_signature = True
        actual = self._test_dataframe_portfolio_helper(
            system_with_dataframe_portfolio,
            add_system_config=add_system_config,
            add_run_signature=add_run_signature,
        )
        # Make sure there is something in the actual outcome.
        hdbg.dassert_lte(10, len(actual.split("\n")))
        expected = self._test_database_portfolio_helper(
            system_with_database_portfolio,
            add_system_config=add_system_config,
            add_run_signature=add_run_signature,
        )
        #
        hdbg.dassert_lte(10, len(expected.split("\n")))
        # Remove `DataFrame*` and `Database*` to avoid mismatches from the fact
        # that the systems are different.
        regex = (
            "DataFramePortfolio|DatabasePortfolio|SimulatedBroker|DatabaseBroker"
        )
        actual = hunitest.filter_text(regex, actual)
        expected = hunitest.filter_text(regex, expected)
        self.assert_equal(
            actual,
            expected,
            fuzzy_match=True,
            purify_text=True,
            purify_expected_text=True,
        )


# #############################################################################
# NonTime_ForecastSystem_vs_Time_ForecastSystem_TestCase1
# #############################################################################


class NonTime_ForecastSystem_vs_Time_ForecastSystem_TestCase1(hunitest.TestCase):
    """
    Reconcile (non-time) `ForecastSystem` and `Time_ForecastSystem`.

    Make sure that (non-time) `ForecastSystem` and `Time_ForecastSystem`
    produce the same predictions.
    """

    @abc.abstractmethod
    def get_Time_ForecastSystem(self) -> dtfsyssyst.System:
        """
        Get the `Time_ForecastSystem` to be compared to the (non-time) `ForecastSystem`.
        """

    @abc.abstractmethod
    def get_NonTime_ForecastSystem_from_Time_ForecastSystem(
        self, time_system: dtfsyssyst.System
    ) -> dtfsyssyst.System:
        """
        Get the (non-time) `ForecastSystem` via initiated `Time_ForecastSystem`.
        """

    # TODO(Grisha): @Dan make `get_file_path()` free-standing.
    def get_file_path(self) -> str:
        """
        Get path to a file with the market data to replay.

        E.g., `s3://.../unit_test/outcomes/Test_C1b_ForecastSystem_vs_Time_ForecastSystem1/input/data.csv.gz`.
        """
        input_dir = self.get_input_dir(
            use_only_test_class=True,
            use_absolute_path=False,
        )
        file_name = "data.csv.gz"
        aws_profile = "ck"
        s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
        file_path = os.path.join(
            s3_bucket_path,
            "unit_test",
            input_dir,
            file_name,
        )
        return file_path

    # TODO(Grisha): Consolidate into `SystemTester`.
    def get_signature(self, result_bundle: dtfcore.ResultBundle, col: str) -> str:
        txt: List[str] = []
        #
        txt.append(hprint.frame(col))
        result_df = result_bundle.result_df
        data = result_df[col].dropna(how="all").round(3)
        data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        txt.append(data_str)
        #
        res = "\n".join(txt)
        return res

    # TODO(Grisha): @Dan factor out the code, given `system_test_case.py`.
    def get_Time_ForecastSystem_signature(
        self, time_system: dtfsyssyst.System, output_col_name: str
    ) -> str:
        """
        Get `Time_ForecastSystem` outcome signature.
        """
        with hasynci.solipsism_context() as event_loop:
            # Complete system config.
            time_system.config["event_loop_object"] = event_loop
            # Build DAG runner.
            time_system_dag_runner = time_system.dag_runner
            # Config is complete: freeze it before running since we want to be
            # notified of any config changes, before running.
            self.check_string(
                str(time_system.config),
                tag="time_system_config",
                purify_text=True,
            )
            # Run.
            coroutines = [time_system_dag_runner.predict()]
            time_system_result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            # Get the last result bundle data for comparison.
            time_system_result_bundle = time_system_result_bundles[0][-1]
            time_system_result_bundle = self.postprocess_result_bundle(
                time_system_result_bundle
            )
            time_system_signature = self.get_signature(
                time_system_result_bundle, output_col_name
            )
        return time_system_signature

    def get_NonTime_ForecastSystem_signature(
        self, non_time_system: dtfsyssyst.System, output_col_name: str
    ) -> str:
        """
        Get (non-time) `ForecastSystem` outcome signature.
        """
        # Build the `Forecast_System` DAG runner.
        non_time_system_dag_runner = non_time_system.dag_runner
        # Config is complete: freeze it before running since we want to be
        # notified of any config changes, before running.
        self.check_string(
            str(non_time_system.config),
            tag="non_time_system_config",
            purify_text=True,
        )
        # Set the time boundaries.
        start_timestamp = non_time_system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ]
        end_timestamp = non_time_system.config["backtest_config", "end_timestamp"]
        non_time_system_dag_runner.set_predict_intervals(
            [(start_timestamp, end_timestamp)],
        )
        # Run.
        non_time_system_result_bundle = non_time_system_dag_runner.predict()
        non_time_system_result_bundle = self.postprocess_result_bundle(
            non_time_system_result_bundle
        )
        non_time_system_signature = self.get_signature(
            non_time_system_result_bundle, output_col_name
        )
        return non_time_system_signature

    @staticmethod
    def postprocess_result_bundle(
        result_bundle: dtfcore.ResultBundle
    ) -> dtfcore.ResultBundle:
        """
        Postprocess result bundle to unify system output format for comparison.
        """
        # Clear index column name in result dataframe.
        result_bundle_df = result_bundle.result_df
        result_bundle_df.index.name = None
        result_bundle.result_df = result_bundle_df
        return result_bundle

    def _test1(self, output_col_name: str) -> None:
        time_system = self.get_Time_ForecastSystem()
        time_system_signature = self.get_Time_ForecastSystem_signature(
            time_system, output_col_name
        )
        non_time_system = self.get_NonTime_ForecastSystem_from_Time_ForecastSystem(time_system)
        non_time_system_signature = self.get_NonTime_ForecastSystem_signature(
            non_time_system, output_col_name
        )
        # Compare system results.
        self.assert_equal(
            time_system_signature,
            non_time_system_signature,
            fuzzy_match=True,
            purify_text=True,
            purify_expected_text=True,
        )


# #############################################################################
# SystemTester
# #############################################################################


# TODO(gp): @all These functions should be free-standing.
class SystemTester:
    """
    Test a System.
    """

    def get_events_signature(self, events) -> str:
        # TODO(gp): Use events.to_str()
        actual = ["# event signature=\n"]
        events_as_str = "\n".join(
            [
                event.to_str(
                    include_tenths_of_secs=False,
                    include_wall_clock_time=False,
                )
                for event in events
            ]
        )
        actual.append("events_as_str=\n%s" % events_as_str)
        actual = "\n".join(actual)
        return actual

    def get_portfolio_signature(self, portfolio) -> Tuple[str, pd.Series]:
        actual = ["\n# portfolio signature=\n"]
        actual.append(str(portfolio))
        actual = "\n".join(actual)
        statistics = portfolio.get_historical_statistics()
        pnl = statistics["pnl"]
        _LOG.debug("pnl=\n%s", pnl)
        return actual, pnl

    def compute_run_signature(
        self,
        dag_runner: dtfcore.DagRunner,
        portfolio: oms.Portfolio,
        result_bundle: dtfcore.ResultBundle,
        forecast_evaluator_from_prices_dict: Dict[str, Any],
    ) -> str:
        hdbg.dassert_isinstance(result_bundle, dtfcore.ResultBundle)
        # Check output.
        actual = []
        #
        events = dag_runner.events
        actual.append(self.get_events_signature(events))
        signature, pnl = self.get_portfolio_signature(portfolio)
        actual.append(signature)
        signature, research_pnl = self.get_research_pnl_signature(
            result_bundle,
            forecast_evaluator_from_prices_dict,
        )
        actual.append(signature)
        if min(pnl.count(), research_pnl.count()) > 1:
            # Drop leading NaNs and burn the first PnL entry.
            research_pnl = research_pnl.dropna().iloc[1:]
            tail = research_pnl.size
            # We create new series because the portfolio times may be
            # disaligned from the research bar times.
            pnl1 = pd.Series(pnl.tail(tail).values)
            _LOG.debug("portfolio pnl=\n%s", pnl1)
            corr_samples = min(tail, pnl1.size)
            pnl2 = pd.Series(research_pnl.tail(corr_samples).values)
            _LOG.debug("research pnl=\n%s", pnl2)
            correlation = pnl1.corr(pnl2)
            actual.append("\n# pnl agreement with research pnl\n")
            actual.append(f"corr = {correlation:.3f}")
            actual.append(f"corr_samples = {corr_samples}")
        actual = "\n".join(map(str, actual))
        return actual

    def get_research_pnl_signature(
        self,
        result_bundle: dtfcore.ResultBundle,
        forecast_evaluator_from_prices_dict: Dict[str, Any],
    ) -> Tuple[str, pd.Series]:
        hdbg.dassert_isinstance(result_bundle, dtfcore.ResultBundle)
        # TODO(gp): @all use actual.append(hprint.frame("system_config"))
        #  to separate the sections of the output.
        actual = ["\n# forecast_evaluator_from_prices signature=\n"]
        hdbg.dassert(
            forecast_evaluator_from_prices_dict,
            "`forecast_evaluator_from_prices_dict` must be nontrivial",
        )
        forecast_evaluator = dtfmod.ForecastEvaluatorFromPrices(
            **forecast_evaluator_from_prices_dict["init"],
        )
        result_df = result_bundle.result_df
        _LOG.debug("result_df=\n%s", hpandas.df_to_str(result_df))
        #
        signature = forecast_evaluator.to_str(
            result_df,
            style=forecast_evaluator_from_prices_dict["style"],
            **forecast_evaluator_from_prices_dict["kwargs"],
        )
        _LOG.debug("signature=\n%s", signature)
        actual.append(signature)
        #
        _, _, _, _, stats = forecast_evaluator.compute_portfolio(
            result_df,
            style=forecast_evaluator_from_prices_dict["style"],
            **forecast_evaluator_from_prices_dict["kwargs"],
        )
        research_pnl = stats["pnl"]
        actual = "\n".join(map(str, actual))
        return actual, research_pnl

    @staticmethod
    def _append(
        list_: List[str], label: str, data: Union[pd.Series, pd.DataFrame]
    ) -> None:
        data_str = hpandas.df_to_str(data, index=True, num_rows=None, decimals=3)
        list_.append(f"{label}=\n{data_str}")


def check_system_config(self: Any, system: dtfsyssyst.System, tag: str) -> None:
    txt = []
    tag = "system_config." + tag
    txt.append(hprint.frame(tag))
    # Ensure that the System was built and thus the config is stable.
    hdbg.dassert(system.is_fully_built)
    txt.append(str(system.config))
    txt = "\n".join(txt)
    txt = hunitest.filter_text("db_connection_object", txt)
    txt = hunitest.filter_text("log_dir:", txt)
    txt = hunitest.filter_text("trade_date:", txt)
    # Sometimes we want to check that the config has not changed, but it
    # was just reordered. In this case we
    # - set `sort=True`
    # - make sure that there are no changes
    # - set `sort=False`
    # - update the golden outcomes with the updated config
    # TODO(gp): Do not commit `sort = True`.
    # sort = True
    sort = False
    self.check_string(txt, tag=tag, purify_text=True, sort=sort)


def check_portfolio_state(
    self: Any, system: dtfsyssyst.System, expected_last_timestamp: pd.Timestamp
) -> None:
    """
    Check some high level property of the Portfolio, e.g.,

    - It contains data up to a certain `expected_last_timestamp`
    - It is not empty at the end of the simulation
    """
    portfolio = system.portfolio
    # 1) The simulation runs up to the right time.
    last_timestamp = portfolio.get_last_timestamp()
    self.assert_equal(str(last_timestamp), str(expected_last_timestamp))
    # 2) The portfolio has some holdings.
    has_no_holdings = portfolio.has_no_holdings()
    self.assertFalse(has_no_holdings)
