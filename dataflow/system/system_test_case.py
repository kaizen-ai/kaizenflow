"""
Import as:

import dataflow.system.system_test_case as dtfssyteca
"""

import abc
import asyncio
import datetime
import logging
import os
from typing import Any, Callable, Coroutine, List, Optional, Tuple

import pandas as pd

import dataflow.core as dtfcore
import dataflow.system.system as dtfsyssyst
import dataflow.system.system_builder_utils as dtfssybuut
import dataflow.system.system_signature as dtfsysysig
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hs3 as hs3
import helpers.hunit_test as hunitest
import oms as oms
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)


# #############################################################################
# Utils
# #############################################################################


def run_NonTime_ForecastSystem_from_backtest_config(
    self: Any,
    system: dtfsyssyst.System,
    method: str,
    config_tag: str,
) -> dtfcore.ResultBundle:
    """
    Run non-time `ForecastSystem` DAG with the specified fit / predict method
    and using the backtest parameters from `SystemConfig`.

    :param system: system object to extract `DagRunner` from
    :param method: "fit" or "predict"
    :param config_tag: tag used to freeze the system config by `check_system_config()`
    :return: result bundle
    """
    hdbg.dassert_in(method, ["fit", "predict"])
    dtfssybuut.apply_unit_test_log_dir(self, system)
    # Build `DagRunner`.
    dag_runner = system.dag_runner
    hdbg.dassert_isinstance(dag_runner, dtfcore.DagRunner)
    # Check the system config against the frozen value.
    dtfsysysig.check_system_config(self, system, config_tag)
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


def run_Time_ForecastSystem(
    self: Any,
    system: dtfsyssyst.System,
    config_tag: str,
) -> List[dtfcore.ResultBundle]:
    """
    Run `Time_ForecastSystem` with predict method.

    :param system: `Time_ForecastSystem` object
    :param config_tag: tag used to freeze the system config by `check_system_config()`
    :return: `DagRunner` result bundles
    """
    dtfssybuut.apply_unit_test_log_dir(self, system)
    #
    with hasynci.solipsism_context() as event_loop:
        coroutines = []
        # Complete the system config.
        system.config["event_loop_object"] = event_loop
        # Create a `DagRunner`.
        dag_runner = system.dag_runner
        # Check the system config against the frozen value.
        dtfsysysig.check_system_config(self, system, config_tag)
        coroutines.append(dag_runner.predict())
        #
        if "order_processor_config" in system.config:
            # Get the `OrderProcessor` coroutine.
            order_processor_coroutine = system.order_processor
            hdbg.dassert_isinstance(order_processor_coroutine, Coroutine)
            coroutines.append(order_processor_coroutine)
        #
        results = hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)
        # Extract the result bundles from the `DagRunner`.
        result_bundles = results[0]
    return result_bundles


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
        # Build `DagRunner`.
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
        method = "fit"
        # TODO(Grisha): @Dan Rename to "forecast_system" in CmTask2739 "Introduce `NonTime_ForecastSystem`."
        config_tag = "forecast_system"
        result_bundle = run_NonTime_ForecastSystem_from_backtest_config(
            self, system, method, config_tag
        )
        # Check outcome.
        actual = dtfsysysig.get_signature(
            system.config, result_bundle, output_col_name
        )
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
        # Build `DagRunner`.
        dag_runner = system.dag_runner
        # Set the time boundaries.
        dag_runner.set_fit_intervals(
            [(start_timestamp, end_timestamp)],
        )
        # Run.
        result_bundle = dag_runner.fit()
        # Check outcome.
        actual = dtfsysysig.get_signature(
            system.config, result_bundle, output_col_name
        )
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
        method = "fit"
        config_tag = "forecast_system"
        fit_result_bundle = run_NonTime_ForecastSystem_from_backtest_config(
            self, system, method, config_tag
        )
        fit_df = fit_result_bundle.result_df
        # Predict.
        method = "predict"
        config_tag = "forecast_system"
        predict_result_bundle = run_NonTime_ForecastSystem_from_backtest_config(
            self, system, method, config_tag
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
        method = "fit"
        config_tag = "forecast_system"
        result_bundle = run_NonTime_ForecastSystem_from_backtest_config(
            self, system, method, config_tag
        )
        # Check the pnl.
        forecast_evaluator_from_prices_dict = system.config[
            "research_forecast_evaluator_from_prices"
        ].to_dict()
        signature, _ = dtfsysysig.get_research_pnl_signature(
            self, result_bundle, forecast_evaluator_from_prices_dict
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
        # Run the system.
        config_tag = "forecast_system"
        result_bundles = run_Time_ForecastSystem(self, system, config_tag)
        # Check the run signature.
        result_bundle = result_bundles[-1]
        actual = dtfsysysig.get_signature(
            system.config, result_bundle, output_col_name
        )
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
        # 1) Run the system.
        config_tag = "dataframe_portfolio"
        result_bundles = run_Time_ForecastSystem(self, system, config_tag)
        # 2) Check the run signature.
        actual = dtfsysysig._get_signature_from_result_bundle(
            self, system, result_bundles, add_system_config, add_run_signature
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
        asset_id_name = system.config["market_data_config", "asset_id_col_name"]
        incremental = False
        oms.create_oms_tables(self.connection, incremental, asset_id_name)
        system.config["db_connection_object"] = self.connection
        # Run the system.
        config_tag = "database_portfolio"
        result_bundles = run_Time_ForecastSystem(self, system, config_tag)
        # Check the run signature.
        actual = dtfsysysig._get_signature_from_result_bundle(
            self, system, result_bundles, add_system_config, add_run_signature
        )
        return actual

    def _test1(self, system: dtfsyssyst.System) -> None:
        """
        Run a system using the desired DB portfolio and freeze the output.
        """
        actual = self._test_database_portfolio_helper(system)
        self.check_string(actual, fuzzy_match=True, purify_text=True)


# #############################################################################
# NonTime_ForecastSystem_vs_Time_ForecastSystem_TestCase1
# #############################################################################


class NonTime_ForecastSystem_vs_Time_ForecastSystem_TestCase1(hunitest.TestCase):
    """
    Reconcile (non-time) `ForecastSystem` and `Time_ForecastSystem`.

    Make sure that (non-time) `ForecastSystem` and `Time_ForecastSystem`
    produce the same predictions.
    """

    @staticmethod
    def postprocess_result_bundle(
        result_bundle: dtfcore.ResultBundle,
    ) -> dtfcore.ResultBundle:
        """
        Postprocess result bundle to unify system output format for comparison.

        - Clear index column name since it may differ for systems,
          e.g. "start_ts" and "start_datetime"
        """
        result_bundle_df = result_bundle.result_df
        result_bundle_df.index.name = None
        result_bundle.result_df = result_bundle_df
        return result_bundle

    @abc.abstractmethod
    def get_NonTime_ForecastSystem_from_Time_ForecastSystem(
        self, time_system: dtfsyssyst.System
    ) -> dtfsyssyst.System:
        """
        Get the (non-time) `ForecastSystem` via initiated
        `Time_ForecastSystem`.
        """

    @abc.abstractmethod
    def get_Time_ForecastSystem(self) -> dtfsyssyst.System:
        """
        Get the `Time_ForecastSystem` to be compared to the (non-time)
        `ForecastSystem`.
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

    # TODO(Grisha): Consolidate with `dtfsysysig.get_signature()`.
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

    def get_NonTime_ForecastSystem_signature(
        self, non_time_system: dtfsyssyst.System, output_col_name: str
    ) -> str:
        """
        Get (non-time) `ForecastSystem` outcome signature.
        """
        # Run the system.
        method = "predict"
        config_tag = "non_time_system"
        non_time_system_result_bundle = (
            run_NonTime_ForecastSystem_from_backtest_config(
                self, non_time_system, method, config_tag
            )
        )
        non_time_system_result_bundle = self.postprocess_result_bundle(
            non_time_system_result_bundle
        )
        non_time_system_signature = self.get_signature(
            non_time_system_result_bundle, output_col_name
        )
        return non_time_system_signature

    # TODO(Grisha): @Dan factor out the code, given `system_test_case.py`.
    def get_Time_ForecastSystem_signature(
        self, time_system: dtfsyssyst.System, output_col_name: str
    ) -> str:
        """
        Get `Time_ForecastSystem` outcome signature.
        """
        # Run the system.
        config_tag = "time_system"
        time_system_result_bundles = run_Time_ForecastSystem(
            self, time_system, config_tag
        )
        # Get the last result bundle data for comparison.
        time_system_result_bundle = time_system_result_bundles[-1]
        time_system_result_bundle = self.postprocess_result_bundle(
            time_system_result_bundle
        )
        time_system_signature = self.get_signature(
            time_system_result_bundle, output_col_name
        )
        return time_system_signature

    def _test1(self, output_col_name: str) -> None:
        time_system = self.get_Time_ForecastSystem()
        time_system_signature = self.get_Time_ForecastSystem_signature(
            time_system, output_col_name
        )
        non_time_system = (
            self.get_NonTime_ForecastSystem_from_Time_ForecastSystem(time_system)
        )
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
# Test_C1b_Time_ForecastSystem_vs_Time_ForecastSystem_with_DataFramePortfolio_TestCase1
# #############################################################################


# TODO(Grisha): Use for the Mock1 pipeline.
class Test_C1b_Time_ForecastSystem_vs_Time_ForecastSystem_with_DataFramePortfolio_TestCase1(
    hunitest.TestCase
):
    """
    Reconcile `Time_ForecastSystem` and
    `Time_ForecastSystem_with_DataFramePortfolio`.

    It is expected that research PnL is strongly correlated with the PnL from Portfolio.
    2 versions of PnL may differ by a constant so we use correlation to compare them
    instead of comparing the values directly.

    Add `ForecastEvaluatorFromPrices` to `Time_ForecastSystem` to compute research PnL.
    """

    # TODO(Grisha): factor out, it is common for all the tests that read data
    # from S3.
    def get_file_path(self) -> str:
        """
        Get path to a file with the market data to replay.

        E.g., `s3://.../unit_test/outcomes/Test_C1b_Time_ForecastSystem_vs_Time_ForecastSystem_with_DataFramePortfolio1/input/data.csv.gz`.
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

    @abc.abstractmethod
    def get_Time_ForecastSystem(self) -> dtfsyssyst.System:
        """
        Get `Time_ForecastSystem` and fill the `system.config`.
        """

    def run_Time_ForecastSystem(self) -> Tuple[str, pd.Series]:
        """
        Run `Time_ForecastSystem` and compute research PnL.
        """
        time_system = self.get_Time_ForecastSystem()
        # Run the system and check the config against the frozen value.
        config_tag = "time_system"
        time_system_result_bundles = run_Time_ForecastSystem(
            self, time_system, config_tag
        )
        # Get the last result bundle data for comparison.
        result_bundle = time_system_result_bundles[-1]
        forecast_evaluator_from_prices_dict = time_system.config[
            "research_forecast_evaluator_from_prices"
        ].to_dict()
        signature, research_pnl = dtfsysysig.get_research_pnl_signature(
            self, result_bundle, forecast_evaluator_from_prices_dict
        )
        return signature, research_pnl

    @abc.abstractmethod
    def get_Time_ForecastSystem_with_DataFramePortfolio(
        self,
    ) -> dtfsyssyst.System:
        """
        Get `Time_ForecastSystem_with_DataFramePortfolio` and fill the
        `system.config`.
        """

    def run_Time_ForecastSystem_with_DataFramePortfolio(
        self,
    ) -> Tuple[str, pd.Series]:
        """
        Run `Time_ForecastSystem_with_DataFramePortfolio` and compute Portfolio
        PnL.
        """
        time_system = self.get_Time_ForecastSystem_with_DataFramePortfolio()
        # Run the system and check the config against the frozen value.
        config_tag = "dataframe_portfolio"
        _ = run_Time_ForecastSystem(self, time_system, config_tag)
        # Compute Portfolio PnL. Get the number of data points
        # that is sufficient for a reconciliation.
        num_periods = 20
        signature, pnl = dtfsysysig.get_portfolio_signature(
            self, time_system.portfolio, num_periods=num_periods
        )
        return signature, pnl

    def _test1(self) -> None:
        actual = []
        # Compute research PnL and check in the signature.
        research_signature, research_pnl = self.run_Time_ForecastSystem()
        actual.append(research_signature)
        # Compute Portfolio PnL and check in the signature.
        (
            portfolio_signature,
            pnl,
        ) = self.run_Time_ForecastSystem_with_DataFramePortfolio()
        actual.append(portfolio_signature)
        # Compute correlation for research PnL vs Portfolio PnL.
        # TODO(Grisha): copy-pasted from `system_test_case.py`, try to share code.
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
        # Check in the output.
        actual = "\n".join(map(str, actual))
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
