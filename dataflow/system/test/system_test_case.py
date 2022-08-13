"""
Import as:

import dataflow.system.test.system_test_case as dtfsytsytc
"""

import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, List, Tuple, Union

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
import helpers.hunit_test as hunitest
import market_data as mdata
import oms as oms
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)

# TODO(gp): -> system_test_case.py


# #############################################################################
# Utils
# #############################################################################


def get_signature(
    system_config: cconfig.Config, result_bundle: dtfcore.ResultBundle, col: str
) -> str:
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
    portfolio = system.portfolio
    dag_runner = system.dag_runner
    txt = []
    # - Compute system signature.
    if add_system_config:
        txt.append(hprint.frame("system_config"))
        txt.append(str(system.config))
    # - Compute run signature.
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
    # - Compute the signature of the output dir.
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
        # Force building the DAG runner.
        _ = system.dag_runner
        #
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
        dtfssybuut.apply_unit_test_log_dir(self, system)
        # Force building the DAG runner.
        dag_runner = system.dag_runner
        hdbg.dassert_isinstance(dag_runner, dtfcore.DagRunner)
        # Set the time boundaries.
        start_datetime = system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ]
        end_datetime = system.config["backtest_config", "end_timestamp"]
        dag_runner.set_fit_intervals(
            [(start_datetime, end_datetime)],
        )
        # Run.
        result_bundle = dag_runner.fit()
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
        # Force building the DAG runner.
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
        dtfssybuut.apply_unit_test_log_dir(self, system)
        # Force building the DAG runner.
        dag_runner = system.dag_runner
        # Get the time boundaries.
        start_datetime = system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ]
        end_datetime = system.config["backtest_config", "end_timestamp"]
        # Fit.
        dag_runner.set_fit_intervals(
            [(start_datetime, end_datetime)],
        )
        fit_result_bundle = dag_runner.fit()
        fit_df = fit_result_bundle.result_df
        # Predict.
        dag_runner.set_predict_intervals(
            [(start_datetime, end_datetime)],
        )
        predict_result_bundle = dag_runner.predict()
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
        dtfssybuut.apply_unit_test_log_dir(self, system)
        dag_runner = system.dag_runner
        # Set the time boundaries.
        start_datetime = system.config[
            "backtest_config", "start_timestamp_with_lookback"
        ]
        end_datetime = system.config["backtest_config", "end_timestamp"]
        dag_runner.set_fit_intervals(
            [(start_datetime, end_datetime)],
        )
        # Run.
        result_bundle = dag_runner.fit()
        # Check.
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

    def _test_save_data(
        self,
        market_data: mdata.MarketData,
        period: pd.Timedelta,
        file_path: str,
    ) -> None:
        """
        Generate test data and store it.
        """
        limit = None
        mdata.save_market_data(market_data, file_path, period, limit)
        _LOG.warning("Updated file '%s'", file_path)

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
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
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
        add_system_config: bool = True,
        add_run_signature: bool = True,
    ) -> str:
        """
        Run a System with a DataframePortfolio.
        """
        dtfssybuut.apply_unit_test_log_dir(self, system)
        with hasynci.solipsism_context() as event_loop:
            #
            system.config["event_loop_object"] = event_loop
            dag_runner = system.dag_runner
            # Run.
            coroutines = [dag_runner.predict()]
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            # Check.
            # Pick the ResultBundle corresponding to the DagRunner execution.
            result_bundles = result_bundles[0]
            actual = _get_signature_from_result_bundle(
                system, result_bundles, add_system_config, add_run_signature
            )
            return actual

    def _test_save_data(
        self, market_data: mdata.MarketData, period: pd.Timedelta, file_name: str
    ) -> None:
        """
        Generate data used in this test.

        E.g.,
        ```
        end_time,start_time,asset_id,close,volume,good_bid,good_ask,sided_bid_count,sided_ask_count,day_spread,day_num_spread
        2022-01-10 09:01:00-05:00,2022-01-10 14:00:00+00:00,10971.0,,0.0,463.0,463.01,0.0,0.0,1.32,59.0
        2022-01-10 09:01:00-05:00,2022-01-10 14:00:00+00:00,13684.0,,0.0,998.14,999.4,0.0,0.0,100.03,59.0
        2022-01-10 09:01:00-05:00,2022-01-10 14:00:00+00:00,17085.0,,0.0,169.27,169.3,0.0,0.0,1.81,59.0
        2022-01-10 09:02:00-05:00,2022-01-10 14:01:00+00:00,10971.0,,0.0,463.03,463.04,0.0,0.0,2.71,119.0
        ```
        """
        # period = "last_day"
        # period = pd.Timedelta("15D")
        limit = None
        mdata.save_market_data(market_data, file_name, period, limit)
        _LOG.warning("Updated file '%s'", file_name)
        # aws s3 cp dataflow_lime/system/test/TestReplayedE8dWithMockedOms1/input/real_time_bar_data.csv s3://eglp-spm-sasm/data/market_data.20220118.csv

    def _test1(self, system: dtfsyssyst.System) -> None:
        """
        Run a system using the desired DataFramePortfolio and freeze the
        output.
        """
        actual = self._test_dataframe_portfolio_helper(system)
        # TODO(Grisha): @Dan we should also freeze the config for all the tests
        # with a Portfolio.
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

    def _test_save_data(
        self, market_data: mdata.MarketData, period: pd.Timedelta, file_name: str
    ) -> None:
        """
        Generate data used in this test.

        E.g.,
        ```
        end_time,start_time,asset_id,close,volume,good_bid,good_ask,sided_bid_count,sided_ask_count,day_spread,day_num_spread
        2022-01-10 09:01:00-05:00,2022-01-10 14:00:00+00:00,10971.0,,0.0,463.0,463.01,0.0,0.0,1.32,59.0
        2022-01-10 09:01:00-05:00,2022-01-10 14:00:00+00:00,13684.0,,0.0,998.14,999.4,0.0,0.0,100.03,59.0
        2022-01-10 09:01:00-05:00,2022-01-10 14:00:00+00:00,17085.0,,0.0,169.27,169.3,0.0,0.0,1.81,59.0
        2022-01-10 09:02:00-05:00,2022-01-10 14:01:00+00:00,10971.0,,0.0,463.03,463.04,0.0,0.0,2.71,119.0
        ```
        """
        # period = "last_day"
        # period = pd.Timedelta("15D")
        limit = None
        mdata.save_market_data(market_data, file_name, period, limit)
        _LOG.warning("Updated file '%s'", file_name)
        # aws s3 cp dataflow_lime/system/test/TestReplayedE8dWithMockedOms1/input/real_time_bar_data.csv s3://eglp-spm-sasm/data/market_data.20220118.csv

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
        # Check.
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
        hdbg.dassert_lte(10, len(expected.split("\n")))
        self.assert_equal(
            actual,
            expected,
            fuzzy_match=True,
            purify_text=True,
            purify_expected_text=True,
        )


# #############################################################################
# SystemTester
# #############################################################################


# TODO(gp): These functions should be free-standing.
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
