import asyncio
import logging

import pandas as pd
import pytest

import dataflow.system.example_pipeline1_system_runner as dtfsepsyru
import dataflow.system.system_tester as dtfsysytes
import helpers.hasyncio as hasynci
import market_data as mdata
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)

# TODO(gp): Factor this out.


class TestExamplePipeline1(otodh.TestOmsDbHelper):
    """
    Test pipeline end-to-end with fake data, features.
    """

    asset_ids = [101]

    @pytest.mark.slow
    def test_market_data1_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df1()
        actual = self._run_coroutines(data, real_time_loop_time_out_in_secs)
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data2_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df2()
        actual = self._run_coroutines(data, real_time_loop_time_out_in_secs)
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data3_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df3()
        actual = self._run_coroutines(data, real_time_loop_time_out_in_secs)
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data1_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df1()
        expected = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)

    @pytest.mark.slow
    def test_market_data2_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df2()
        expected = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)

    @pytest.mark.superslow("Times out in GH Actions.")
    def test_market_data3_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df3()
        expected = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=True
        )
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)

    def _run_coroutines(
        self,
        data: pd.DataFrame,
        real_time_loop_time_out_in_secs: int,
        is_database_portfolio: bool = True,
    ) -> str:
        with hasynci.solipsism_context() as event_loop:
            asset_ids = [101]
            if is_database_portfolio:
                system_runner = dtfsepsyru.ExamplePipeline1_Database_SystemRunner(
                    asset_ids, event_loop, db_connection=self.connection
                )
            else:
                system_runner = dtfsepsyru.ExamplePipeline1_Dataframe_SystemRunner(
                    asset_ids, event_loop
                )
            market_data = system_runner.get_market_data(data)
            portfolio = system_runner.get_portfolio(market_data)
            returns_col = "vwap.ret_0"
            volatility_col = "vwap.ret_0.vol"
            prediction_col = "feature1"
            config, dag_builder = system_runner.get_dag(
                portfolio,
                prediction_col=prediction_col,
                volatility_col=volatility_col,
                returns_col=returns_col,
            )
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

    @staticmethod
    def _get_market_data_df1() -> pd.DataFrame:
        """
        Generate price series that alternates every 5 minutes.
        """
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 10:10:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
        price_pattern = [101.0] * 5 + [100.0] * 5
        price = price_pattern * 4
        data["close"] = price
        data["asset_id"] = 101
        data["volume"] = 100
        feature_pattern = [1.0] * 5 + [-1.0] * 5
        feature = feature_pattern * 4
        data["feature1"] = feature
        real_time_loop_time_out_in_secs = 35 * 60
        return data, real_time_loop_time_out_in_secs

    @staticmethod
    def _get_market_data_df2() -> pd.DataFrame:
        """
        Generate price series that alternates every 5 minutes.
        """
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 10:10:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
        price_pattern = [101.0] * 2 + [100.0] * 2 + [101.0] * 2 + [102.0] * 4
        price = price_pattern * 4
        data["close"] = price
        data["asset_id"] = 101
        data["volume"] = 100
        feature_pattern = [-1.0] * 5 + [1.0] * 5
        feature = feature_pattern * 4
        data["feature1"] = feature
        real_time_loop_time_out_in_secs = 35 * 60
        return data, real_time_loop_time_out_in_secs

    @staticmethod
    def _get_market_data_df3() -> pd.DataFrame:
        """
        Generate price series that alternates every 5 minutes.
        """
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 11:30:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
        price_pattern = [101.0] * 3 + [100.0] * 3 + [101.0] * 3 + [102.0] * 6
        price = price_pattern * 8
        data["close"] = price
        data["asset_id"] = 101
        data["volume"] = 100
        feature_pattern = [-1.0] * 5 + [1.0] * 5
        feature = feature_pattern * 12
        data["feature1"] = feature
        real_time_loop_time_out_in_secs = 115 * 60
        return data, real_time_loop_time_out_in_secs
