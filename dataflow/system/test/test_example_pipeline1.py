import asyncio
import logging
from typing import List, Union

import pandas as pd
import pytest

import core.real_time_example as cretiexa
import dataflow.pipelines.examples.pipeline1 as dtfpiexpip
import dataflow.system.real_time_dag_adapter as dtfsrtdaad
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import dataflow.system.system_tester as dtfsysytes
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import market_data as mdata
import oms.oms_db as oomsdb
import oms.order_processor as oordproc
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as otodh

_LOG = logging.getLogger(__name__)

# TODO(Paul, gp): Factor this out.


class TestExamplePipeline1(otodh.TestOmsDbHelper):
    """
    Test pipeline end-to-end with fake data, features.
    """

    asset_ids = [101]

    def get_database_portfolio(
        self,
        event_loop: asyncio.AbstractEventLoop,
        market_data: mdata.AbstractMarketData,
    ) -> omportfo.MockedPortfolio:
        db_connection = self.connection
        table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
        # Neither the fake data nor the pipeline is filtering out weekends, and
        # so this is treated as a valid trading day.
        portfolio = oporexam.get_mocked_portfolio_example1(
            event_loop,
            db_connection,
            table_name,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=self.asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio

    def get_dataframe_portfolio(
        self,
        event_loop: asyncio.AbstractEventLoop,
        market_data: mdata.AbstractMarketData,
    ) -> omportfo.SimulatedPortfolio:
        # Neither the fake data nor the pipeline is filtering out weekends, and
        # so this is treated as a valid trading day.
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            market_data=market_data,
            mark_to_market_col="close",
            pricing_method="twap.5T",
            asset_ids=self.asset_ids,
        )
        portfolio.broker._column_remap = {
            "bid": "bid",
            "ask": "ask",
            "midpoint": "midpoint",
            "price": "close",
        }
        return portfolio

    def get_order_processor(
        self, portfolio: omportfo.MockedPortfolio
    ) -> oordproc.OrderProcessor:
        db_connection = self.connection
        get_wall_clock_time = portfolio._get_wall_clock_time
        order_processor_poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
        # order_processor_poll_kwargs["sleep_in_secs"] = 1
        # Since orders should come every 5 mins we give it a buffer of 5 extra
        # mins.
        order_processor_poll_kwargs["timeout_in_secs"] = 60 * 20
        delay_to_accept_in_secs = 3
        delay_to_fill_in_secs = 10
        broker = portfolio.broker
        order_processor = oordproc.OrderProcessor(
            db_connection,
            delay_to_accept_in_secs,
            delay_to_fill_in_secs,
            broker,
            poll_kwargs=order_processor_poll_kwargs,
        )
        return order_processor

    @pytest.mark.slow
    def test_market_data1_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df1()
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data2_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df2()
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data3_database_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df3()
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.check_string(actual)

    @pytest.mark.slow
    def test_market_data1_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df1()
        expected = self._run_coroutines(data, real_time_loop_time_out_in_secs)
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)

    @pytest.mark.slow
    def test_market_data2_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df2()
        expected = self._run_coroutines(data, real_time_loop_time_out_in_secs)
        actual = self._run_coroutines(
            data, real_time_loop_time_out_in_secs, is_database_portfolio=False
        )
        self.assert_equal(actual, expected)

    @pytest.mark.superslow("Times out in GH Actions.")
    def test_market_data3_database_vs_dataframe_portfolio(self) -> None:
        data, real_time_loop_time_out_in_secs = self._get_market_data_df3()
        expected = self._run_coroutines(data, real_time_loop_time_out_in_secs)
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
            initial_replayed_delay = 5
            market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
                event_loop,
                initial_replayed_delay,
                data,
            )
            if is_database_portfolio:
                # Clean the DB tables.
                oomsdb.create_oms_tables(self.connection, incremental=False)
                portfolio = self.get_database_portfolio(event_loop, market_data)
            else:
                portfolio = self.get_dataframe_portfolio(event_loop, market_data)
            # Create the real-time DAG.
            base_dag_builder = dtfpiexpip.ExamplePipeline1_ModelBuilder()
            prediction_col = "feature1"
            volatility_col = "vwap.ret_0.vol"
            returns_col = "vwap.ret_0"
            timedelta = pd.Timedelta("7D")
            asset_id_col = "asset_id"
            dag_builder = dtfsrtdaad.RealTimeDagAdapter(
                base_dag_builder,
                portfolio,
                prediction_col,
                volatility_col,
                returns_col,
                timedelta,
                asset_id_col,
                # log_dir="tmp_log_dir",
            )
            _LOG.debug("dag_builder=\n%s", dag_builder)
            config = dag_builder.get_config_template()
            # Set up the event loop.
            sleep_interval_in_secs = 60 * 5
            execute_rt_loop_kwargs = (
                cretiexa.get_replayed_time_execute_rt_loop_kwargs(
                    sleep_interval_in_secs, event_loop=event_loop
                )
            )
            execute_rt_loop_kwargs[
                "time_out_in_secs"
            ] = real_time_loop_time_out_in_secs
            dag_runner_kwargs = {
                "config": config,
                "dag_builder": dag_builder,
                "fit_state": None,
                "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
                "dst_dir": None,
            }
            # Set up coroutines.
            dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
            coroutines = [dag_runner.predict()]
            if is_database_portfolio:
                # Build OrderProcessor.
                order_processor = self.get_order_processor(portfolio)
                # TODO(Paul): Maybe make this public.
                initial_timestamp = portfolio._initial_timestamp
                offset = pd.Timedelta(
                    real_time_loop_time_out_in_secs, unit="seconds"
                )
                termination_condition = initial_timestamp + offset
                order_processor_coroutine = order_processor.run_loop(
                    termination_condition
                )
                coroutines.append(order_processor_coroutine)
            # Run.
            # TODO(Paul): Why is this a list?
            result_bundles = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )
            #
            system_tester = dtfsysytes.SystemTester()
            result_bundles = result_bundles[0]
            returns_col = "vwap.ret_0"
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

    @staticmethod
    def _append(
        list_: List[str], label: str, data: Union[pd.Series, pd.DataFrame]
    ) -> None:
        data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        list_.append(f"{label}=\n{data_str}")
