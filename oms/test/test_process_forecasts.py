import asyncio
import datetime
import logging
import pytest
from typing import List, Tuple, Union

import pandas as pd

import core.signal_processing as csigproc
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest
import market_data as mdata
import oms.oms_db as oomsdb
import oms.order_processor as oordproc
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.process_forecasts as oprofore
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)


class TestSimulatedProcessForecasts1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(
                self._test_simulated_system1(event_loop), event_loop=event_loop
            )

    async def _test_simulated_system1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Run `process_forecasts()` logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        (
            market_data,
            get_wall_clock_time,
        ) = mdata.get_ReplayedTimeMarketData_example3(event_loop)
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        # Build a Portfolio.
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            market_data=market_data,
            asset_ids=[101, 202],
        )
        config["order_type"] = "price@twap"
        config["order_duration"] = 5
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        config["execution_mode"] = "batch"
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        actual = str(portfolio)
        expected = r"""# historical holdings=
asset_id                         101        202            -1
2000-01-01 09:35:00-05:00        0.0        0.0  1000000.000000
2000-01-01 09:35:01-05:00        0.0        0.0  1000000.000000
2000-01-01 09:40:01-05:00  33.322939  66.645878   900039.564897
2000-01-01 09:45:01-05:00 -24.994967    74.9849   950024.378208
# historical holdings marked to market=
asset_id                            101           202            -1
2000-01-01 09:35:00-05:00           0.0           0.0  1000000.000000
2000-01-01 09:35:01-05:00           0.0           0.0  1000000.000000
2000-01-01 09:40:01-05:00  33329.649211  66659.298423   900039.564897
2000-01-01 09:45:01-05:00 -24992.928951  74978.786852   950024.378208
# historical statistics=
                           net_asset_holdings            cash    net_wealth  gross_exposure  leverage        pnl  realized_pnl  unrealized_pnl
2000-01-01 09:35:00-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000        NaN           NaN             NaN
2000-01-01 09:35:01-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000   0.000000      0.000000        0.000000
2000-01-01 09:40:01-05:00        99988.947634   900039.564897  1.000029e+06    99988.947634  0.099986  28.512531 -99960.435103    99988.947634
2000-01-01 09:45:01-05:00        49985.857901   950024.378208  1.000010e+06    99971.715802  0.099971 -18.276422  49984.813311   -50003.089733"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts1(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            #
            oomsdb.create_oms_tables(self.connection, incremental=False)
            #
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                asset_ids=[101, 202],
            )
            # Build OrderProcessor.
            get_wall_clock_time = portfolio._get_wall_clock_time
            poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
            # poll_kwargs["sleep_in_secs"] = 1
            poll_kwargs["timeout_in_secs"] = 60 * 10
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            termination_condition = 3
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                poll_kwargs=poll_kwargs,
            )
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            coroutines = [
                self._test_mocked_system1(portfolio),
                order_processor_coroutine,
            ]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        portfolio,
    ) -> None:
        """
        Run process_forecasts() logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        prediction_data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1, 1],
            [1, 1],
            [1, 1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        config["order_type"] = "price@twap"
        config["order_duration"] = 5
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        config["execution_mode"] = "batch"
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        # TODO(Paul): Re-check the correctness after fixing the issue with
        #  pricing assets not currently in the portfolio.
        actual = str(portfolio)
        # TODO(Paul): Get this and the simulated test output to agree perfectly.
        expected = r"""# historical holdings=
asset_id                         101        202            -1
2000-01-01 09:35:00-05:00        0.0        0.0  1000000.000000
2000-01-01 09:35:01-05:00        NaN        NaN  1000000.000000
2000-01-01 09:40:01-05:00  33.322939  66.645878   900039.564897
2000-01-01 09:45:01-05:00 -24.994967    74.9849   950024.378208
# historical holdings marked to market=
                                    101           202            -1
2000-01-01 09:35:00-05:00           0.0           0.0  1000000.000000
2000-01-01 09:35:01-05:00           NaN           NaN  1000000.000000
2000-01-01 09:40:01-05:00  33329.649211  66659.298423   900039.564897
2000-01-01 09:45:01-05:00 -24992.928951  74978.786852   950024.378208
# historical statistics=
                           net_asset_holdings            cash    net_wealth  gross_exposure  leverage        pnl  realized_pnl  unrealized_pnl
2000-01-01 09:35:00-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000        NaN           NaN             NaN
2000-01-01 09:35:01-05:00            0.000000  1000000.000000  1.000000e+06        0.000000  0.000000   0.000000      0.000000        0.000000
2000-01-01 09:40:01-05:00        99988.947634   900039.564897  1.000029e+06    99988.947634  0.099986  28.512531 -99960.435103    99988.947634
2000-01-01 09:45:01-05:00        49985.857901   950024.378208  1.000010e+06    99971.715802  0.099971 -18.276422  49984.813311   -50003.089733"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedProcessForecasts2(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        data = self._get_market_data_df1()
        predictions, volatility = self._get_predictions_and_volatility1(data)
        self._run_coroutines(data, predictions, volatility)

    def test_mocked_system2(self) -> None:
        data = self._get_market_data_df2()
        predictions, volatility = self._get_predictions_and_volatility1(data)
        self._run_coroutines(data, predictions, volatility)

    def test_mocked_system3(self) -> None:
        data = self._get_market_data_df1()
        predictions, volatility = self._get_predictions_and_volatility2(data)
        self._run_coroutines(data, predictions, volatility)

    @pytest.mark.skip(
        "This test times out because nothing interesting happens after the first set of orders."
    )
    def test_mocked_system4(self) -> None:
        data = self._get_market_data_df2()
        predictions, volatility = self._get_predictions_and_volatility2(data)
        self._run_coroutines(data, predictions, volatility)

    def _run_coroutines(self, data, predictions, volatility):
        with hasynci.solipsism_context() as event_loop:
            # Build MarketData.
            initial_replayed_delay = 5
            asset_id = [data["asset_id"][0]]
            market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
                event_loop,
                initial_replayed_delay,
                data,
            )
            # Create a portfolio with one asset (and cash).
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_oms_tables(self.connection, incremental=False)
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                market_data=market_data,
                asset_ids=asset_id,
            )
            # Build OrderProcessor.
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            poll_kwargs = hasynci.get_poll_kwargs(portfolio._get_wall_clock_time)
            poll_kwargs["timeout_in_secs"] = 60 * 10
            order_processor = oordproc.OrderProcessor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                poll_kwargs=poll_kwargs,
            )
            # Build order process coroutine.
            termination_condition = 4
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            coroutines = [
                self._test_mocked_system1(predictions, volatility, portfolio),
                order_processor_coroutine,
            ]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    @staticmethod
    def _get_market_data_df1() -> pd.DataFrame:
        """
        Generate price series that alternates every 5 minutes.
        """
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 09:55:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
        price = [
            101.0,
            101.0,
            101.0,
            101.0,
            101.0,
            #
            100.0,
            100.0,
            100.0,
            100.0,
            100.0,
            #
            101.0,
            101.0,
            101.0,
            101.0,
            101.0,
            #
            100.0,
            100.0,
            100.0,
            100.0,
            100.0,
            #
            101.0,
            101.0,
            101.0,
            101.0,
            101.0,
        ]
        data["price"] = price
        data["asset_id"] = 101
        return data

    @staticmethod
    def _get_market_data_df2() -> pd.DataFrame:
        idx = pd.date_range(
            start=pd.Timestamp(
                "2000-01-01 09:31:00-05:00", tz="America/New_York"
            ),
            end=pd.Timestamp("2000-01-01 09:55:00-05:00", tz="America/New_York"),
            freq="T",
        )
        bar_duration = "1T"
        bar_delay = "0T"
        data = mdata.build_timestamp_df(idx, bar_duration, bar_delay)
        data["price"] = 100
        data["asset_id"] = 101
        return data

    @staticmethod
    def _get_predictions_and_volatility1(
        market_data_df,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate a signal that alternates every 5 minutes.
        """
        # Build predictions.
        asset_id = market_data_df["asset_id"][0]
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
        ]
        # Sanity check the index (e.g., in case we update the test).
        hdbg.dassert_is_subset(index, market_data_df["end_datetime"].to_list())
        columns = [asset_id]
        prediction_data = [
            [1],
            [-1],
            [1],
            [-1],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        return predictions, volatility

    @staticmethod
    def _get_predictions_and_volatility2(
        market_data_df,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate a signal that is only long.
        """
        # Build predictions.
        asset_id = market_data_df["asset_id"][0]
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
        ]
        # Sanity check the index (e.g., in case we update the test).
        hdbg.dassert_is_subset(index, market_data_df["end_datetime"].to_list())
        columns = [asset_id]
        prediction_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        predictions = pd.DataFrame(prediction_data, index, columns)
        volatility_data = [
            [1],
            [1],
            [1],
            [1],
        ]
        volatility = pd.DataFrame(volatility_data, index, columns)
        return predictions, volatility

    async def _test_mocked_system1(
        self,
        predictions: pd.DataFrame,
        volatility: pd.DataFrame,
        portfolio: omportfo.MockedPortfolio,
    ) -> None:
        """
        Run process_forecasts() logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        config["order_type"] = "price@twap"
        config["order_duration"] = 5
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        config["execution_mode"] = "batch"
        # Run.
        await oprofore.process_forecasts(
            predictions,
            volatility,
            portfolio,
            config,
        )
        #
        asset_ids = portfolio.universe
        hdbg.dassert_eq(len(asset_ids), 1)
        asset_id = asset_ids[0]
        price = portfolio.market_data.get_data_for_interval(
            pd.Timestamp("2000-01-01 09:30:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
            ts_col_name="timestamp_db",
            asset_ids=asset_ids,
            left_close=True,
            right_close=True,
        )["price"]
        #
        twap = csigproc.resample(price, rule="5T").mean().rename("twap")
        rets = twap.pct_change().rename("rets")
        predictions_srs = predictions[asset_id].rename("prediction")
        research_pnl = (
            predictions_srs.shift(2).multiply(rets).rename("research_pnl")
        )
        #
        actual = []
        self._append(actual, "TWAP", twap)
        self._append(actual, "rets", rets)
        self._append(actual, "prediction", predictions_srs)
        self._append(actual, "research_pnl", research_pnl)
        actual.append(portfolio)
        actual = "\n".join(map(str, actual))
        self.check_string(actual)

    @staticmethod
    def _append(
        list_: List[str], label: str, data: Union[pd.Series, pd.DataFrame]
    ) -> None:
        data_str = hunitest.convert_df_to_string(data, index=True, decimals=3)
        list_.append(f"{label}=\n{data_str}")
