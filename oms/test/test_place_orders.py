import asyncio
import datetime
import io
import logging

import pandas as pd

import core.config as cconfig
import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest
import market_data.market_data_interface_example as mdmdinex
import oms.broker_example as obroexam
import oms.mr_market as omrmark
import oms.oms_db as oomsdb
import oms.place_orders as oplaorde
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)


class TestMarkToMarket1(hunitest.TestCase):
    def test1(self) -> None:
        # Set up price interface components.
        event_loop = None
        db_txt = """
start_datetime,end_datetime,timestamp_db,price,asset_id
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,107.73,101
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,93.25,202
"""
        df = pd.read_csv(
            io.StringIO(db_txt),
            parse_dates=["start_datetime", "end_datetime", "timestamp_db"],
        )
        # Build a ReplayedTimeMarketDataInterface.
        initial_replayed_delay = 5
        delay_in_secs = 0
        sleep_in_secs = 30
        time_out_in_secs = 60 * 5
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        start_datetime = initial_timestamp
        end_datetime = pd.Timestamp("2000-01-01 09:35:00-05:00")
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example1(
            event_loop,
            initial_replayed_delay,
            df,
            delay_in_secs=delay_in_secs,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs,
        )
        # Build a Broker.
        broker = obroexam.get_simulated_broker_example1(
            event_loop, market_data_interface=market_data_interface
        )
        # Initialize portfolio.
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        portfolio = omportfo.SimulatedPortfolio.from_cash(
            strategy_id,
            account,
            market_data_interface,
            get_wall_clock_time,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            broker=broker,
            initial_cash=1e6,
            initial_timestamp=initial_timestamp,
        )
        # Initialize a prediction series.
        predictions = pd.Series(
            index=[101, 202], data=[0.3, -0.1], name="prediction"
        )
        # Mark to market.
        actual = oplaorde._mark_to_market(
            initial_timestamp, predictions, portfolio
        )
        txt = r"""
asset_id,curr_num_shares,prediction,price,value
-1,1000000,0,1,1000000
101,0.0,0.3,107.73,0.0
202,0.0,-0.1,93.25,0.0
"""
        expected = pd.read_csv(
            io.StringIO(txt),
        )
        # Set `asset_id` as index so numpy will type the (remaining) values
        # correctly.
        self.assert_dfs_close(
            actual.set_index("asset_id"),
            expected.set_index("asset_id"),
            rtol=1e-5,
            atol=1e-5,
        )


class TestOptimizeAndUpdate1(hunitest.TestCase):
    def test1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(self._test_coroutine(event_loop), event_loop=event_loop)

    async def _test_coroutine(self, event_loop):
        # Get price data.
        db_txt = """
start_datetime,end_datetime,timestamp_db,price,asset_id
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,107.73,101
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,2000-01-01 09:35:00-05:00,93.25,202
2000-01-01 09:35:00-05:00,2000-01-01 09:40:00-05:00,2000-01-01 09:40:00-05:00,108.73,101
2000-01-01 09:35:00-05:00,2000-01-01 09:40:00-05:00,2000-01-01 09:40:00-05:00,93.13,202
"""
        db_df = pd.read_csv(
            io.StringIO(db_txt),
            parse_dates=["start_datetime", "end_datetime", "timestamp_db"],
        )
        db_df["start_datetime"] = db_df["start_datetime"].dt.tz_convert(
            "America/New_York"
        )
        db_df["end_datetime"] = db_df["end_datetime"].dt.tz_convert(
            "America/New_York"
        )
        db_df["timestamp_db"] = db_df["timestamp_db"].dt.tz_convert(
            "America/New_York"
        )
        # Build a ReplayedTimeMarketDataInterface.
        initial_replayed_delay = 5
        delay_in_secs = 0
        sleep_in_secs = 30
        time_out_in_secs = 60 * 5
        initial_timestamp = pd.Timestamp(
            "2000-01-01 09:35:00-05:00", tz="America/New_York"
        )
        # TODO(gp): These params are not used.
        start_datetime = initial_timestamp
        end_datetime = pd.Timestamp(
            "2000-01-01 09:40:00-05:00", tz="America/New_York"
        )
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example1(
            event_loop,
            initial_replayed_delay,
            df=db_df,
            delay_in_secs=delay_in_secs,
            sleep_in_secs=sleep_in_secs,
            time_out_in_secs=time_out_in_secs,
        )
        # Build a Broker.
        broker = obroexam.get_simulated_broker_example1(
            event_loop, market_data_interface=market_data_interface
        )
        # Initialize Portfolio.
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        portfolio = omportfo.SimulatedPortfolio.from_cash(
            strategy_id,
            account,
            market_data_interface,
            get_wall_clock_time,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            broker=broker,
            initial_cash=1e6,
            initial_timestamp=initial_timestamp,
        )
        # Initialize a prediction series.
        predictions = pd.Series(
            index=[101, 202], data=[0.3, -0.1], name="prediction"
        )
        # Set up order config for 9:35 to 9:40.
        end_timestamp = pd.Timestamp(
            "2000-01-01 09:40:00-05:00", tz="America/New_York"
        )
        order_dict_ = {
            "type_": "price@twap",
            "creation_timestamp": initial_timestamp,
            "start_timestamp": initial_timestamp,
            "end_timestamp": end_timestamp,
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        # Compute target positions.
        target_positions = oplaorde._compute_target_positions_in_shares(
            initial_timestamp, predictions, portfolio
        )
        orders = oplaorde._generate_orders(
            target_positions["diff_num_shares"], order_config
        )
        # Submit orders.
        await broker.submit_orders(orders)
        # Wait 5 minutes.
        await asyncio.sleep(60 * 5)
        portfolio.update_state()
        actual = portfolio.get_characteristics(
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York")
        )
        # Check.
        txt = r"""
,2000-01-01 09:40:00-05:00
net_asset_holdings,50728.36
cash,949271.64
net_wealth,1000000.00
gross_exposure,100664.01
leverage,0.1007
"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
        )
        # The timestamp doesn't parse correctly from the CSV.
        expected.columns = [
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York")
        ]
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)


class TestPlaceOrders1(hunitest.TestCase):
    def test_initialization1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            hasynci.run(self._test_coroutine1(event_loop), event_loop=event_loop)

    async def _test_coroutine1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        config = {}
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:35:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(data, index=index, columns=columns)
        # Build a Portfolio.
        initial_timestamp = pd.Timestamp(
            "2000-01-01 09:30:00-05:00", tz="America/New_York"
        )
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            initial_timestamp,
            market_data_interface=market_data_interface,
        )
        config["market_data_interface"] = market_data_interface
        config["portfolio"] = portfolio
        config["broker"] = portfolio.broker
        config["order_type"] = "price@twap"
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        # Run.
        execution_mode = "batch"
        await oplaorde.place_orders(
            predictions,
            execution_mode,
            config,
        )
        # TODO(Paul): Add a check of the output.


class TestMockedPlaceOrders1(omtodh.TestOmsDbHelper):
    def test_mocked_system1(self) -> None:
        with hasynci.solipsism_context() as event_loop:
            # Build a Portfolio.
            db_connection = self.connection
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            # TODO(gp): Factor out in a single function.
            oomsdb.create_accepted_orders_table(
                self.connection, incremental=False
            )
            oomsdb.create_submitted_orders_table(
                self.connection, incremental=False
            )
            oomsdb.create_current_positions_table(
                self.connection, incremental=False, table_name=table_name
            )
            #
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop,
                db_connection,
                table_name,
                initial_timestamp,
            )
            # Build OrderProcessor.
            get_wall_clock_time = portfolio._get_wall_clock_time
            poll_kwargs = hasynci.get_poll_kwargs(get_wall_clock_time)
            # poll_kwargs["sleep_in_secs"] = 1
            poll_kwargs["timeout_in_secs"] = 60 * 10
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            broker = portfolio.broker
            end_timestamp = pd.Timestamp("2000-01-01 09:50:00-05:00")
            order_processor = omrmark.order_processor(
                db_connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                end_timestamp,
                poll_kwargs=poll_kwargs,
            )
            coroutines = [self._test_mocked_system1(portfolio), order_processor]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def _test_mocked_system1(
        self,
        portfolio,
    ) -> None:
        """
        Run place_orders() logic with a given prediction df to update a
        Portfolio.
        """
        config = {}
        # Build predictions.
        index = [
            pd.Timestamp("2000-01-01 09:40:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:45:00-05:00", tz="America/New_York"),
            pd.Timestamp("2000-01-01 09:50:00-05:00", tz="America/New_York"),
        ]
        columns = [101, 202]
        data = [
            [0.1, 0.2],
            [-0.1, 0.3],
            [-0.3, 0.0],
        ]
        predictions = pd.DataFrame(data, index=index, columns=columns)
        # TODO(gp): Remove mdi and broker since they are passed through Portfolio.
        config["market_data_interface"] = portfolio._market_data_interface
        config["portfolio"] = portfolio
        config["broker"] = portfolio.broker
        config["order_type"] = "price@twap"
        config["ath_start_time"] = datetime.time(9, 30)
        config["trading_start_time"] = datetime.time(9, 35)
        config["ath_end_time"] = datetime.time(16, 00)
        config["trading_end_time"] = datetime.time(15, 55)
        # Run.
        execution_mode = "batch"
        await oplaorde.place_orders(
            predictions,
            execution_mode,
            config,
        )
        # TODO(Paul): Add a check of the output.
