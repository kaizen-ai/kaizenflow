import io
import logging
from typing import Any, Dict

import pandas as pd

import core.real_time as creatime
import helpers.hasyncio as hasynci
import helpers.printing as hprint
import helpers.sql as hsql
import helpers.unit_test as hunitest
import market_data.market_data_interface as mdmadain
import market_data.market_data_interface_example as mdmdinex
import oms.broker_example as obroexam
import oms.oms_db as oomsdb
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)

_5mins = pd.DateOffset(minutes=5)


# #############################################################################


class TestSimulatedPortfolio1(hunitest.TestCase):
    def test_get_holdings1(self) -> None:
        """
        Check non-cash holdings for a Portfolio with only cash.
        """
        expected = r"""
        Empty DataFrame
        Columns: [asset_id, curr_num_shares]
        Index: []"""
        timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        asset_id = None
        exclude_cash = True
        self._test_get_holdings(
            expected, timestamp, asset_id, exclude_cash=exclude_cash
        )

    def test_get_holdings2(self) -> None:
        """
        Check holdings for a Portfolio with only cash.
        """
        expected = r"""
                                   asset_id  curr_num_shares
        2000-01-01 09:35:00-05:00      -1.0        1000000.0"""
        timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        asset_id = None
        exclude_cash = False
        self._test_get_holdings(
            expected, timestamp, asset_id, exclude_cash=exclude_cash
        )

    def test_get_holdings3(self) -> None:
        """
        Check holdings after the last timestamp, which returns an empty df.
        """
        expected = r"""
        Empty DataFrame
        Columns: [asset_id, curr_num_shares]
        Index: []"""
        timestamp = pd.Timestamp("2000-01-01 09:40:00-05:00")
        asset_id = None
        exclude_cash = False
        self._test_get_holdings(
            expected, timestamp, asset_id, exclude_cash=exclude_cash
        )

    def _get_portfolio1(self):
        """
        Return a freshly minted Portfolio with only cash.
        """
        # Build a ReplayedTimeMarketDataInterface.
        event_loop = None
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        # Build a Portfolio.
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            initial_timestamp,
            market_data_interface=market_data_interface,
        )
        return portfolio

    def _test_get_holdings(
        self, expected: str, *args: Any, **kwargs: Dict[str, Any]
    ) -> None:
        portfolio = self._get_portfolio1()
        # Run.
        holdings = portfolio.get_holdings(*args, **kwargs)
        # Check.
        self.assert_equal(str(holdings), expected, fuzzy_match=True)


# #############################################################################


class TestSimulatedPortfolio2(hunitest.TestCase):
    def test_initialization_with_cash1(self) -> None:
        """
        Initialize a Portfolio with cash.
        """
        # Build ReplayedTimeMarketDataInterface.
        event_loop = None
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        # Build Portfolio.
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            initial_timestamp,
            market_data_interface=market_data_interface,
        )
        # Check.
        txt = r"""
,asset_id,curr_num_shares
2000-01-01 09:35:00-05:00,-1.0,1000000.0"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
            parse_dates=True,
        )
        self.assert_dfs_close(portfolio.holdings, expected)

    def test_initialization_with_holdings1(self) -> None:
        """
        Initialize a Portfolio with holdings.
        """
        # Build ReplayedTimeMarketDataInterface.
        event_loop = None
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        # Build Broker.
        broker = obroexam.get_simulated_broker_example1(
            event_loop, market_data_interface=market_data_interface
        )
        # Build Portfolio.
        strategy_id = "str1"
        account = "paper"
        market_data_interface = market_data_interface
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        holdings_dict = {101: 727.5, 202: 1040.3, -1: 10000}
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.SimulatedPortfolio.from_dict(
            strategy_id,
            account,
            market_data_interface,
            get_wall_clock_time,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            broker=broker,
            holdings_dict=holdings_dict,
            initial_timestamp=initial_timestamp,
        )
        # Check.
        txt = r"""
,asset_id,curr_num_shares
2000-01-01 09:35:00-05:00,101,727.5
2000-01-01 09:35:00-05:00,202,1040.3
2000-01-01 09:35:00-05:00,-1.0,10000.0"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
            parse_dates=True,
        )
        self.assert_dfs_close(portfolio.holdings, expected)

    def test_characteristics1(self) -> None:
        # Build MarketDataInterface.
        event_loop = None
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        #
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            initial_timestamp,
            market_data_interface=market_data_interface,
        )
        # Check.
        txt = r"""
,2000-01-01 09:35:00-05:00
net_asset_holdings,0
cash,1000000.0
net_wealth,1000000.0
gross_exposure,0.0
leverage,0.0
"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
        )
        # The timestamp doesn't parse correctly from the CSV.
        expected.columns = [initial_timestamp]
        actual = portfolio.get_characteristics(initial_timestamp)
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)

    def test_characteristics2(self) -> None:
        # Build MarketDataInterface.
        event_loop = None
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example3(event_loop)
        # Build Broker.
        broker = obroexam.get_simulated_broker_example1(
            event_loop, market_data_interface=market_data_interface
        )
        # Build Portfolio.
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        holdings_dict = {101: 727.5, 202: 1040.3, -1: 10000}
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.SimulatedPortfolio.from_dict(
            strategy_id,
            account,
            market_data_interface,
            get_wall_clock_time,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            broker=broker,
            holdings_dict=holdings_dict,
            initial_timestamp=initial_timestamp,
        )
        txt = r"""
,2000-01-01 09:35:00-05:00i
net_asset_holdings,1768351.42
cash,10000.0
net_wealth,1778351.42
gross_exposure,1768351.42
leverage,0.994
"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
        )
        # The timestamp doesn't parse correctly from the CSV.
        expected.columns = [initial_timestamp]
        actual = portfolio.get_characteristics(initial_timestamp)
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)

    def test_characteristics3(self) -> None:
        # Build MarketDataInterface.
        tz = "ET"
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        event_loop = None
        get_wall_clock_time = creatime.get_replayed_wall_clock_time(
            tz,
            initial_timestamp,
            event_loop=event_loop,
        )
        price_txt = r"""
start_datetime,end_datetime,asset_id,price
2000-01-01 09:30:00-05:00,2000-01-01 09:35:00-05:00,100,100.34
"""
        price_df = pd.read_csv(
            io.StringIO(price_txt),
            parse_dates=["start_datetime", "end_datetime"],
        )
        start_time_col_name = "start_datetime"
        end_time_col_name = "end_datetime"
        knowledge_datetime_col_name = "end_datetime"
        delay_in_secs = 0
        asset_id_col_name = "asset_id"
        asset_ids = None
        columns = []
        market_data_interface = mdmadain.ReplayedTimeMarketDataInterface(
            price_df,
            knowledge_datetime_col_name,
            delay_in_secs,
            asset_id_col_name,
            asset_ids,
            start_time_col_name,
            end_time_col_name,
            columns,
            get_wall_clock_time,
        )
        portfolio = oporexam.get_simulated_portfolio_example1(
            event_loop,
            initial_timestamp,
            market_data_interface=market_data_interface,
        )
        # Check.
        txt = r"""
,2000-01-01 09:35:00-05:00
net_asset_holdings,0
cash,1000000.0
net_wealth,1000000.0
gross_exposure,0.0
leverage,0.0
"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
        )
        # The timestamp doesn't parse correctly from the CSV.
        expected.columns = [initial_timestamp]
        actual = portfolio.get_characteristics(initial_timestamp)
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)


# #############################################################################


def _get_row1() -> pd.Series:
    row = """
    strategyid,SAU1
    account,candidate
    id,0
    tradedate,2000-01-01
    timestamp_db,2000-01-01 21:38:39.419536
    asset_id,101
    target_position,10
    current_position,20.0
    open_quantity,0
    net_cost,0
    bod_position,0
    bod_price,0
    """
    srs = hsql.csv_to_series(row, sep=",")
    return srs


def _get_row2() -> pd.Series:
    row = """
    strategyid,SAU1
    account,candidate
    id,0
    tradedate,2000-01-01
    timestamp_db,2000-01-01 21:38:39.419536
    asset_id,101
    target_position,10
    current_position,20.0
    open_quantity,0
    net_cost,1903.1217
    bod_position,0
    bod_price,0
    """
    srs = hsql.csv_to_series(row, sep=",")
    return srs


class TestMockedPortfolio1(omtodh.TestOmsDbHelper):
    def test1(self) -> None:
        """
        Test that the update of Portfolio works.
        """
        with hasynci.solipsism_context() as event_loop:
            # Create current positions in the table.
            row = _get_row1()
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_current_positions_table(
                self.connection, incremental=False, table_name=table_name
            )
            hsql.execute_insert_query(self.connection, row, table_name)
            if False:
                # Print the DB status.
                query = """SELECT * FROM current_positions"""
                df = hsql.execute_query_to_df(self.connection, query)
                print(hprint.dataframe_to_str(df))
                assert 0
            #
            # Create MockedPortfolio with some initial cash.
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop, self.connection, table_name, initial_timestamp
            )
            portfolio.update_state()
            # Check.
            actual = str(portfolio)
            expected = r"""# holdings=
                                       asset_id  curr_num_shares
            2000-01-01 09:35:00-05:00       101               20
            2000-01-01 09:35:00-05:00        -1          1000000
            2000-01-01 09:30:00-05:00        -1          1000000"""
            self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test that the update of Portfolio works when accounting for costs.
        """
        with hasynci.solipsism_context() as event_loop:
            # Create current positions in the table.
            row = _get_row2()
            table_name = oomsdb.CURRENT_POSITIONS_TABLE_NAME
            oomsdb.create_current_positions_table(
                self.connection, incremental=False, table_name=table_name
            )
            hsql.execute_insert_query(self.connection, row, table_name)
            if False:
                # Print the DB status.
                query = """SELECT * FROM current_positions"""
                df = hsql.execute_query_to_df(self.connection, query)
                print(hprint.dataframe_to_str(df))
                assert 0
            #
            # Create MockedPortfolio with some initial cash.
            initial_timestamp = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            portfolio = oporexam.get_mocked_portfolio_example1(
                event_loop, self.connection, table_name, initial_timestamp
            )
            portfolio.update_state()
            # Check.
            actual = str(portfolio)
            expected = r"""# holdings=
                                       asset_id  curr_num_shares
            2000-01-01 09:35:00-05:00       101          20.0
            2000-01-01 09:35:00-05:00        -1      998096.8783
            2000-01-01 09:30:00-05:00        -1     1000000.0"""
            self.assert_equal(actual, expected, fuzzy_match=True)
