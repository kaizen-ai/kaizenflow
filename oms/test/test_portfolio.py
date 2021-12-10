"""
Import as:

import oms.test.test_portfolio as ottport
"""

import io
import logging
from typing import Any, Dict

import pandas as pd

import core.real_time as creatime
import helpers.unit_test as hunitest
import market_data.market_data_interface as mdmadain
import market_data.market_data_interface_example as mdmdinex
import oms.broker as ombroker
import oms.portfolio as omportfo
import oms.portfolio_example as oporexam

_LOG = logging.getLogger(__name__)

_5mins = pd.DateOffset(minutes=5)


class TestPortfolio1(hunitest.TestCase):
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
        # Build a ReplayedTimePriceInterface.
        event_loop = None
        (
            market_data_interface,
            _,
        ) = mdmdinex.get_replayed_time_market_data_interface_example2(
            event_loop
        )
        # Build a Portfolio.
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = oporexam.get_portfolio_example1(
            market_data_interface, initial_timestamp
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


class TestPortfolio2(hunitest.TestCase):
    def test_initialization1(self) -> None:
        # Build a ReplayedTimePriceInterface.
        event_loop = None
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example2(
            event_loop
        )
        # Build Broker.
        broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
        # Build a Portfolio.
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        initial_cash = 1e6
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.Portfolio.from_cash(
            strategy_id,
            account,
            market_data_interface,
            get_wall_clock_time,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            broker=broker,
            initial_cash=initial_cash,
            initial_timestamp=initial_timestamp,
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

    def test_initialization2(self) -> None:
        # Build a ReplayedTimePriceInterface.
        event_loop = None
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example2(
            event_loop
        )
        # Build Broker.
        broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
        # Build a Portfolio.
        strategy_id = "str1"
        account = "paper"
        market_data_interface = market_data_interface
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        holdings_dict = {101: 727.5, 202: 1040.3, -1: 10000}
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.Portfolio.from_dict(
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
        # Build PriceInterface.
        event_loop = None
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example2(
            event_loop
        )
        # Build Broker.
        broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
        # Build Portfolio.
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        initial_cash = 1e6
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.Portfolio.from_cash(
            strategy_id,
            account,
            market_data_interface,
            get_wall_clock_time,
            asset_id_col,
            mark_to_market_col,
            timestamp_col,
            broker=broker,
            initial_cash=initial_cash,
            initial_timestamp=initial_timestamp,
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
        # Build PriceInterface.
        event_loop = None
        (
            market_data_interface,
            get_wall_clock_time,
        ) = mdmdinex.get_replayed_time_market_data_interface_example2(
            event_loop
        )
        # Build Broker.
        broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
        # Build Portfolio.
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        holdings_dict = {101: 727.5, 202: 1040.3, -1: 10000}
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        portfolio = omportfo.Portfolio.from_dict(
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
        # Build PriceInterface.
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
        market_data_interface = mdmadain.ReplayedTimeMarketDataInterface(
            price_df,
            start_time_col_name="start_datetime",
            end_time_col_name="end_datetime",
            knowledge_datetime_col_name="end_datetime",
            delay_in_secs=0,
            id_col_name="asset_id",
            ids=None,
            columns=[],
            get_wall_clock_time=get_wall_clock_time,
        )
        # Build Broker.
        broker = ombroker.Broker(market_data_interface, get_wall_clock_time)
        # Build Portfolio.
        strategy_id = "str1"
        account = "paper"
        asset_id_col = "asset_id"
        mark_to_market_col = "price"
        timestamp_col = "end_datetime"
        portfolio = omportfo.Portfolio.from_cash(
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
