"""
Import as:

import oms.test.test_portfolio as ottport
"""

import io
import logging
from typing import Any, Dict

import pandas as pd

import core.dataflow.price_interface as cdtfprint
import core.dataflow.test.test_price_interface as dartttdi
import helpers.printing as hprintin
import helpers.unit_test as huntes
import oms.order as oord
import oms.portfolio as opor

_LOG = logging.getLogger(__name__)


def get_portfolio_example1(
    price_interface: cdtfprint.AbstractPriceInterface,
    initial_timestamp: pd.Timestamp,
):
    strategy_id = "st1"
    account = "paper"
    asset_id_column = "asset_id"
    # price_column = "midpoint"
    price_column = "price"
    #
    initial_cash = 1e6
    portfolio = omportfo.Portfolio.from_cash(
        strategy_id,
        account,
        #
        price_interface,
        asset_id_column,
        price_column,
        #
        initial_cash,
        initial_timestamp,
    )
    return portfolio


def get_replayed_time_price_interface(event_loop):
    start_datetime = pd.Timestamp("2000-01-01 09:30:00-05:00")
    end_datetime = pd.Timestamp("2000-01-01 10:30:00-05:00")
    columns_ = ["price"]
    asset_ids = [101, 202]
    # asset_ids = [1000]
    df = dartttdi.generate_synthetic_db_data(
        start_datetime, end_datetime, columns_, asset_ids
    )
    _LOG.debug("df=%s", hprintin.dataframe_to_str(df))
    # Build a ReplayedTimePriceInterface.
    initial_replayed_delay = 5
    delay_in_secs = 0
    sleep_in_secs = 30
    time_out_in_secs = 60 * 5
    price_interface = dartttdi.get_replayed_time_price_interface_example1(
        event_loop,
        start_datetime,
        end_datetime,
        initial_replayed_delay,
        delay_in_secs,
        df=df,
        sleep_in_secs=sleep_in_secs,
        time_out_in_secs=time_out_in_secs,
    )
    return price_interface


_5mins = pd.DateOffset(minutes=5)


class TestPortfolio1(huntes.TestCase):
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

    def test_place_orders1(self) -> None:
        order_id = 0
        # Build a ReplayedTimePriceInterface.
        event_loop = None
        price_interface = get_replayed_time_price_interface(event_loop)
        # Get order.
        timestamp = pd.Timestamp("2000-01-01 09:30:00-05:00")
        creation_timestamp = timestamp + _5mins
        asset_id = 101
        type_ = "price@twap"
        timestamp_start = timestamp + _5mins
        timestamp_end = timestamp + 2 * _5mins
        num_shares = 10
        order = oord.Order(
            order_id,
            price_interface,
            creation_timestamp,
            asset_id,
            type_,
            timestamp_start,
            timestamp_end,
            num_shares,
        )
        orders = [order]
        # Build a Portfolio.
        initial_timestamp = timestamp
        portfolio = get_portfolio_example1(price_interface, initial_timestamp)
        # Execute.
        try:
            # Since there is no simulated time, we need to enable future peeking.
            old_value = price_interface.set_allow_future_peeking(True)
            portfolio.process_filled_orders(
                timestamp_start, timestamp_end, orders
            )
        finally:
            price_interface.set_allow_future_peeking(old_value)
        # Check.
        act = str(portfolio)
        exp = r"""# holdings=
                                   asset_id  curr_num_shares
        2000-01-01 09:40:00-05:00       101             10.0
        2000-01-01 09:30:00-05:00        -1        1000000.0
        # orders=
                                  order_id               creation_timestamp asset_id       type_                  start_timestamp                    end_timestamp num_shares num_shares_filled holdings+1  execution_price        cash+1
        2000-01-01 09:35:00-05:00        0        2000-01-01 09:35:00-05:00      101  price@twap        2000-01-01 09:35:00-05:00        2000-01-01 09:40:00-05:00         10                10         10        -0.083847  1.000001e+06
        2000-01-01 09:30:00-05:00      NaN                              NaT      NaN         NaN                              NaT                              NaT        NaN               NaN        NaN              NaN           NaN"""
        self.assert_equal(act, exp, fuzzy_match=True)

    def _get_portfolio1(self):
        """
        Return a freshly minted Portfolio with only cash.
        """
        # Build a ReplayedTimePriceInterface.
        event_loop = None
        price_interface = get_replayed_time_price_interface(event_loop)
        # Build a Portfolio.
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = get_portfolio_example1(price_interface, initial_timestamp)
        return portfolio

    def _test_get_holdings(
        self, expected: str, *args: Any, **kwargs: Dict[str, Any]
    ) -> None:
        portfolio = self._get_portfolio1()
        # Run.
        holdings = portfolio.get_holdings(*args, **kwargs)
        # Check.
        self.assert_equal(str(holdings), expected, fuzzy_match=True)


class TestPortfolio2(hunitest.TestCase):
    def test_initialization1(self) -> None:
        event_loop = None
        price_interface = get_replayed_time_price_interface(event_loop)
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.Portfolio.from_cash(
            strategy_id="str1",
            account="paper",
            price_interface=price_interface,
            asset_id_column="asset_id",
            price_column="price",
            initial_cash=1e6,
            initial_timestamp=initial_timestamp,
        )
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
        event_loop = None
        price_interface = get_replayed_time_price_interface(event_loop)
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        dict_ = {101: 727.5, 202: 1040.3, -1: 10000}
        portfolio = omportfo.Portfolio.from_dict(
            strategy_id="str1",
            account="paper",
            price_interface=price_interface,
            asset_id_column="asset_id",
            price_column="price",
            holdings_dict=dict_,
            initial_timestamp=initial_timestamp,
        )
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
        event_loop = None
        price_interface = get_replayed_time_price_interface(event_loop)
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        portfolio = omportfo.Portfolio.from_cash(
            strategy_id="str1",
            account="paper",
            price_interface=price_interface,
            asset_id_column="asset_id",
            price_column="price",
            initial_cash=1e6,
            initial_timestamp=initial_timestamp,
        )
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
        # The timestamp doesn't parse correctly from the csv.
        expected.columns = [initial_timestamp]
        actual = portfolio.get_characteristics(initial_timestamp)
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)

    def test_characteristics2(self) -> None:
        event_loop = None
        price_interface = get_replayed_time_price_interface(event_loop)
        initial_timestamp = pd.Timestamp("2000-01-01 09:35:00-05:00")
        dict_ = {101: 727.5, 202: 1040.3, -1: 10000}
        portfolio = omportfo.Portfolio.from_dict(
            strategy_id="str1",
            account="paper",
            price_interface=price_interface,
            asset_id_column="asset_id",
            price_column="price",
            holdings_dict=dict_,
            initial_timestamp=initial_timestamp,
        )
        txt = r"""
,2000-01-01 09:35:00-05:00
net_asset_holdings,551.422
cash,10000.0
net_wealth,10551.422
gross_exposure,551.422
leverage,0.052
"""
        expected = pd.read_csv(
            io.StringIO(txt),
            index_col=0,
        )
        # The timestamp doesn't parse correctly from the csv.
        expected.columns = [initial_timestamp]
        actual = portfolio.get_characteristics(initial_timestamp)
        self.assert_dfs_close(actual.to_frame(), expected, rtol=1e-2, atol=1e-2)
