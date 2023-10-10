import logging
from typing import List

import numpy as np
import pandas as pd

import helpers.hunit_test as hunitest
import oms.broker.ccxt.ccxt_utils as obccccut
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)


# #############################################################################
# Prepare data for tests
# #############################################################################


def _get_test_order() -> List[oordorde.Order]:
    """
    Build toy list of 1 order for tests.
    """
    # Prepare test data.
    order_str = "Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
    asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
    end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121\
    tz=America/New_York extra_params={}"
    # Get orders.
    orders = oordorde.orders_from_string(order_str)
    return orders


def _get_test_order_fills1() -> List[oordorde.Order]:
    """
    Get CCXT order structures for child orders associated with
    `_get_test_order` on the "buy" side.

    `info` field is not included to make fills more readable.
    """
    fills = [
        {
            "id": "8389765589152377439",
            "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
            "timestamp": "",
            "datetime": "",
            "lastTradeTimestamp": "",
            "symbol": "ETH/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": "",
            "reduceOnly": "",
            "side": "buy",
            "price": 1783.455,
            "stopPrice": "",
            "amount": 0.14,
            "cost": 249.6837,
            "average": 1783.455,
            "filled": 0.14,
            "remaining": 0.0,
            "status": "closed",
            "fee": "",
            "trades": [],
            "fees": [],
        },
        {
            "id": "8389765589152377439",
            "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
            "timestamp": "",
            "datetime": "",
            "lastTradeTimestamp": "",
            "symbol": "ETH/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": "",
            "reduceOnly": "",
            "side": "buy",
            "price": 1800.0,
            "stopPrice": "",
            "amount": 0.14,
            "cost": 252.0,
            "average": 1800.0,
            "filled": 0.14,
            "remaining": 0.0,
            "status": "closed",
            "fee": "",
            "trades": [],
            "fees": [],
        },
    ]
    return fills


def _get_test_order_fills2() -> List[oordorde.Order]:
    """
    Get CCXT order structures for child orders associated with
    `_get_test_order` on the "sell" side.

    `info` field is not included to make fills more readable.
    """
    fills = [
        {
            "id": "8389765589152377439",
            "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
            "timestamp": "",
            "datetime": "",
            "lastTradeTimestamp": "",
            "symbol": "ETH/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": "",
            "reduceOnly": "",
            "side": "sell",
            "price": 1783.455,
            "stopPrice": "",
            "amount": 0.14,
            "cost": 249.6837,
            "average": 1783.455,
            "filled": 0.14,
            "remaining": 0.0,
            "status": "closed",
            "fee": "",
            "trades": [],
            "fees": [],
        },
        {
            "id": "8389765589152377439",
            "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
            "timestamp": "",
            "datetime": "",
            "lastTradeTimestamp": "",
            "symbol": "ETH/USDT",
            "type": "limit",
            "timeInForce": "GTC",
            "postOnly": "",
            "reduceOnly": "",
            "side": "sell",
            "price": 1800.0,
            "stopPrice": "",
            "amount": 0.14,
            "cost": 252.0,
            "average": 1800.0,
            "filled": 0.14,
            "remaining": 0.0,
            "status": "closed",
            "fee": "",
            "trades": [],
            "fees": [],
        },
    ]
    return fills


def get_test_bid_ask_data() -> pd.DataFrame:
    """
    Build artificial bid / ask data for the test.
    """
    df = pd.DataFrame(
        columns=[
            "id",
            "timestamp",
            "asset_id",
            "bid_size_l1",
            "ask_size_l1",
            "bid_price_l1",
            "ask_price_l1",
            "full_symbol",
            "end_download_timestamp",
            "knowledge_timestamp",
        ],
        # fmt: off
        data=[
            [
                0,
                pd.Timestamp("2022-08-05 10:35:46-04:00", tz="America/New_York"),
                1464553467,
                30,
                40,
                50,
                60,
                "binance::ETH_USDT",
                pd.Timestamp("2022-08-05 10:35:46-04:00", tz="America/New_York"),
                pd.Timestamp("2022-08-05 10:35:46-04:00", tz="America/New_York")
            ],
            [
                1,
                pd.Timestamp("2022-08-05 10:35:47-04:00", tz="America/New_York"),
                1464553467,
                31,
                41,
                51,
                61,
                "binance::ETH_USDT",
                pd.Timestamp("2022-08-05 10:35:47-04:00", tz="America/New_York"),
                pd.Timestamp("2022-08-05 10:35:47-04:00", tz="America/New_York")
            ],
            [
                2,
                pd.Timestamp("2022-08-05 10:35:46-04:00", tz="America/New_York"),
                1467591036,
                10,
                20,
                30,
                40,
                "binance::BTC_USDT",
                pd.Timestamp("2022-08-05 10:35:46-04:00", tz="America/New_York"),
                pd.Timestamp("2022-08-05 10:35:46-04:00", tz="America/New_York")
            ],
            [
                3,
                pd.Timestamp("2022-08-05 10:35:47-04:00", tz="America/New_York"),
                1467591036,
                12,
                22,
                32,
                42,
                "binance::BTC_USDT",
                pd.Timestamp("2022-08-05 10:35:47-04:00", tz="America/New_York"),
                pd.Timestamp("2022-08-05 10:35:47-04:00", tz="America/New_York")
            ],
        ]
        # pylint: enable=line-too-long
        # fmt: on
    )
    df = df.set_index("timestamp")
    return df


# #############################################################################


class Test_roll_up_child_order_fills_into_parent(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test child order fills aggregation for "buy".
        """
        # Get test order and corresponding fills.
        parent_order = _get_test_order()[0]
        child_order_fills = _get_test_order_fills1()
        # Calculate the fill amount and price.
        (
            fill_amount,
            fill_price,
        ) = obccccut.roll_up_child_order_fills_into_parent(
            parent_order, child_order_fills
        )
        self.assertEqual(fill_amount, 0.28)
        self.assertEqual(fill_price, 1791.7274999999997)

    def test2(self) -> None:
        """
        Test child order fills aggregation for non-filled orders.
        """
        # Get test order and corresponding fills.
        parent_order = _get_test_order()[0]
        child_order_fills = _get_test_order_fills1()
        # Assign empty value to fill amount.
        child_order_fills[0]["filled"] = 0
        child_order_fills[1]["filled"] = 0
        # Calculate the fill amount and price.
        (
            fill_amount,
            fill_price,
        ) = obccccut.roll_up_child_order_fills_into_parent(
            parent_order, child_order_fills
        )
        self.assertEqual(fill_amount, 0)
        self.assertTrue(np.isnan(fill_price))

    def test3(self) -> None:
        """
        Test child order fills aggregation for "sell".
        """
        # Get test order and corresponding fills.
        parent_order = _get_test_order()[0]
        child_order_fills = _get_test_order_fills2()
        # Calculate the fill amount and price.
        (
            fill_amount,
            fill_price,
        ) = obccccut.roll_up_child_order_fills_into_parent(
            parent_order, child_order_fills
        )
        self.assertEqual(fill_amount, -0.28)
        self.assertEqual(fill_price, 1791.7274999999997)

    def test4(self) -> None:
        """
        Test assertion on mixed buy/sell child orders in aggregation.
        """
        parent_order = _get_test_order()[0]
        child_order_buy_fills = _get_test_order_fills1()
        child_order_sell_fills = _get_test_order_fills2()
        child_order_fills = child_order_buy_fills + child_order_sell_fills
        with np.testing.assert_raises(AssertionError):
            obccccut.roll_up_child_order_fills_into_parent(
                parent_order, child_order_fills
            )
    
    def test5(self) -> None:
        """
        Test child order fills aggregation for "sell", with specified rounding.
        """
        # Get test order and corresponding fills.
        parent_order = _get_test_order()[0]
        child_order_fills = _get_test_order_fills2()
        # Calculate the fill amount and price.
        (
            fill_amount,
            fill_price,
        ) = obccccut.roll_up_child_order_fills_into_parent(
            parent_order, child_order_fills, price_quantization=2
        )
        self.assertEqual(fill_amount, -0.28)
        self.assertEqual(fill_price, 1791.73)
