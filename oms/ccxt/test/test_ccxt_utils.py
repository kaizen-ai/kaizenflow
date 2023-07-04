import logging
import pprint
from typing import List

import numpy as np
import pandas as pd

import helpers.hunit_test as hunitest
import oms.ccxt.ccxt_utils as occccuti
import oms.order as omorder

_LOG = logging.getLogger(__name__)


# #############################################################################
# Prepare data for tests
# #############################################################################


def _get_test_order() -> List[omorder.Order]:
    """
    Build toy list of 1 order for tests.
    """
    # Prepare test data.
    order_str = "Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
    asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
    end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121\
    tz=America/New_York extra_params={}"
    # Get orders.
    orders = omorder.orders_from_string(order_str)
    return orders


def _get_test_order_fills1() -> List[omorder.Order]:
    """
    Get CCXT order structures for child orders associated with
    `_get_test_order`.
    """
    fills = [
        {
            "info": {
                "orderId": "8389765589152377439",
                "symbol": "ETHUSDT",
                "status": "FILLED",
                "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
                "price": "1780.0",
                "avgPrice": "1780.70000",
                "origQty": "0.06",
                "executedQty": "0.06",
                "cumQty": "0.06",
                "cumQuote": "48.07890",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": "",
                "closePosition": "",
                "side": "BUY",
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": "",
                "origType": "LIMIT",
                "updateTime": "1680207361629",
            },
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
            "price": 1780.0,
            "stopPrice": "",
            "amount": 0.06,
            "cost": 106.8,
            "average": 1780.7,
            "filled": 0.06,
            "remaining": 0.0,
            "status": "closed",
            "fee": "",
            "trades": [],
            "fees": [],
        },
        {
            "info": {
                "orderId": "8389765589152377439",
                "symbol": "ETHUSDT",
                "status": "FILLED",
                "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
                "price": "1770.10",
                "avgPrice": "1770.70000",
                "origQty": "0.06",
                "executedQty": "0.06",
                "cumQty": "0.06",
                "cumQuote": "48.07890",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": "",
                "closePosition": "",
                "side": "BUY",
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": "",
                "origType": "LIMIT",
                "updateTime": "1680207361629",
            },
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
            "price": 1770.0,
            "stopPrice": "",
            "amount": 0.06,
            "cost": 106.2,
            "average": 1770.7,
            "filled": 0.06,
            "remaining": 0.0,
            "status": "closed",
            "fee": "",
            "trades": [],
            "fees": [],
        },
    ]
    return fills


def _get_test_bid_ask_data() -> pd.DataFrame:
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
        # pylint: disable=line-too-long
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
    def tests1(self) -> None:
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
        ) = occccuti.roll_up_child_order_fills_into_parent(
            parent_order, child_order_fills
        )
        self.assertEqual(fill_amount, 0.12)
        self.assertEqual(fill_price, 1775.0)

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
        ) = occccuti.roll_up_child_order_fills_into_parent(
            parent_order, child_order_fills
        )
        self.assertEqual(fill_amount, 0)
        self.assertTrue(np.isnan(fill_price))


class Test_calculate_limit_price(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test limit price calculation using side="buy".
        """
        data = _get_test_bid_ask_data()
        asset_id = 1464553467
        side = "buy"
        passivity_factor = 0.1
        max_deviation = 0.01
        price_dict = occccuti.calculate_limit_price(
            data, asset_id, side, passivity_factor, max_deviation
        )
        actual = pprint.pformat(price_dict)
        expected = r"""
        {'ask_price_mean': 60.5,
        'bid_price_mean': 50.5,
        'latest_ask_price': 61,
        'latest_bid_price': 51,
        'limit_price': 60.0,
        'passivity_factor': 0.1,
        'used_ask_price': 'latest_ask_price',
        'used_bid_price': 'latest_bid_price'}
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test limit price calculation using side="sell".
        """
        data = _get_test_bid_ask_data()
        asset_id = 1464553467
        side = "sell"
        passivity_factor = 0.5
        max_deviation = 0.01
        price_dict = occccuti.calculate_limit_price(
            data, asset_id, side, passivity_factor, max_deviation
        )
        actual = pprint.pformat(price_dict)
        expected = r"""
        {'ask_price_mean': 60.5,
        'bid_price_mean': 50.5,
        'latest_ask_price': 61,
        'latest_bid_price': 51,
        'limit_price': 56.0,
        'passivity_factor': 0.5,
        'used_ask_price': 'latest_ask_price',
        'used_bid_price': 'latest_bid_price'}
        """
        self.assert_equal(actual, expected, fuzzy_match=True)
