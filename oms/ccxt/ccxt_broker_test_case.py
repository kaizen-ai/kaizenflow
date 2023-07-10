"""
Import as:

import oms.ccxt.ccxt_broker_test_case as occbteca
"""
import abc
import logging
import os
import pprint
import re
import unittest.mock as umock
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import pytest

import helpers.hio as hio
import helpers.hprint as hprint
import helpers.hsystem as hsystem
import helpers.hunit_test as hunitest
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu
import market_data as mdata
import oms.broker as ombroker
import oms.ccxt.abstract_ccxt_broker as ocabccbr
import oms.ccxt.ccxt_broker_v1 as occcbrv1
import oms.hsecrets.secret_identifier as ohsseide
import oms.order as omorder

_LOG = logging.getLogger(__name__)


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
# Generate Fills and Orders
# #############################################################################


def _get_test_order(order_type: str) -> List[omorder.Order]:
    """
    Build toy list of 1 order for tests.
    """
    # Prepare test data.
    order_str = f"Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
    asset_id=1464553467 type_={order_type} start_timestamp=2022-08-05 10:36:44.976104-04:00\
    end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121"
    order_str += " tz=America/New_York extra_params={}"
    # Get orders.
    orders = omorder.orders_from_string(order_str)
    return orders


def _get_child_order_response():
    """
    Corresponds to `orders[0]` from `get_test_orders`.
    """
    child_order_response = {
        "info": {
            "orderId": "7954906695",
            "symbol": "ETHUSDT",
            "status": "NEW",
            "clientOrderId": "x-xcKtGhcub89989e55d47273a3610a9",
            "price": "1780.0",
            "avgPrice": "1780.70000",
            "origQty": "0.06",
            "executedQty": "0.06",
            "cumQty": "0.06",
            "cumQuote": "48.07890",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "reduceOnly": False,
            "closePosition": False,
            "side": "BUY",
            "positionSide": "BOTH",
            "stopPrice": "0",
            "workingType": "CONTRACT_PRICE",
            "priceProtect": False,
            "origType": "LIMIT",
            "updateTime": "1680207361629",
        },
        "id": "7954906695",
        "clientOrderId": "x-xcKtGhcub89989e55d47273a3610a9",
        "timestamp": 1680207361629,
        "datetime": "2023-03-15T16:35:38.582Z",
        "lastTradeTimestamp": None,
        "symbol": "ETH/USDT",
        "type": "limit",
        "timeInForce": "GTC",
        "postOnly": False,
        "reduceOnly": False,
        "side": "buy",
        "price": 1780.0,
        "stopPrice": None,
        "amount": 0.06,
        "cost": 106.8,
        "average": 1780.7,
        "filled": 0.0,
        "remaining": 0.06,
        "status": "open",
        "fee": None,
        "trades": [],
        "fees": [],
    }
    return child_order_response


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


# #############################################################################
# TestCcxtBroker_TestCase
# #############################################################################


class TestCcxtBroker_TestCase(hunitest.TestCase, abc.ABC):
    # Mock calls to external objects (i.e., AWS, CCXT).
    # Since CCXT is imported twice in the abstract class and in
    # child class, it is required to be mocked twice: for uses in abstract
    # class, and for child class uses.
    get_secret_patch = umock.patch.object(ocabccbr.hsecret, "get_secret")
    abstract_ccxt_patch = umock.patch.object(ocabccbr, "ccxt", spec=ocabccbr.ccxt)

    # Ensure we can pass class name to the hunitest.TestCase
    # to obtain the correct path to golden outcome, avoid repetitively
    # copying goldens for multiple child classes.
    @staticmethod
    def get_class_name() -> str:
        return "TestCcxtBroker_TestCase"

    @staticmethod
    def reset() -> None:
        ombroker.Fill._fill_id = 0
        omorder.Order._order_id = 0
        ombroker.Broker._submitted_order_id = 0

    @abc.abstractmethod
    def get_broker_class(self) -> Type[ocabccbr.AbstractCcxtBroker]:
        """
        Return class used to instantiate CCXT Broker.
        """
        ...

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's `start()` method.
        self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
        self.abstract_ccxt_mock: umock.MagicMock = (
            self.abstract_ccxt_patch.start()
        )
        # Set dummy credentials for all tests.
        self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}
        # Reset static variables on each test run.
        self.reset()

    def tearDown(self) -> None:
        self.get_secret_patch.stop()
        self.abstract_ccxt_patch.stop()

        # Deallocate in reverse order to avoid race conditions.
        super().tearDown()
        # Reset static variables on each test run.
        self.reset()

    # //////////////////////////////////////////////////////////////////////////

    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        # Build broker.
        universe_version = "v5"
        account_type = "trading"
        contract_type = "spot"
        secret_id = 1
        bid_ask_im_client = None
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        # Check broker state and remove dynamic mock ids.
        broker_state_with_mock_ids = pprint.pformat(vars(broker))
        broker_state = re.sub(
            r" id='(.*?)'>", " id='***'>", broker_state_with_mock_ids
        )
        self.check_string(broker_state, test_class_name=self.get_class_name())

    def test_log_into_exchange1(self) -> None:
        """
        Verify that login is done correctly with `spot` contract type.
        """
        self._test_log_into_exchange_class("trading", "spot")

    def test_log_into_exchange2(self) -> None:
        """
        Verify that login is done correctly with `futures` contract type.
        """
        self._test_log_into_exchange_class("trading", "futures")

    def test_log_into_exchange3(self) -> None:
        """
        Verify that login is done correctly with `sandbox` account type.
        """
        self._test_log_into_exchange_class("sandbox", "futures")

    def test_calculate_twap_child_order_size1(
        self,
    ) -> None:
        """
        Verify twap child order size is properly calculated via mocked
        exchange.
        """
        expected_str = r"""
        {0: -3.2, 1: -1.2, 2: 7.6}
        """
        self._test_calculate_twap_child_order_size(
            pd.Timestamp("2023-02-21 03:00:00-05:00"),
            pd.Timestamp("2023-02-21 03:05:00-05:00"),
            pd.Timedelta("1T"),
            expected_str,
        )

    def test_calculate_twap_child_order_size2(
        self,
    ) -> None:
        """
        Verify twap child order size is properly calculated via mocked
        exchange.
        """
        expected_str = r"""
        {0: -8.0, 1: -3.0, 2: 19.0}
        """
        self._test_calculate_twap_child_order_size(
            pd.Timestamp("2023-02-21 03:00:00-05:00"),
            pd.Timestamp("2023-02-21 03:10:00-05:00"),
            pd.Timedelta("5T"),
            expected_str,
        )

    def test_calculate_twap_child_order_size3(
        self,
    ) -> None:
        """
        Verify twap child order size is properly calculated via mocked
        exchange.
        """
        expected_str = r"""
        {0: -16.0, 1: -6.0, 2: 38.0}
        """
        self._test_calculate_twap_child_order_size(
            pd.Timestamp("2023-02-21 03:00:00-05:00"),
            pd.Timestamp("2023-02-21 03:10:00-05:00"),
            pd.Timedelta("10T"),
            expected_str,
        )

    def test_get_fills1(self) -> None:
        """
        Verify that orders are filled properly via mocked exchange.
        """
        orders = _get_test_order("price@twap")
        # Load fills corresponding to the test orders.
        fills = _get_test_order_fills1()
        # Assign a CCXT order ID corresponding to the order's fill.
        for order, fill in zip(orders, fills):
            order.extra_params["ccxt_id"] = int(fill["id"])
        expected_fills = "(1) Fill: asset_id=1464553467 fill_id=0 timestamp=2023-03-30 20:16:01.629000+00:00 num_shares=0.12 price=1775.0"
        prev_parent_orders_ts = pd.to_datetime("2023-03-30 20:16:01+00:00")
        self._test_get_fills(orders, fills, expected_fills, prev_parent_orders_ts)

    def test_get_fills2(self) -> None:
        """
        Verify that no fills are returned without an order execution timestamp.
        """
        orders = _get_test_order("price@twap")
        # Load fills corresponding to the test orders.
        fills = _get_test_order_fills1()
        # Assign a CCXT order ID corresponding to the order's fill.
        for order, fill in zip(orders, fills):
            order.extra_params["ccxt_id"] = int(fill["id"])
        expected_fills = hprint.format_list([])
        # Mock missing last order execution time using previous_parent_orders_timestamp=None.
        prev_parent_orders_ts = None
        self._test_get_fills(orders, fills, expected_fills, prev_parent_orders_ts)

    def test_get_fills3(self) -> None:
        """
        Verify that if some parent orders have a CCXT ID, and some do not, only
        those with an ID are considered filled.
        """
        orders = self._get_test_orders1()
        # Load fills corresponding to the test orders.
        fills = self._get_test_order_fills2()
        # Assign a CCXT order ID corresponding to the order's fill.
        # Don't assign a CCXT ID to the 1st order.
        for order, fill in zip(orders[1:], fills[1:]):
            order.extra_params["ccxt_id"] = int(fill["id"])
        prev_parent_orders_ts = pd.to_datetime("2023-03-30 20:16:01+00:00")
        # Note: the resulting values were calculated by hand with given test orders.
        expected_fills = "(2) Fill: asset_id=3065029174 fill_id=1 timestamp=2023-03-30 20:16:01.629000+00:00 num_shares=0.18 price=1774.3333 Fill: asset_id=8968126878 fill_id=2 timestamp=2023-03-30 20:16:01.629000+00:00 num_shares=0.18 price=1774.3333"
        self._test_get_fills(orders, fills, expected_fills, prev_parent_orders_ts)

    def test_get_fills4(self) -> None:
        """
        Verify that no fills are returned in case when
        `Broker._previous_parent_orders = None`.
        """
        orders = None
        fills = _get_test_order_fills1()
        expected_fills = hprint.format_list([])
        prev_parent_orders_ts = None
        self._test_get_fills(orders, fills, expected_fills, prev_parent_orders_ts)

    def test_get_fills5(self) -> None:
        """
        Verify that no fills are returned for an order with no fills.
        """
        orders = _get_test_order("price@twap")
        # Assign a CCXT order ID.
        orders[0].extra_params["ccxt_id"] = -1
        fills = []
        expected_fills = hprint.format_list([])
        prev_parent_orders_ts = None
        self._test_get_fills(orders, fills, expected_fills, prev_parent_orders_ts)

    def test_get_fills6(self) -> None:
        """
        Verify that orders with multiple child orders are filled correctly.
        """
        orders = _get_test_order("price@twap")
        fills = self._get_test_order_fills3()
        # Assign a CCXT order ID.
        orders[0].extra_params["ccxt_id"] = [
            8389765589152377433,
            8389765589152377488,
        ]
        prev_parent_orders_ts = pd.to_datetime("2023-03-30 20:16:01+00:00")
        # Note: the resulting values were calculated by hand with given test orders.
        expected_fills = "(1) Fill: asset_id=1464553467 fill_id=0 timestamp=2023-03-30 20:16:01.629000+00:00 num_shares=0.15 price=1774.0"
        self._test_get_fills(orders, fills, expected_fills, prev_parent_orders_ts)

    # //////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _get_test_orders1() -> List[omorder.Order]:
        """
        Build a list of toy orders based or real data for tests.
        """
        # pylint: disable=line-too-long
        orders_str = "\n".join(
            [
                "Order: order_id=0 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=3303714233 type_=price@twap start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-16.0 tz=America/New_York extra_params={}",
                "Order: order_id=1 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=3065029174 type_=price@twap start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=-6.0 tz=America/New_York extra_params={}",
                "Order: order_id=2 creation_timestamp=2023-02-21 02:55:44.508525-05:00 asset_id=8968126878 type_=price@twap start_timestamp=2023-02-21 02:55:44.508525-05:00 end_timestamp=2023-02-21 03:00:00-05:00 curr_num_shares=0.0 diff_num_shares=38.0 tz=America/New_York extra_params={}",
            ]
        )
        # pylint: disable=line-too-long
        orders = omorder.orders_from_string(orders_str)
        return orders

    @staticmethod
    def _get_test_order_fills2() -> List[omorder.Order]:
        """
        Get CCXT order structures for child orders associated with
        `_get_test_order1`.
        """
        fills = [
            {
                "info": {
                    "orderId": "8389765589152377439",
                    "symbol": "BNBUSDT",
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
                "symbol": "BNB/USDT",
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
                    "symbol": "DOGEUSDT",
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
                "symbol": "DOGE/USDT",
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
            {
                "info": {
                    "orderId": "8389765589152377439",
                    "symbol": "ADAUSDT",
                    "status": "FILLED",
                    "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
                    "price": "1773.0",
                    "avgPrice": "1773.70000",
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
                "symbol": "ADA/USDT",
                "type": "limit",
                "timeInForce": "GTC",
                "postOnly": "",
                "reduceOnly": "",
                "side": "buy",
                "price": 1773.0,
                "stopPrice": "",
                "amount": 0.06,
                "cost": 106.38,
                "average": 1773.7,
                "filled": 0.06,
                "remaining": 0.0,
                "status": "closed",
                "fee": "",
                "trades": [],
                "fees": [],
            },
        ]
        return fills

    @staticmethod
    def _get_test_order_fills3() -> List[omorder.Order]:
        """
        Get CCXT order structures for child orders associated with
        `_get_test_order`.
        """
        fills = [
            {
                "info": {
                    "orderId": "8389765589152377488",
                    "symbol": "ETHUSDT",
                    "status": "FILLED",
                    "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
                    "price": "1770.10",
                    "avgPrice": "1770.70000",
                    "origQty": "0.09",
                    "executedQty": "0.09",
                    "cumQty": "0.09",
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
                "id": "8389765589152377488",
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
                "amount": 0.09,
                "cost": 159.3,
                "average": 1770.7,
                "filled": 0.09,
                "remaining": 0.0,
                "status": "closed",
                "fee": "",
                "trades": [],
                "fees": [],
            },
            {
                "info": {
                    "orderId": "8389765589152377433",
                    "symbol": "ETHUSDT",
                    "status": "FILLED",
                    "clientOrderId": "x-xcKtGhcu2538b26ad6ffd65a877c16",
                    "price": "1780.10",
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
                "id": "8389765589152377433",
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
        ]
        return fills

    @staticmethod
    def _get_test_orders2() -> List[omorder.Order]:
        """
        Build a list of toy orders for unit testing.
        """
        # Prepare test data.
        # pylint: disable=line-too-long
        orders_str = "\n".join(
            [
                "Order: order_id=0 creation_timestamp=2022-08-05 10:36:00-04:00 asset_id=1464553467 type_=limit start_timestamp=2022-08-05 10:36:00-04:00 end_timestamp=2022-08-05 10:39:00-04:00 curr_num_shares=2500.0 diff_num_shares=10 tz=America/New_York extra_params={}",
                "Order: order_id=1 creation_timestamp=2022-08-05 10:36:00-04:00 asset_id=1467591036 type_=limit start_timestamp=2022-08-05 10:36:00-04:00 end_timestamp=2022-08-05 10:39:00-04:00 curr_num_shares=1000.0 diff_num_shares=-20.0 tz=America/New_York extra_params={}",
            ]
        )
        # Get orders.
        orders = omorder.orders_from_string(orders_str)
        return orders

    @staticmethod
    def _get_mock_bid_ask_im_client() -> icdc.ImClient:
        """
        Get Mock bid / ask ImClient using synthetical data.
        """
        df = _get_test_bid_ask_data()
        # Get universe.
        universe_mode = "trade"
        vendor = "CCXT"
        version = "v7"
        universe = ivcu.get_vendor_universe(
            vendor,
            universe_mode,
            version=version,
            as_full_symbol=True,
        )
        # Build ImClient.
        im_client = icdc.DataFrameImClient(df, universe)
        return im_client

    def _get_test_broker(
        self,
        universe_version: str,
        account_type: str,
        contract_type: str,
        secret_id: int,
        bid_ask_im_client: Optional[icdc.ImClient],
        *,
        log_dir: Optional[str] = None,
    ) -> ocabccbr.AbstractCcxtBroker:
        """
        Build the mocked `AbstractCcxtBroker` for unit testing.

        See `AbstractCcxtBroker` for params description.
        """
        exchange_id = "binance"
        stage = "preprod"
        portfolio_id = "ccxt_portfolio_mock"
        secret_id = ohsseide.SecretIdentifier(
            exchange_id, stage, account_type, secret_id
        )
        broker = self.get_broker_class()(
            exchange_id,
            account_type,
            portfolio_id,
            contract_type,
            secret_id,
            bid_ask_im_client=bid_ask_im_client,
            strategy_id="dummy_strategy_id",
            market_data=umock.create_autospec(
                spec=mdata.MarketData, instance=True
            ),
            universe_version=universe_version,
            stage=stage,
            log_dir=log_dir,
        )
        return broker

    # //////////////////////////////////////////////////////////////////////////

    def _test_log_into_exchange_class(
        self,
        account_type: str,
        contract_type: str,
        *,
        universe_version: str = "v5",
        secret_id: int = 1,
    ) -> None:
        """
        Verify login is performed correctly.

        - Verify that login is done correctly with given contract type.
        - Verify constructed secret for obtaining credentials from AWS secrets.
        - Verify that `sandbox` mode is set where applicable.
        """
        bid_ask_im_client = None
        exchange_mock = self.abstract_ccxt_mock.binance
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        # Check exchange mock assertions.
        actual_args = pprint.pformat(tuple(exchange_mock.call_args))
        call_args_dict = {"apiKey": "test", "rateLimit": False, "secret": "test"}
        if contract_type == "futures":
            call_args_dict["options"] = {"defaultType": "future"}
        expected_args = pprint.pformat(((call_args_dict,), {}))
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)
        # Check secrets mock assertions.
        self.assertEqual(self.get_secret_mock.call_count, 1)
        actual_args = tuple(self.get_secret_mock.call_args)
        expected_args = ((f"binance.preprod.{account_type}.{secret_id}",), {})
        self.assertEqual(actual_args, expected_args)
        # Check broker assertions.
        actual_method_calls = str(broker._exchange.method_calls)
        expected_method_call = "call.set_sandbox_mode(True)"
        if account_type == "sandbox":
            self.assertIn(expected_method_call, actual_method_calls)
        else:
            self.assertNotIn(expected_method_call, actual_method_calls)

    # //////////////////////////////////////////////////////////////////////////

    def _test_calculate_twap_child_order_size(
        self,
        execution_start: pd.Timestamp,
        execution_end: pd.Timestamp,
        execution_freq: pd.Timedelta,
        expected_str: str,
    ) -> None:
        """
        Verify twap child order size is properly calculated via mocked
        exchange.
        """
        orders = self._get_test_orders1()
        # Build broker.
        universe_version = "v5"
        account_type = "trading"
        contract_type = "spot"
        secret_id = 1
        bid_ask_im_client = None
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        # Run.
        actual = broker._calculate_twap_child_order_size(
            orders,
            execution_start,
            execution_end,
            execution_freq,
        )
        # Check the count of calls.
        self.assertEqual(3, len(actual))
        # Check the args.
        actual_str = pprint.pformat(actual)
        self.assert_equal(actual_str, expected_str, fuzzy_match=True)

    # //////////////////////////////////////////////////////////////////////////

    def _test_get_fills(
        self,
        orders: List[omorder.Order],
        fills: List[omorder.Order],
        expected_fills_as_str: str,
        previous_parent_orders_timestamp: Optional[pd.Timestamp],
    ) -> None:
        # Define broker parameters.
        universe_version = "v5"
        account_type = "trading"
        contract_type = "spot"
        secret_id = 4
        bid_ask_im_client = None
        # Initialize class.
        broker = self._get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        broker._previous_parent_orders = orders
        # Mock last order execution time. Unix time: 1680207361629 ms.
        broker.previous_parent_orders_timestamp = previous_parent_orders_timestamp
        with umock.patch.object(
            broker._exchange, "fetch_orders", create=True
        ) as fetch_orders_mock:
            fetch_orders_mock.return_value = fills
            # Run.
            fills = broker.get_fills()
        actual_fills = hprint.format_list(fills)
        self.assertEqual(actual_fills, expected_fills_as_str)


# TODO(gp): Add test for _submit_single_ccxt_order.


# #############################################################################
# TestSaveBrokerData1
# #############################################################################


@pytest.mark.skip(
    "Run manually. Cannot be run via GH because Binance is not accessible from North America."
)
class TestSaveBrokerData1(hunitest.TestCase):
    """
    Capture data from a CCXT broker so that it can be reused in other tests and
    code.
    """

    # TODO(gp): @Danya we need to document the `aws cp command`.
    def test_save_market_info1(self) -> None:
        """
        Save market info.

        See `AbstractCcxtBroker._get_market_info()` for details.
        """
        # Get market information from the exchange.
        broker = self._get_test_broker()
        market_info = broker.market_info
        _LOG.debug("asset_market_info dict '%s' ...", market_info)
        self._save_broker_data(market_info)

    def test_save_trading_fees_info1(self) -> None:
        """
        Save binance fee info.

        See `AbstractCcxtBroker._get_trading_fee_info()` for details.
        """
        # Get market information from the exchange.
        broker = self._get_test_broker()
        fees_info = broker.fees
        _LOG.debug("asset_fees dict '%s' ...", fees_info)
        self._save_broker_data(fees_info)

    @staticmethod
    def _get_test_broker() -> occcbrv1.CcxtBroker_v1:
        # Initialize broker.
        universe_version = "v7.1"
        account_type = "trading"
        contract_type = "futures"
        secret_id = 3
        bid_ask_im_client = None
        broker = _get_test_broker(
            universe_version,
            account_type,
            contract_type,
            secret_id,
            bid_ask_im_client,
        )
        return broker

    def _save_broker_data(self, data: Dict[Any, Any]) -> None:
        # Build file path.
        dst_dir = self.get_input_dir(use_only_test_class=False)
        file_name = "broker_data.json"
        file_path = os.path.join(dst_dir, file_name)
        # Save data.
        _LOG.info("Saving data in '%s' ...", file_path)
        hio.to_json(file_path, data)
        _LOG.info("Saving in '%s' done", file_path)


# #############################################################################
# Test_log_child_order_fills
# #############################################################################


class Test_log_child_order_fills(hunitest.TestCase):
    def test_log_child_order_fills(self) -> None:
        """
        Verify that child order fills are logged correctly.
        """
        fills = _get_test_order_fills1()
        start_ts = pd.Timestamp("2022-08-05 10:36:44-04:00")
        end_ts = pd.Timestamp("2022-08-05 10:38:44-04:00")
        # Log test order fills.
        log_dir = self.get_scratch_space()
        occcbrv1.CcxtBroker_v1._log_child_order_fills(
            log_dir, fills, start_ts, end_ts
        )
        # Find a JSON file where child order fills are saved.
        cmd = f"find {log_dir} -type f -name *.json"
        _, path = hsystem.system_to_string(cmd)
        # Read.
        with open(path, "r") as f:
            act = f.read()
        #
        exp = r"""
            [
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
                        "updateTime": "1680207361629"
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
                    "fees": []
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
                        "updateTime": "1680207361629"
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
                    "fees": []
                }
            ]
            """
        self.assert_equal(act, exp, fuzzy_match=True)


# #############################################################################
# Test_log_child_order
# #############################################################################


class Test_log_child_order(hunitest.TestCase):

    # TODO(gp): @juraj Add docstring.
    def helper(
        self,
        order: omorder.Order,
        child_order_response: ocabccbr.CcxtData,
        expected: str,
    ) -> None:
        log_dir = self.get_scratch_space()
        # Get wall clock time from market data.
        market_data = umock.create_autospec(spec=mdata.MarketData, instance=True)
        wall_clock_time = market_data.get_wall_clock_time
        extra_info = {}
        occcbrv1.CcxtBroker_v1.log_child_order(
            log_dir, wall_clock_time, order, child_order_response, extra_info
        )
        # Build a path to a child orders log dir.
        child_order_log_dir = os.path.join(log_dir, "oms_child_orders")
        # Find a csv file where child order information is saved.
        cmd = f"find {child_order_log_dir} -type f -name *.csv"
        _, path = hsystem.system_to_string(cmd)
        # Read.
        with open(path, "r") as f:
            act = f.read()
        self.assert_equal(act, expected, fuzzy_match=True)

    def test1(self) -> None:
        """
        Verify that a child order with CCXT order info and additional
        parameters is logged correctly.
        """
        # Get an order and set a CCXT id.
        order = _get_test_order("price@twap")[0]
        order.extra_params["ccxt_id"] = 7954906695
        # Get a child order response.
        child_order_response = _get_child_order_response()
        # Check.
        exp = r"""
        ,0
        order_id,0
        creation_timestamp,2022-08-05 10:36:44.976104-04:00
        asset_id,1464553467
        type_,price@twap
        start_timestamp,2022-08-05 10:36:44.976104-04:00
        end_timestamp,2022-08-05 10:38:44.976104-04:00
        curr_num_shares,0.0
        diff_num_shares,0.121
        tz,America/New_York
        extra_params,{'ccxt_id': 7954906695}
        ccxt_id,7954906695
        """
        self.helper(order, child_order_response, exp)

    def test2(self) -> None:
        """
        Verify that a child order without a corresponding order response is
        logged without a CCXT ID.
        """
        # Get an order and a CCXT id is absent.
        order = _get_test_order("price@twap")[0]
        # Get a child order response.
        child_order_response = {}
        #
        exp = r"""
        ,0
        order_id,0
        creation_timestamp,2022-08-05 10:36:44.976104-04:00
        asset_id,1464553467
        type_,price@twap
        start_timestamp,2022-08-05 10:36:44.976104-04:00
        end_timestamp,2022-08-05 10:38:44.976104-04:00
        curr_num_shares,0.0
        diff_num_shares,0.121
        tz,America/New_York
        extra_params,{}
        ccxt_id,-1
        """
        self.helper(order, child_order_response, exp)
