import asyncio
import datetime
import logging
import pprint
import pytest
import unittest.mock as umock
from typing import Dict, List, Optional, Tuple, Type

import ccxt
import numpy as np
import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hunit_test as hunitest
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu
import market_data.market_data_example as mdmadaex
import market_data.replayed_market_data as mdremada
import oms.broker.broker as obrobrok
import oms.broker.ccxt.abstract_ccxt_broker as obcaccbr
import oms.broker.ccxt.ccxt_broker_v2 as obccbrv2
import oms.broker.ccxt.mock_ccxt_exchange as obcmccex
import oms.broker.ccxt.test.ccxt_broker_test_case as obccbteca
import oms.broker.ccxt.test.test_ccxt_utils as obcttcut
import oms.hsecrets.secret_identifier as ohsseide
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)

# #############################################################################
# TestCcxtBroker_V2
# #############################################################################

# The value is reused in various place for interface reasons.
_UNIVERSE_VERSION = "v7"

_TEST_BID_ASK_COLS = [
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
]

_ASSET_ID_SYMBOL_MAPPING = {
    1464553467: "binance::ETH_USDT",
    1467591036: "binance::BTC_USDT",
    6051632686: "binance::APE_USDT",
}


def _generate_test_bid_ask_data(
    start_ts: pd.Timestamp,
    asset_id_symbol_mapping: Dict[int, str],
    num_minutes: int,
) -> pd.DataFrame:
    """
    Generate artifical bid/ask data for testing.

    :param start_ts: timestamp of the first generated row
    :param num_minutes: how many bid/ask data to generate
    """

    # Set seed to generate predictable values.
    np.random.seed(0)
    data = []
    for asset_id, full_symbol in asset_id_symbol_mapping.items():
        bid_size_l1, ask_size_l1, bid_price_l1, ask_price_l1 = (
            np.random.randint(10, 40) for _ in range(4)
        )
        for i in range(num_minutes):
            timestamp_iter = start_ts + datetime.timedelta(minutes=i)
            row = [
                i,
                timestamp_iter,
                asset_id,
                bid_size_l1,
                ask_size_l1,
                bid_price_l1,
                ask_price_l1,
                full_symbol,
                timestamp_iter,
                timestamp_iter,
            ]
            data.append(row)
            timestamp_iter += datetime.timedelta(seconds=1)
            row = [
                i,
                timestamp_iter,
                asset_id,
                bid_size_l1,
                ask_size_l1,
                bid_price_l1,
                ask_price_l1,
                full_symbol,
                timestamp_iter,
                timestamp_iter,
            ]
            data.append(row)
    df = pd.DataFrame(columns=_TEST_BID_ASK_COLS, data=data).set_index(
        "timestamp"
    )
    return df

@pytest.mark.skip(reason="Skipped to work in follow-ups, test will be reenabled in CmTask4852")
class TestCcxtBroker_V2(obccbteca.TestCcxtBroker_TestCase):
    def get_broker_class(self) -> Type[obcaccbr.AbstractCcxtBroker]:
        """
        Return class used to instantiate CCXT Broker.
        """
        return obccbrv2.CcxtBroker_v2

    def test_get_ccxt_order_structure1(self) -> None:
        """
        Test that _get_ccxt_order_structure() returns expected results:

        - Raises AssertionError if ccxt_id is not in order.
        - None if ccxt_id is -1.
        - Raises AssertionError if ccxt_id is -1 without an error message
        - ccxt_order if ccxt_id is not -1.
        """
        # Build broker.
        broker = self._get_local_test_broker()
        # Mock methods.
        broker._async_exchange = umock.create_autospec(ccxt.Exchange)
        expected_order = "unbelievable order"
        broker.asset_id_to_ccxt_symbol_mapping = umock.MagicMock()
        broker._async_exchange.fetch_order = umock.MagicMock(
            return_value=expected_order
        )
        #
        # Check that assertion is raised if ccxt_id is not in order.
        order = obccbteca._get_test_order("market")[0]
        order.extra_params = {}
        with self.assertRaises(AssertionError) as e:
            broker._get_ccxt_order_structure(order)
        excepted_msg = "* Failed assertion *\n'ccxt_id' in '{}'"
        actual_msg = str(e.exception)
        self.assert_equal(excepted_msg, actual_msg, fuzzy_match=True)
        #
        # Assign a ccxt_id of -1 as if the order was not accepted.
        order = obcaccbr.AbstractCcxtBroker._set_ccxt_id_to_child_order(order, -1)
        #
        # Check that the assertion is raised if there is no error message.
        with self.assertRaises(AssertionError) as e:
            broker._get_ccxt_order_structure(order)
        excepted_msg = (
            f"* Failed assertion *\n'error_msg' in '{str(order.extra_params)}'"
        )
        actual_msg = str(e.exception)
        self.assert_equal(excepted_msg, actual_msg, fuzzy_match=True)
        #
        # Check that it returns None if ccxt_id is -1 with error message.
        order.extra_params["error_msg"] = "Some error message"
        result = broker._get_ccxt_order_structure(order)
        self.assertIsNone(result)
        #
        # Check normal case.
        order = obcaccbr.AbstractCcxtBroker._set_ccxt_id_to_child_order(order, 1)
        result = broker._get_ccxt_order_structure(order)
        print(result)
        self.assert_equal(expected_order, result)

    def test_get_ccxt_fills(self) -> None:
        """
        Test that get_ccxt_fills() returns expected results:

        - Empty list if ccxt_order is None.
        - List with ccxt_order if ccxt_order is not None.
        """
        # Build broker.
        broker = self._get_local_test_broker()
        # Mock method.
        broker._get_ccxt_order_structure = umock.create_autospec(
            broker._get_ccxt_order_structure, return_value=None
        )
        # Check that it returns empty list if ccxt_order is None.
        order = obccbteca._get_test_order("market")
        result = broker.get_ccxt_fills(order)
        self.assertListEqual([], result)
        expected_order = "dummy_order"
        broker._get_ccxt_order_structure = umock.create_autospec(
            broker._get_ccxt_order_structure, return_value=expected_order
        )
        # Check normal case.
        result = broker.get_ccxt_fills(order)
        self.assertListEqual([expected_order], result)

    def test_update_stats_for_order(self) -> None:
        """
        Test that _update_stats_for_order() updates
        order.extra_params["stats"].
        """
        # Build broker.
        broker = self._get_local_test_broker()
        order = obccbteca._get_test_order("market")[0]
        order.id = "123"
        function_name = "test_update_stats_for_order"
        tag = "ccxt_order"
        value = "value"
        # Check that order was updated.
        broker._update_stats_for_order(order, tag, value)
        self.assertDictEqual(
            {function_name + "::" + tag: value}, order.extra_params["stats"]
        )

    def _get_local_test_broker(self) -> obcaccbr.AbstractCcxtBroker:
        """
        Return a CCXT Broker for local testing.
        """
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
        return broker


@pytest.mark.skip(reason="Skipped to work in follow-ups, test will be reenabled in CmTask4852")
class TestCcxtBroker_V2_UsingFakeExchange(hunitest.TestCase):

    # This mock is necessary because it is not possible to initialize a broker
    # without going through creation of CCXT Exchange, will be resolved in
    # CmTask4698.
    get_secret_patch = umock.patch.object(obcaccbr.hsecret, "get_secret")
    abstract_ccxt_patch = umock.patch.object(obcaccbr, "ccxt", spec=obcaccbr.ccxt)

    @staticmethod
    def reset() -> None:
        obrobrok.Fill._fill_id = 0
        oordorde.Order._order_id = 0
        obrobrok.Broker._submitted_order_id = 0

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

    def test_submit_twap_orders1(self) -> None:
        """
        Verify that a single TWAP buy order is submitted correctly over a
        single iteration.
        """
        creation_timestamp = pd.Timestamp("2022-08-05 09:30:55+00:00")
        start_timestamp = pd.Timestamp("2022-08-05 09:31:00+00:00")
        end_timestamp = pd.Timestamp("2022-08-05 09:32:01+00:00")
        curr_num_shares = 0
        orders_str = "\n".join(
            [
                f"Order: order_id=1 creation_timestamp={creation_timestamp} asset_id=1464553467 type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares={curr_num_shares} diff_num_shares=1.0 tz=UTC extra_params={{}}",
            ]
        )
        # Get orders and postions.
        orders = oordorde.orders_from_string(orders_str)
        starting_positions = [
            {"info": {"positionAmt": curr_num_shares}, "symbol": "ETH/USDT"},
        ]
        # Run TWAP submission.
        orders, broker = self._test_submit_twap_orders(orders, starting_positions)
        submitted_orders = broker._previous_parent_orders
        exp = r"""
        [Order:
        order_id=0
        creation_timestamp=2022-08-05 05:31:01-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 05:31:01-04:00
        end_timestamp=2022-08-05 05:32:01-04:00
        curr_num_shares=0.0
        diff_num_shares=1.0
        tz=America/New_York
        extra_params={'ccxt_id': [0],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.0': Timestamp('2022-08-05 05:32:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York')}}]
        """
        actual_orders = pprint.pformat(orders)
        self.assert_equal(actual_orders, exp, fuzzy_match=True)
        # TODO(Grisha): test also when there are no ccxt_ids for the parent order.
        submitted_orders = pprint.pformat(submitted_orders)
        # TODO(Grisha): convert to a df and freeze instead of using string.
        # TODO(Juraj): since this is almost the same as the generated input, it should not be hardcoded,
        # instead it should be made programatically possible to generate the expected output.
        exp = r"""
        [Order:
        order_id=1
        creation_timestamp=2022-08-05 09:30:55+00:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 09:31:00+00:00
        end_timestamp=2022-08-05 09:32:01+00:00
        curr_num_shares=0.0
        diff_num_shares=1.0
        tz=UTC
        extra_params={'ccxt_id': [0],
        'stats': {'_submit_twap_child_order::child_order.id_added_to_parent_order.0': Timestamp('2022-08-05 05:32:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.0': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.end': Timestamp('2022-08-05 05:31:01-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.start': Timestamp('2022-08-05 05:30:55-0400', tz='America/New_York'),
                'submit_twap_orders::start': Timestamp('2022-08-05 05:30:55-0400', tz='America/New_York')}}]
        """
        self.assert_equal(submitted_orders, exp, fuzzy_match=True)
        # Assert fills.
        fills = broker.get_fills()
        actual_fills = pprint.pformat(fills)
        # The timestamp is taken as last update time of all child orders.
        exp = r"""
        [Fill: asset_id=1464553467 fill_id=1 timestamp=2022-08-05 09:31:01+00:00 num_shares=1.0 price=20.5]
        """
        self.assert_equal(actual_fills, exp, fuzzy_match=True)

    def test_submit_twap_orders2(self) -> None:
        """
        Verify that a single TWAP sell order is submitted correctly over
        multiple iterations.
        """
        creation_timestamp = pd.Timestamp("2022-08-05 09:29:55+00:00")
        start_timestamp = pd.Timestamp("2022-08-05 09:30:00+00:00")
        end_timestamp = pd.Timestamp("2022-08-05 09:33:01+00:00")
        curr_num_shares = 12
        asset_ids = list(_ASSET_ID_SYMBOL_MAPPING.keys())
        orders_str = "\n".join(
            [
                f"Order: order_id=1 creation_timestamp={creation_timestamp} asset_id={asset_ids[0]} type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares={curr_num_shares} diff_num_shares=-{curr_num_shares} tz=UTC extra_params={{}}",
            ]
        )
        # Get orders and postions.
        orders = oordorde.orders_from_string(orders_str)
        starting_positions = [
            {
                "info": {"positionAmt": curr_num_shares},
                "symbol": _ASSET_ID_SYMBOL_MAPPING[asset_ids[0]],
            },
        ]
        # Run TWAP submission.
        orders, broker = self._test_submit_twap_orders(orders, starting_positions)
        submitted_orders = broker._previous_parent_orders
        exp = r"""
        [Order:
        order_id=0
        creation_timestamp=2022-08-05 05:30:01-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 05:30:01-04:00
        end_timestamp=2022-08-05 05:31:01-04:00
        curr_num_shares=0.0
        diff_num_shares=-4.0
        tz=America/New_York
        extra_params={'ccxt_id': [0],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.0': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York')}},
        Order:
        order_id=1
        creation_timestamp=2022-08-05 05:31:00-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 05:31:00-04:00
        end_timestamp=2022-08-05 05:32:00-04:00
        curr_num_shares=0.0
        diff_num_shares=-4.0
        tz=America/New_York
        extra_params={'ccxt_id': [1],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York')}},
        Order:
        order_id=2
        creation_timestamp=2022-08-05 05:31:00-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 05:31:00-04:00
        end_timestamp=2022-08-05 05:32:00-04:00
        curr_num_shares=0.0
        diff_num_shares=-4.0
        tz=America/New_York
        extra_params={'ccxt_id': [2],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York')}}]
        """
        actual_orders = pprint.pformat(orders)
        self.assert_equal(actual_orders, exp, fuzzy_match=True)
        # TODO(Grisha): test also when there are no ccxt_ids for the parent order.
        submitted_orders = pprint.pformat(submitted_orders)
        # TODO(Grisha): convert to a df and freeze instead of using string.
        # TODO(Juraj): since this is almost the same as the generated input, it should not be hardcoded,
        # instead it should be made programatically possible to generate the expected output.
        exp = r"""
        [Order:
        order_id=1
        creation_timestamp=2022-08-05 09:29:55+00:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 09:30:00+00:00
        end_timestamp=2022-08-05 09:33:01+00:00
        curr_num_shares=12.0
        diff_num_shares=-12.0
        tz=UTC
        extra_params={'ccxt_id': [0, 1, 2],
        'stats': {'_submit_twap_child_order::child_order.id_added_to_parent_order.0': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.id_added_to_parent_order.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.id_added_to_parent_order.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.0': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.1': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.2': Timestamp('2022-08-05 05:31:00-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.end': Timestamp('2022-08-05 05:30:01-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.start': Timestamp('2022-08-05 05:29:55-0400', tz='America/New_York'),
                'submit_twap_orders::start': Timestamp('2022-08-05 05:29:55-0400', tz='America/New_York')}}]
        """
        self.assert_equal(submitted_orders, exp, fuzzy_match=True)
        # Assert fills.
        fills = broker.get_fills()
        actual_fills = pprint.pformat(fills)
        exp = r"""
        [Fill: asset_id=1464553467 fill_id=1 timestamp=2022-08-05 09:31:00+00:00 num_shares=-12.0 price=20.5]
        """
        self.assert_equal(actual_fills, exp, fuzzy_match=True)

    def test_submit_twap_orders3(self) -> None:
        """
        Verify that 2 TWAP orders (1 buy, 1 sell) are submitted correctly.
        """
        creation_timestamp = pd.Timestamp("2022-08-05 10:35:55-04:00")
        start_timestamp = pd.Timestamp("2022-08-05 10:36:00-04:00")
        # 1 second is added inside the broker code logic of `submit_twap_orders` method.
        end_timestamp = pd.Timestamp("2022-08-05 10:38:01-04:00")
        starting_positions = [
            {"info": {"positionAmt": 2500}, "symbol": "ETH/USDT"},
            {"info": {"positionAmt": 1000}, "symbol": "BTC/USDT"},
        ]
        orders = self._get_test_orders(
            creation_timestamp, start_timestamp, end_timestamp
        )

        orders, broker = self._test_submit_twap_orders(orders, starting_positions)
        submitted_orders = broker._previous_parent_orders
        # Check the order Dataframe.
        # TODO(Juraj): since this is correlated with the generated input, it should not be hardcoded,
        # instead it should be made programatically possible to generate the expected output.
        # TODO(Juraj, Danya): check if the expected string is correct.
        exp = r"""
        [Order:
        order_id=0
        creation_timestamp=2022-08-05 10:36:01-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 10:36:01-04:00
        end_timestamp=2022-08-05 10:37:01-04:00
        curr_num_shares=2500.0
        diff_num_shares=5.0
        tz=America/New_York
        extra_params={'ccxt_id': [0],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.0': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York')}},
        Order:
        order_id=1
        creation_timestamp=2022-08-05 10:36:01-04:00
        asset_id=1467591036
        type_=limit
        start_timestamp=2022-08-05 10:36:01-04:00
        end_timestamp=2022-08-05 10:37:01-04:00
        curr_num_shares=1000.0
        diff_num_shares=-10.0
        tz=America/New_York
        extra_params={'ccxt_id': [1],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.0': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York')}},
        Order:
        order_id=2
        creation_timestamp=2022-08-05 10:37:00-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 10:37:00-04:00
        end_timestamp=2022-08-05 10:38:00-04:00
        curr_num_shares=2505.0
        diff_num_shares=5.0
        tz=America/New_York
        extra_params={'ccxt_id': [2],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York')}},
        Order:
        order_id=3
        creation_timestamp=2022-08-05 10:37:00-04:00
        asset_id=1467591036
        type_=limit
        start_timestamp=2022-08-05 10:37:00-04:00
        end_timestamp=2022-08-05 10:38:00-04:00
        curr_num_shares=990.0
        diff_num_shares=-10.0
        tz=America/New_York
        extra_params={'ccxt_id': [3],
        'max_leverage': 1,
        'stats': {'_submit_single_order_to_ccxt_with_retry::end.timestamp': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_single_order_to_ccxt_with_retry::num_attempts': 0,
                '_submit_single_order_to_ccxt_with_retry::start.timestamp': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::aligned_with_next_child_order.start.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.created.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.limit_price_calculated.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.logged.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.submitted.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York')}}]
        """
        actual_orders = pprint.pformat(orders)
        self.assert_equal(actual_orders, exp, fuzzy_match=True)
        # TODO(Grisha): test also when there are no ccxt_ids for the parent order.
        submitted_orders = pprint.pformat(submitted_orders)
        # TODO(Grisha): convert to a df and freeze instead of using string.
        # TODO(Juraj): since this is almost the same as the generated input, it should not be hardcoded,
        # instead it should be made programatically possible to generate the expected output.
        exp = r"""
        [Order:
        order_id=0
        creation_timestamp=2022-08-05 10:35:55-04:00
        asset_id=1464553467
        type_=limit
        start_timestamp=2022-08-05 10:36:00-04:00
        end_timestamp=2022-08-05 10:38:01-04:00
        curr_num_shares=2500.0
        diff_num_shares=10.0
        tz=America/New_York
        extra_params={'ccxt_id': [0, 2],
        'stats': {'_submit_twap_child_order::child_order.id_added_to_parent_order.0': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.id_added_to_parent_order.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.end': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.start': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'),
                'submit_twap_orders::start': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}},
        Order:
        order_id=1
        creation_timestamp=2022-08-05 10:35:55-04:00
        asset_id=1467591036
        type_=limit
        start_timestamp=2022-08-05 10:36:00-04:00
        end_timestamp=2022-08-05 10:38:01-04:00
        curr_num_shares=1000.0
        diff_num_shares=-20.0
        tz=America/New_York
        extra_params={'ccxt_id': [1, 3],
        'stats': {'_submit_twap_child_order::child_order.id_added_to_parent_order.0': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::child_order.id_added_to_parent_order.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_order::start.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::bid_ask_market_data.done.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.0': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                '_submit_twap_child_orders::order_coroutines_created.1': Timestamp('2022-08-05 10:37:00-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.end': Timestamp('2022-08-05 10:36:01-0400', tz='America/New_York'),
                'submit_twap_orders::align_with_parent_order.start': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York'),
                'submit_twap_orders::start': Timestamp('2022-08-05 10:35:55-0400', tz='America/New_York')}}]
        """
        self.assert_equal(submitted_orders, exp, fuzzy_match=True)

    def test_submit_twap_orders_with_price_assertion(self):
        """
        Verify that the fill corresponding to the order has the correct limit
        price.

        This test case uses engineered bid/ask data instead of randomly
        generated ones in order to be able to assert the average price
        calculated in the fill. The test assumes that the obtained price
        is exactly the limit price and each order is fully filled.
        """
        creation_timestamp = pd.Timestamp("2022-08-05 10:35:55-04:00")
        start_timestamp = pd.Timestamp("2022-08-05 10:35:55-04:00")
        end_timestamp = pd.Timestamp("2022-08-05 10:37:01-04:00")
        curr_num_shares = 0
        asset_id = 1464553467
        orders_str = "\n".join(
            [
                f"Order: order_id=1 creation_timestamp={creation_timestamp} asset_id={asset_id} type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares={curr_num_shares} diff_num_shares=1.0 tz=UTC extra_params={{}}",
            ]
        )
        # Get orders and postions.
        orders = oordorde.orders_from_string(orders_str)
        starting_positions = [
            {
                "info": {"positionAmt": curr_num_shares},
                "symbol": _ASSET_ID_SYMBOL_MAPPING[asset_id],
            },
        ]
        # Run TWAP submission.
        bid_ask_df = obcttcut.get_test_bid_ask_data()
        _, broker = self._test_submit_twap_orders(
            orders, starting_positions, bid_ask_df=bid_ask_df
        )
        # Assert fills.
        fills = broker.get_fills()
        actual_fills = pprint.pformat(fills)
        # The timestamp is taken as last update time of all child orders.
        # The price corresponds to using passivity factor = 0.5.
        exp = r"""
        [Fill: asset_id=1464553467 fill_id=1 timestamp=2022-08-05 14:35:56+00:00 num_shares=1.0 price=56.0]
        """
        self.assert_equal(actual_fills, exp, fuzzy_match=True)

    @staticmethod
    def _get_mock_bid_ask_im_client(df: pd.DataFrame) -> icdc.ImClient:
        """
        Get Mock bid / ask ImClient using synthetical data.
        """
        # Get universe.
        universe_mode = "trade"
        vendor = "CCXT"
        version = _UNIVERSE_VERSION
        universe = ivcu.get_vendor_universe(
            vendor,
            universe_mode,
            version=version,
            as_full_symbol=True,
        )
        # Build ImClient.
        im_client = icdc.DataFrameImClient(df, universe)
        return im_client

    @staticmethod
    def _get_test_orders(
        creation_timestamp: pd.Timestamp,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> List[oordorde.Order]:
        """
        Build a list of toy orders for unit testing.

        :param creation_timestamp: creation_timestamp to set to the order objects
        :param start_timestmap: start timestamp to set to the order objects
        :param creation_timestamp: end timestamp to set to the order objects
        """
        orders_str = "\n".join(
            [
                f"Order: order_id=0 creation_timestamp={creation_timestamp} asset_id=1464553467 type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares=2500.0 diff_num_shares=10 tz=America/New_York extra_params={{}}",
                f"Order: order_id=1 creation_timestamp={creation_timestamp} asset_id=1467591036 type_=limit start_timestamp={start_timestamp} end_timestamp={end_timestamp} curr_num_shares=1000.0 diff_num_shares=-20.0 tz=America/New_York extra_params={{}}",
            ]
        )
        # Get orders.
        orders = oordorde.orders_from_string(orders_str)
        return orders

    def _test_submit_twap_orders(
        self,
        orders: List[oordorde.Order],
        positions: Dict,
        *,
        bid_ask_df: pd.DataFrame = None,
    ) -> Tuple[List[oordorde.Order], obccbrv2.CcxtBroker_v2]:
        """
        Verify that a TWAP order(s) is submitted correctly.

        Base method used by test cases.

        :orders: orders to place
        :positions: simulated positions the experiment should start with, example structure found in MockCcxtExchange
        :return: all orders generated by twap_submit_orders and a corresponding broker object
        """
        with hasynci.solipsism_context() as event_loop:
            creation_timestamp = orders[0].creation_timestamp
            num_minutes = 10
            if bid_ask_df is None:
                bid_ask_df = _generate_test_bid_ask_data(
                    creation_timestamp, _ASSET_ID_SYMBOL_MAPPING, num_minutes
                )
            bid_ask_im_client = self._get_mock_bid_ask_im_client(bid_ask_df)
            # The second element in the returned tuple is a callable.
            (
                market_data,
                get_wall_clock_time,
            ) = mdmadaex.get_ReplayedTimeMarketData_example2(
                event_loop,
                # For solipsism to work correctly it is important to set the correct time interval.
                creation_timestamp,
                creation_timestamp + pd.Timedelta(minutes=num_minutes),
                0,
                list(_ASSET_ID_SYMBOL_MAPPING.keys()),
            )
            broker = self._get_test_broker(
                bid_ask_im_client, market_data, 2, event_loop, get_wall_clock_time
            )
            broker._async_exchange._positions = positions
            passivity_factor = 0.5
            execution_freq = "1T"
            # TODO(Juraj): How to set timestamp for the test orders?
            # On one hand we do not want to freeze time but passing real wall clock time
            # might results in irrepeatable test?
            coroutines = [
                broker.submit_twap_orders(
                    orders, passivity_factor, execution_freq=execution_freq
                )
            ]
            orders = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )[0]
        return orders, broker

    def _get_test_broker(
        self,
        bid_ask_im_client: Optional[icdc.ImClient],
        market_data: mdremada.ReplayedMarketData,
        mock_exchange_delay: obcmccex.DelayType,
        event_loop: asyncio.AbstractEventLoop,
        get_wall_clock_time: hdateti.GetWallClockTime,
        *,
        log_dir: Optional[str] = None,
    ) -> obccbrv2.CcxtBroker_v2:
        """
        Build the mocked `AbstractCcxtBroker` for unit testing.

        See `AbstractCcxtBroker` for params description.
        """
        universe_version = _UNIVERSE_VERSION
        account_type = "trading"
        contract_type = "futures"
        secret_id = 1
        exchange_id = "binance"
        stage = "preprod"
        portfolio_id = "ccxt_portfolio_mock"
        secret_id = ohsseide.SecretIdentifier(
            exchange_id, stage, account_type, secret_id
        )
        # Inject Mock Exchange instead of real CCXT object.
        mock_exchange = obcmccex.MockCcxtExchange(
            mock_exchange_delay, event_loop, get_wall_clock_time
        )
        # TODO(Juraj): this is a hacky way, we mock ccxt lib meaning
        # the broker initially received mock exchange and then we replace
        # it with fake.
        broker = obccbrv2.CcxtBroker_v2(
            exchange_id=exchange_id,
            account_type=account_type,
            portfolio_id=portfolio_id,
            contract_type=contract_type,
            secret_identifier=secret_id,
            bid_ask_im_client=bid_ask_im_client,
            strategy_id="dummy_strategy_id",
            market_data=market_data,
            universe_version=universe_version,
            stage=stage,
            log_dir=log_dir,
        )
        broker._async_exchange = mock_exchange
        return broker
