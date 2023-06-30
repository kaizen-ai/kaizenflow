import asyncio
import logging
import unittest.mock as umock
from typing import Type

import ccxt

import oms.ccxt.abstract_ccxt_broker as ocabccbr
import oms.ccxt.ccxt_broker_test_case as occbteca
import oms.ccxt.ccxt_broker_v2 as occcbrv2

_LOG = logging.getLogger(__name__)

# #############################################################################
# TestCcxtBroker_V2
# #############################################################################


class TestCcxtBroker_V2(occbteca.TestCcxtBroker_TestCase):
    def get_broker_class(self) -> Type[ocabccbr.AbstractCcxtBroker]:
        """
        Return class used to instantiate CCXT Broker.
        """
        return occcbrv2.CcxtBroker_v2

    def test_submit_orders1(self) -> None:
        """
        Test that _submit_orders() raises ValueError.
        """
        # Build broker.
        broker = self._get_local_test_broker()
        # Run.
        orders = occbteca._get_test_order("market")
        with self.assertRaises(ValueError) as e:
            asyncio.run(
                broker._submit_orders(orders, "dummy_timestamp", dry_run=True)
            )
        excepted_msg = "Do not get here"
        actual_msg = str(e.exception)
        self.assert_equal(excepted_msg, actual_msg, fuzzy_match=True)

    def test_get_ccxt_order_structure(self) -> None:
        """
        Test that _get_ccxt_order_structure() returns expected results:

        - Raises AssertionError if ccxt_id is not in order.
        - None if ccxt_id is -1.
        - ccxt_order if ccxt_id is not -1.
        """
        # Build broker.
        broker = self._get_local_test_broker()
        # Mock methods.
        broker._exchange = umock.create_autospec(ccxt.Exchange)
        expected_order = "unbelievable order"
        broker.asset_id_to_ccxt_symbol_mapping = umock.MagicMock()
        broker._exchange.fetch_order = umock.MagicMock(
            return_value=expected_order
        )
        # Check that assertion is raised if ccxt_id is not in order.
        order = occbteca._get_test_order("market")[0]
        order.extra_params = {}
        with self.assertRaises(AssertionError) as e:
            broker._get_ccxt_order_structure(order)
        excepted_msg = "* Failed assertion *\n'ccxt_id' in '{}'"
        actual_msg = str(e.exception)
        self.assert_equal(excepted_msg, actual_msg, fuzzy_match=True)
        # Check that it returns None if ccxt_id is -1.
        order.extra_params["ccxt_id"] = -1
        result = broker._get_ccxt_order_structure(order)
        self.assertIsNone(result)
        # Check normal case.
        order.extra_params["ccxt_id"] = 1
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
        order = occbteca._get_test_order("market")
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
        order = occbteca._get_test_order("market")[0]
        order.id = "123"
        tag = "ccxt_order"
        value = "value"
        # Check that order was updated.
        broker._update_stats_for_order(order, tag, value)
        self.assertDictEqual({tag: value}, order.extra_params["stats"])

    def _get_local_test_broker(self) -> ocabccbr.AbstractCcxtBroker:
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
