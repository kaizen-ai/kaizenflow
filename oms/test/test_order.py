import logging

import helpers.hunit_test as hunitest
import oms.order as omorder
import oms.order_example as oordexam

_LOG = logging.getLogger(__name__)


class TestOrder1(hunitest.TestCase):
    def helper(self, order: omorder.Order, expected_order_as_str: str) -> None:
        """
        Test building and serializing an Order.
        """
        # Check.
        order_as_str = str(order)
        expected_order_as_str = expected_order_as_str.replace("\n", " ")
        self.assert_equal(order_as_str, expected_order_as_str, fuzzy_match=True)
        # Deserialize from string.
        order_from_string = omorder.Order.from_string(order_as_str)
        # Check.
        order_from_string = str(order_from_string)
        self.assert_equal(
            order_from_string, expected_order_as_str, fuzzy_match=True
        )

    def test1(self) -> None:
        """
        Supplied extra parameters.
        """
        extra_params = {"ccxt_id": 0}
        order = oordexam.get_order_example1(order_extra_params=extra_params)
        expected_order_as_str = r"""Order: order_id=0
        creation_timestamp=2000-01-01 09:30:00-05:00
        asset_id=101
        type_=price@twap
        start_timestamp=2000-01-01 09:35:00-05:00
        end_timestamp=2000-01-01 09:40:00-05:00
        curr_num_shares=0.0
        diff_num_shares=100.0
        tz=America/New_York
        extra_params={'ccxt_id': 0}"""
        # Check.
        self.helper(order, expected_order_as_str)

    def test2(self) -> None:
        """
        Extra parameters are not supplied.
        """
        extra_params = None
        order = oordexam.get_order_example1(order_extra_params=extra_params)
        expected_order_as_str = r"""Order: order_id=0
        creation_timestamp=2000-01-01 09:30:00-05:00
        asset_id=101
        type_=price@twap
        start_timestamp=2000-01-01 09:35:00-05:00
        end_timestamp=2000-01-01 09:40:00-05:00
        curr_num_shares=0.0
        diff_num_shares=100.0
        tz=America/New_York
        extra_params={}"""
        # Check.
        self.helper(order, expected_order_as_str)


class TestMultipleOrders1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test building and serializing a list of Orders.
        """
        extra_params1 = {"ccxt_id": 0}
        extra_params2 = None
        orders = [
            oordexam.get_order_example1(order_extra_params=extra_params1),
            oordexam.get_order_example1(order_extra_params=extra_params2),
        ]
        act = omorder.orders_to_string(orders)
        exp = r"""
Order: order_id=0 creation_timestamp=2000-01-01 09:30:00-05:00 asset_id=101 type_=price@twap start_timestamp=2000-01-01 09:35:00-05:00 end_timestamp=2000-01-01 09:40:00-05:00 curr_num_shares=0.0 diff_num_shares=100.0 tz=America/New_York extra_params={'ccxt_id': 0}
Order: order_id=0 creation_timestamp=2000-01-01 09:30:00-05:00 asset_id=101 type_=price@twap start_timestamp=2000-01-01 09:35:00-05:00 end_timestamp=2000-01-01 09:40:00-05:00 curr_num_shares=0.0 diff_num_shares=100.0 tz=America/New_York extra_params={}
"""
        self.assert_equal(act, exp, fuzzy_match=True)
        # Deserialize from string.
        orders2 = omorder.orders_from_string(act)
        # Check.
        act = omorder.orders_to_string(orders2)
        self.assert_equal(act, exp, fuzzy_match=True)
