import logging

import helpers.unit_test as hunitest
import oms.order as omorder
import oms.order_example as oordexam

_LOG = logging.getLogger(__name__)


class TestOrder1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test building and serializing an Order.
        """
        order = oordexam.get_order_example1()
        # Check.
        act = str(order)
        exp = r"""Order: order_id=0
        creation_timestamp=2000-01-01 09:30:00-05:00
        asset_id=101
        type_=price@twap
        start_timestamp=2000-01-01 09:35:00-05:00
        end_timestamp=2000-01-01 09:40:00-05:00
        num_shares=100.0
        tz=America/New_York"""
        exp = exp.replace("\n", " ")
        self.assert_equal(act, exp, fuzzy_match=True)
        # Deserialize from string.
        order2 = omorder.Order.from_string(act)
        # Check.
        act = str(order2)
        self.assert_equal(act, exp, fuzzy_match=True)


class TestOrders1(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test building and serializing a list of Orders.
        """
        orders = [oordexam.get_order_example1(), oordexam.get_order_example1()]
        act = omorder.orders_to_string(orders)
        exp = r"""
Order: order_id=0 creation_timestamp=2000-01-01 09:30:00-05:00 asset_id=101 type_=price@twap start_timestamp=2000-01-01 09:35:00-05:00 end_timestamp=2000-01-01 09:40:00-05:00 num_shares=100.0 tz=America/New_York
Order: order_id=0 creation_timestamp=2000-01-01 09:30:00-05:00 asset_id=101 type_=price@twap start_timestamp=2000-01-01 09:35:00-05:00 end_timestamp=2000-01-01 09:40:00-05:00 num_shares=100.0 tz=America/New_York
"""
        # exp = exp.replace("\n", " ")
        self.assert_equal(act, exp, fuzzy_match=True)
        # Deserialize from string.
        orders2 = omorder.orders_from_string(act)
        # Check.
        act = omorder.orders_to_string(orders2)
        self.assert_equal(act, exp, fuzzy_match=True)
