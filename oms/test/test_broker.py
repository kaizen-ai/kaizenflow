import logging

import helpers.unit_test as hunitest
import oms.broker_example as obroexam
import oms.order_example as oordexam
import oms.test.test_oms_db as ottb

_LOG = logging.getLogger(__name__)


class TestSimulatedBroker1(hunitest.TestCase):
    def test1(self) -> None:
        """
        
        """
        event_loop = None
        broker = obroexam.get_broker_example1(event_loop)
        order = oordexam.get_order_example1()
        #
        orders = [order]
        broker.submit_orders(orders)
        #


class TestMockedBroker1(ottb._TestOmsDbHelper):
    def test1(self) -> None:
        """
        
        """
        event_loop = None
        broker = obroexam.get_broker_example1(event_loop)
        order = oordexam.get_order_example1()
        #
        orders = [order]
        broker.submit_orders(orders)
