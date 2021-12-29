import asyncio
import logging
from typing import List

import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest
import oms.broker as ombroker
import oms.broker_example as obroexam
import oms.oms_db as oomsdb
import oms.order as omorder
import oms.order_example as oordexam
import oms.order_processor as oordproc
import oms.test.oms_db_helper as omtodh

_LOG = logging.getLogger(__name__)


class TestSimulatedBroker1(hunitest.TestCase):
    def test_submit_and_fill1(self) -> None:
        event_loop = None
        hasynci.run(self._test_coroutine1(event_loop), event_loop=event_loop)

    async def _test_coroutine1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Submit orders to a SimulatedBroker.
        """
        # Build a SimulatedBroker.
        broker = obroexam.get_simulated_broker_example1(event_loop)
        # Submit an order.
        order = oordexam.get_order_example2()
        orders = [order]
        await broker.submit_orders(orders)
        # Check fills.
        fills = broker.get_fills()
        self.assertEqual(len(fills), 1)
        actual = str(fills[0])
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=1000.3449750508295
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestMockedBroker1(omtodh.TestOmsDbHelper):
    def setUp(self) -> None:
        super().setUp()
        # Create OMS tables.
        oomsdb.create_oms_tables(self.connection, incremental=False)

    def tearDown(self) -> None:
        # Remove the OMS tables.
        oomsdb.remove_oms_tables(self.connection)
        super().tearDown()

    def test1(self) -> None:
        """
        Test submitting orders to a MockedBroker.
        """
        order = oordexam.get_order_example1()
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:40:00-05:00 num_shares=100.0 price=999.9161531095003
        """
        self.helper(order, expected)

    def helper(self, order: omorder.Order, expected: str) -> ombroker.Fill:
        with hasynci.solipsism_context() as event_loop:
            broker = obroexam.get_mocked_broker_example1(
                event_loop, self.connection
            )
            order_processor_coroutine = self._order_processor_coroutine(broker)
            broker_coroutine = self._broker_coroutine(broker, order)
            coroutines = [order_processor_coroutine, broker_coroutine]
            fills = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )[1]
            self.assertEqual(len(fills), 1)
            actual = str(fills[0])
            self.assert_equal(actual, expected, fuzzy_match=True)

    async def _order_processor_coroutine(
        self, broker: ombroker.MockedBroker
    ) -> None:
        delay_to_accept_in_secs = 2
        delay_to_fill_in_secs = 1
        order_processor = oordproc.OrderProcessor(
            self.connection,
            delay_to_accept_in_secs,
            delay_to_fill_in_secs,
            broker,
        )
        await order_processor.enqueue_orders()

    async def _broker_coroutine(
        self, broker: ombroker.MockedBroker, order
    ) -> List[ombroker.Fill]:
        orders = [order]
        get_wall_clock_time = broker.market_data_interface.get_wall_clock_time
        # await asyncio.sleep(1)
        await hasynci.sleep(1, get_wall_clock_time)
        await broker.submit_orders(orders)
        # Wait until order fulfillment.
        fulfillment_deadline = order.end_timestamp
        await hasynci.wait_until(fulfillment_deadline, get_wall_clock_time)
        # Check fills.
        fills = broker.get_fills()
        return fills
