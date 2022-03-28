import asyncio
import logging
from typing import List

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest
import market_data as mdata
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


class TestSimulatedBroker2(hunitest.TestCase):
    def test_collect_spread_buy(self) -> None:
        order = oordexam.get_order_example3(0.0)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=998.0308407941129"""
        self.helper(order, expected)

    def test_collect_spread_sell(self) -> None:
        order = oordexam.get_order_example3(0.0, -100)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=-100.0 price=998.0311059094466"""
        self.helper(order, expected)

    def test_midpoint_buy(self) -> None:
        order = oordexam.get_order_example3(0.5)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=998.0309733517797"""
        self.helper(order, expected)

    def test_midpoint_sell(self) -> None:
        order = oordexam.get_order_example3(0.5, -100)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=-100.0 price=998.0309733517797"""
        self.helper(order, expected)

    def test_quarter_spread_buy(self) -> None:
        order = oordexam.get_order_example3(0.75)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=998.0310396306132"""
        self.helper(order, expected)

    def test_quarter_spread_sell(self) -> None:
        order = oordexam.get_order_example3(0.75, -100)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=-100.0 price=998.0309070729463"""
        self.helper(order, expected)

    def test_cross_spread_buy(self) -> None:
        order = oordexam.get_order_example3(1.0)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=998.0311059094466"""
        self.helper(order, expected)

    def test_cross_spread_sell(self) -> None:
        order = oordexam.get_order_example3(1.0, -100)
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=-100.0 price=998.0308407941129"""
        self.helper(order, expected)

    def helper(self, order: omorder.Order, expected: str) -> ombroker.Fill:
        with hasynci.solipsism_context() as event_loop:
            start_datetime = pd.Timestamp(
                "2000-01-01 09:30:00-05:00", tz="America/New_York"
            )
            end_datetime = pd.Timestamp(
                "2000-01-01 09:50:00-05:00", tz="America/New_York"
            )
            asset_ids = [101]
            market_data, _ = mdata.get_ReplayedTimeMarketData_example5(
                event_loop,
                start_datetime,
                end_datetime,
                asset_ids,
            )
            broker = obroexam.get_simulated_broker_example1(
                event_loop, market_data=market_data
            )
            broker_coroutine = self._broker_coroutine(broker, order)
            coroutines = [broker_coroutine]
            fills = hasynci.run(
                asyncio.gather(*coroutines), event_loop=event_loop
            )[0]
            self.assertEqual(len(fills), 1)
            actual = str(fills[0])
            self.assert_equal(actual, expected, fuzzy_match=True)

    async def _broker_coroutine(
        self, broker: ombroker.SimulatedBroker, order
    ) -> List[ombroker.Fill]:
        orders = [order]
        get_wall_clock_time = broker.market_data.get_wall_clock_time
        # await asyncio.sleep(1)
        await hasynci.sleep(1, get_wall_clock_time)
        await broker.submit_orders(orders)
        # Wait until order fulfillment.
        fulfillment_deadline = order.end_timestamp
        await hasynci.wait_until(fulfillment_deadline, get_wall_clock_time)
        # Check fills.
        fills = broker.get_fills()
        return fills


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
        get_wall_clock_time = broker.market_data.get_wall_clock_time
        # await asyncio.sleep(1)
        await hasynci.sleep(1, get_wall_clock_time)
        await broker.submit_orders(orders)
        # Wait until order fulfillment.
        fulfillment_deadline = order.end_timestamp
        await hasynci.wait_until(fulfillment_deadline, get_wall_clock_time)
        # Check fills.
        fills = broker.get_fills()
        return fills
