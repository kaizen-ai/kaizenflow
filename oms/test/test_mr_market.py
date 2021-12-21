import asyncio
from typing import Union

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.hasyncio as hasynci
import oms.broker as ombroker
import oms.broker_example as obroexam
import oms.mr_market as omrmark
import oms.oms_db as oomsdb
import oms.order_example as oordexam
import oms.test.oms_db_helper as omtodh


class TestMrMarketOrderProcessor1(omtodh.TestOmsDbHelper):
    def setUp(self) -> None:
        super().setUp()
        # Create OMS tables.
        oomsdb.create_oms_tables(self.connection, incremental=False)

    def tearDown(self) -> None:
        # Create OMS tables.
        oomsdb.remove_oms_tables(self.connection)
        super().tearDown()

    def helper(self, termination_condition: Union[int, pd.Timestamp]) -> None:
        """
        Create two coroutines, one with a MockedBroker, the other with an
        OrderProcessor.
        """
        #
        with hasynci.solipsism_context() as event_loop:
            # Build MockedBroker.
            broker = obroexam.get_mocked_broker_example1(
                event_loop, self.connection
            )
            get_wall_clock_time = broker.market_data_interface.get_wall_clock_time
            # Create a coroutine executing the Broker.
            broker_coroutine = self.broker_coroutine(broker, get_wall_clock_time)
            # Build OrderProcessor.
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            order_processor = omrmark.order_processor(
                self.connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
                termination_condition,
            )
            # Run.
            coroutines = [order_processor, broker_coroutine]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    async def broker_coroutine(
        self,
        broker: ombroker.AbstractBroker,
        get_wall_clock_time: hdateti.GetWallClockTime,
    ) -> None:
        # We need to wait 1 sec to make sure that the OrderProcessor comes up first
        # and starts polling, avoiding
        await hasynci.sleep(1, get_wall_clock_time)
        # Create an order.
        order = oordexam.get_order_example1()
        # Submit the order to the broker.
        await broker.submit_orders([order])

    def test_submit_order1(self) -> None:
        """
        Test submitting one order and having the OrderProcessor accept that.
        """
        # Shut down after one order is received.
        termination_condition = 1
        self.helper(termination_condition)

    def test_submit_order2(self) -> None:
        """
        Test submitting one order and having the OrderProcessor accept that.
        """
        # Shut down after one order is received.
        termination_condition = pd.Timestamp("2000-01-01 09:35:15-05:00")
        self.helper(termination_condition)

    def test_submit_order_and_timeout1(self) -> None:
        """
        Test submitting one order and having the OrderProcessor accept that.
        """
        # Shut down after two orders are received, but the MockedBroker sends only
        # one, so OrderProcessor times out.
        termination_condition = 2
        with self.assertRaises(TimeoutError):
            self.helper(termination_condition)

    def test_submit_order_and_timeout2(self) -> None:
        """
        Test submitting one order and having the OrderProcessor accept that.
        """
        # Shut down after 1 hour, but the MockedBroker sends only one at 9:35,
        # so OrderProcessor times out.
        termination_condition = pd.Timestamp("2000-01-01 10:30:00-05:00")
        with self.assertRaises(TimeoutError):
            self.helper(termination_condition)
