import asyncio
from typing import Union

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import oms.broker as ombroker
import oms.broker_example as obroexam
import oms.oms_db as oomsdb
import oms.order_example as oordexam
import oms.order_processor as oordproc
import oms.test.oms_db_helper as omtodh


class TestOrderProcessor1(omtodh.TestOmsDbHelper):
    @staticmethod
    async def broker_coroutine(
        broker: ombroker.Broker,
        get_wall_clock_time: hdateti.GetWallClockTime,
    ) -> None:
        # We need to wait 1 sec to make sure that the OrderProcessor comes up first
        # and starts polling, avoiding a deadlock.
        await hasynci.sleep(1, get_wall_clock_time)
        # Create an order.
        order = oordexam.get_order_example1()
        # Submit the order to the broker.
        await broker.submit_orders([order])

    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def setUp(self) -> None:
        super().setUp()
        # Create OMS tables.
        incremental = False
        asset_id_name = "asset_id"
        oomsdb.create_oms_tables(self.connection, incremental, asset_id_name)

    def tearDown(self) -> None:
        # Remove the OMS tables.
        oomsdb.remove_oms_tables(self.connection)
        super().tearDown()

    def helper(self, termination_condition: Union[int, pd.Timestamp]) -> None:
        """
        Create two coroutines, one with a `DatabaseBroker`, the other with an
        `OrderProcessor`.
        """
        #
        with hasynci.solipsism_context() as event_loop:
            # Build a DatabaseBroker.
            broker = obroexam.get_DatabaseBroker_example1(
                event_loop, self.connection
            )
            get_wall_clock_time = broker.market_data.get_wall_clock_time
            # Create a coroutine executing the Broker.
            broker_coroutine = self.broker_coroutine(broker, get_wall_clock_time)
            # Build an OrderProcessor.
            delay_to_accept_in_secs = 3
            delay_to_fill_in_secs = 10
            order_processor = oordproc.OrderProcessor(
                self.connection,
                delay_to_accept_in_secs,
                delay_to_fill_in_secs,
                broker,
            )
            order_processor_coroutine = order_processor.run_loop(
                termination_condition
            )
            # Run.
            coroutines = [order_processor_coroutine, broker_coroutine]
            hasynci.run(asyncio.gather(*coroutines), event_loop=event_loop)

    def test_submit_order1(self) -> None:
        """
        Submit one order and have the OrderProcessor accept that.
        """
        # Shut down after one order is received.
        termination_condition = 1
        self.helper(termination_condition)

    def test_submit_order2(self) -> None:
        """
        Submit one order and have the OrderProcessor accept that.
        """
        # Shut down after one order is received.
        termination_condition = pd.Timestamp("2000-01-01 09:35:15-05:00")
        self.helper(termination_condition)

    def test_submit_order_and_timeout1(self) -> None:
        """
        Test OrderProcessor timing out.
        """
        # Shut down after two orders are received, but the DatabaseBroker sends only
        # one, so the OrderProcessor times out.
        termination_condition = 2
        with self.assertRaises(TimeoutError):
            self.helper(termination_condition)

    def test_submit_order_and_timeout2(self) -> None:
        """
        Test OrderProcessor timing out.
        """
        # Shut down the OrderProcessor after 1 hour, but the DatabaseBroker sends
        # only one order at 9:35, so the OrderProcessor times out.
        termination_condition = pd.Timestamp("2000-01-01 10:30:00-05:00")
        with self.assertRaises(TimeoutError):
            self.helper(termination_condition)
