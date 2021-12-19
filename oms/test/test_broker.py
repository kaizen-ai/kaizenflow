import asyncio
import logging

import pandas as pd
import pytest

import helpers.hasyncio as hasynci
import helpers.unit_test as hunitest
import oms.broker_example as obroexam
import oms.order_example as oordexam
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
        order = oordexam.get_order_example2(event_loop)
        orders = [order]
        await broker.submit_orders(orders)
        # Check fills.
        timestamp = pd.Timestamp(
            "2000-01-01 09:35:00-05:00", tz="America/New_York"
        )
        fills = broker.get_fills(timestamp)
        self.assertEqual(len(fills), 1)
        actual = str(fills[0])
        expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=1000.3449750508295
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


# TODO(gp): Finish this.
@pytest.mark.skip(reason="Need to be finished")
class TestMockedBroker1(omtodh.TestOmsDbHelper):
    def test1(self) -> None:
        """
        Test submitting orders to a MockedBroker.
        """
        event_loop = None
        broker = obroexam.get_mocked_broker_example1(event_loop, self.connection)
        #
        order = oordexam.get_order_example1()
        orders = [order]
        # await broker.submit_orders(orders)
        # Check fills.
        broker.get_fills
        # TODO(gp): Implement this.
