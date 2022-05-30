import asyncio
import logging
from typing import List

import pandas as pd
import pytest

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

import oms.ccxt_broker as occxbrok


class TestCcxtBroker1(hunitest.TestCase):
    def test_submit_and_fill1(self) -> None:
        event_loop = None
        hasynci.run(self._test_coroutine1(event_loop), event_loop=event_loop)

    async def _test_coroutine1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Submit orders to a CcxtBroker initialized for coinbase prime.
        """
        market_data, _ = mdata.get_ReplayedTimeMarketData_example3(None)
        strategy = "SAU1"
        # Instantiante a broker with specified exchange.
        broker = occxbrok.CcxtBroker("coinbaseprime", "test", strategy_id=strategy, market_data=market_data)                                                                      
        # Submit an order.
        order = _get_order_example1()
        orders = [order]
        await broker.submit_orders(orders)
        # Check fills.
        #fills = broker.get_fills()
        #self.assertEqual(len(fills), 1)
        #actual = str(fills[0])
        #expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=1000.3449750508295
        #"""
        #self.assert_equal(actual, expected, fuzzy_match=True)

    def test_unsupported_exchange1(self) -> None:
        """
        Test that ValueError is raised when CcxtBroker cannot be
        instantiated with given exchange because of missing method 
        implementation.
        """
        market_data, _ = mdata.get_ReplayedTimeMarketData_example3(None)
        strategy = "SAU1"
        with pytest.raises(ValueError) as fail:
            broker = occxbrok.CcxtBroker("coinbase", "test", strategy_id=strategy, market_data=market_data) 
        actual = str(fail.value)
        self.assertIn("coinbase", actual)
        self.assertIn("fetchClosedOrders", actual)

def _get_order_example1() -> omorder.Order:
    creation_timestamp = pd.Timestamp(
        "2000-01-01 09:30:00-05:00", tz="America/New_York"
    )
    asset_id = "ADA/USDT"
    type_ = "TWAP"
    start_timestamp = pd.Timestamp(
        "2000-01-01 09:35:00-05:00", tz="America/New_York"
    )
    end_timestamp = pd.Timestamp(
        "2000-01-01 09:40:00-05:00", tz="America/New_York"
    )
    curr_num_shares = 0
    diff_num_shares = 100
    order_id = 0
    # Build Order.
    order = omorder.Order(
        creation_timestamp,
        asset_id,
        type_,
        start_timestamp,
        end_timestamp,
        curr_num_shares,
        diff_num_shares,
        order_id=order_id,
    )
    return order