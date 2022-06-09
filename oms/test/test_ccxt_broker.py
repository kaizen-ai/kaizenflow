import asyncio
import logging
from typing import List
import helpers.hsecrets as hsecret
import helpers.hdbg as hdbg

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
import oms.order_example as oordexam
import oms.coinbaseprime_exchange as ocoiexch


class TestCcxtBroker1(hunitest.TestCase):

    async def run_coroutine1(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Submit orders to a CcxtBroker initialized for coinbase prime.
        """
        market_data, _ = mdata.get_ReplayedTimeMarketData_example3(None)
        strategy = "SAU1"
        # Instantiante a broker with specified exchange.
        exchange =  self._log_into_coinbaseprime_exchange()
        # TODO(Juraj) mock the API calls.
        broker = occxbrok.CcxtBroker(exchange, "v5", "prod", "c27158ee-ac73-49bb-a1f3-ec022cac33c2", strategy_id=strategy, market_data=market_data)                                                             
        # Submit an order.
        order = oordexam.get_order_example4()
        orders = [order]
        await broker.submit_orders(orders)
        # Check fills.
        #fills = broker.get_fills()
        #self.assertEqual(len(fills), 1)
        #actual = str(fills[0])
        #expected = r"""Fill: asset_id=101 fill_id=0 timestamp=2000-01-01 09:35:00-05:00 num_shares=100.0 price=1000.3449750508295
        #"""
        #self.assert_equal(actual, expected, fuzzy_match=True)

    def test_submit_and_fill1(self) -> None:
        event_loop = None
        hasynci.run(self.run_coroutine1(event_loop), event_loop=event_loop)

    @pytest.mark.skip(reason="Code in development.")
    def test_unsupported_exchange1(self) -> None:
        """
        Test that ValueError is raised when CcxtBroker cannot be
        instantiated with given exchange because of missing method 
        implementation.
        """
        market_data, _ = mdata.get_ReplayedTimeMarketData_example3(None)
        strategy = "SAU1"
        with pytest.raises(ValueError) as fail:
            broker = occxbrok.CcxtBroker("coinbase", "v3", "test", strategy_id=strategy, market_data=market_data) 
        actual = str(fail.value)
        self.assertIn("The coinbase exchange is not fully supported for placing orders.", actual)

    def _log_into_coinbaseprime_exchange(self) -> ocoiexch.coinbaseprime:
        """
        Log into coinbaseprime and return the corresponding
        `ccxt.Exchange` object.
        """
        # Select credentials for provided exchange.
        credentials = hsecret.get_secret("coinbaseprime")
        # Enable rate limit.
        credentials["rateLimit"] = True
        # Create a CCXT Exchange class object.
        exchange = ocoiexch.coinbaseprime(credentials)
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange
