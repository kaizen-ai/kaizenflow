"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import logging
from typing import Any, Dict, List

import ccxt
import helpers.hsecrets as hsecret
import oms.broker as ombroker
import oms.order as omorder
import pandas as pd
import numpy as np
import helpers.hdbg as hdbg
import itertools
import pytest

_LOG = logging.getLogger(__name__)


class CcxtBroker(ombroker.Broker):
    def __init__(
        self, exchange: str, mode: str, *args: Any, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        """
        Constructor.

        :param exchange: name of the exchange to initialize the broker for
        :param mode: supportted values: "test", "prod", if "test", launches the broker
         in sandbox environment (not supported for every exchange), 
         if "prod" launches with production API.
        """
        hdbg.dassert_in(mode, ["prod", "test"])
        self._exchange_id = exchange
        self._exchange = self._log_into_exchange()
        self.assert_order_methods_presence()
        if mode == "test":
            self._exchange.set_sandbox_mode(True)

    def assert_order_methods_presence(self) -> None:
        """
        Assert that the requested exchange supports all methods
        necessary to make placing/fetching orders possible.
        """
        methods = ['createOrder', 'fetchClosedOrders']
        mask = list(map(lambda x: self._exchange.has[x], methods))
        print(self._exchange.has)
        if not all(mask):
            rev_mask = [not x for x in mask]
            # Informs which methods are missing in the ccxt implementation.
            raise ValueError(f"{self._exchange_id} does not support: {list(itertools.compress(methods, rev_mask))}")

    def get_fills(self) -> List[ombroker.Fill]:
        return []

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into an exchange via CCXT and return the corresponding
        `ccxt.Exchange` object.
        """
        # TODO(Juraj): Perform check if all required methods are
        # supported for given exchange.
        # Select credentials for provided exchange.
        credentials = hsecret.get_secret(self._exchange_id)
        # Enable rate limit.
        credentials["rateLimit"] = True
        exchange_class = getattr(ccxt, self._exchange_id)
        # Create a CCXT Exchange class object.
        exchange = exchange_class(credentials)
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange

    async def _submit_orders(
        self,
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> str:
        """
        Submit orders.
        """
        order_resps: List[Dict] = []
        for order in orders:
            # TODO(Juraj): perform bunch of assertions for order attributes.
            type_ = order.type_
            side_ = "buy" if order.diff_num_shares > 0 else "sell"

            order_resp = self._exchange.createOrder(
                symbol=order.asset_id,
                type=type_,
                side=side_,
                amount=order.diff_num_shares,
            )
            order_resps.append(order_resp)

    def _wait_for_accepted_orders(
        self,
        file_name: str,
    ) -> None:
        raise NotImplementedError