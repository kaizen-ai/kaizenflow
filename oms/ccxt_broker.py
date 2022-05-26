"""
An implementation of broker class for CCXT.
"""

import json
import logging
import urllib
from typing import Any, Dict, List, Optional

import requests

import oms.broker as ombroker
import helpers.hsecrets as hsecret
import oms.order as omorder


_LOG = logging.getLogger(__name__)

class CcxtBroker(ombroker.Broker):
    def __init__(self, exchange: str, is_test: str *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        """
        Constructor.

        :param exchange: name of the exchange to initialize the broker for
        :param is_test: if True, launches the broker in sandbox environment
         (not supported for every exchange)
        """
        self.exchange = exchange
        self._exchange = self._log_into_exchange(exchange)
        # TODO(Juraj) add assertion if test environment is supported.
        if is_test:
            self._exchange.set_sandbox_mode(True)

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into an exchange via CCXT and return the corresponding
        `ccxt.Exchange` object.
        """
        # TODO(Juraj) Perform check if all required methods are
        # supported for given exchange.
        # Select credentials for provided exchange.
        credentials = hsecret.get_secret(self.exchange)
        # Enable rate limit.
        credentials["rateLimit"] = True
        exchange_class = getattr(ccxt, self.exchange)
        # Create a CCXT Exchange class object.
        exchange = exchange_class(credentials)
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange

    @abc.abstractmethod
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
            #TODO(Juraj): is it determined by order.type_?
            type_ = order.type_
            # TODO(Juraj): is this correct?
            side_ = "buy" if order.diff_num_shares > 0 else "sell"

            order_resp = self._exchange.createOrder(symbol=order.asset_id, type=type_, side=side_, amount=order.diff_num_shares)
            order_resps.append(order_resp)
            

    def get_fills(self) -> List[Fill]:
        raise NotImplementedError

    def _wait_for_accepted_orders(
        self,
        file_name: str,
    ) -> None:
        raise NotImplementedError


