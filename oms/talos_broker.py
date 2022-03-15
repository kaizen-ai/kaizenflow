"""
An implementation of broker class for Talos API.

Import as:

import oms.talos_broker as otalbrok
"""

import base64
import datetime
import hashlib
import hmac
import json
import logging
import uuid
from typing import Any, Dict, List

import requests

import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


class TalosBroker(ombroker.AbstractBroker):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.api_keys = hsecret.get_secret(self._account)
        self.endpoint = self.get_endpoint()

    def calculate_signature(self, parts) -> str:
        """
        Encode the request.
        """
        payload = "\n".join(parts)
        hash = hmac.new(
            self.api_keys["secretKey"].encode("ascii"),
            payload.encode("ascii"),
            hashlib.sha256,
        )
        hash.hexdigest()
        signature = base64.urlsafe_b64encode(hash.digest()).decode()
        return signature

    def get_endpoint(self) -> str:
        """
        Get entrypoint to Talos API.

        Based on the 'account' value.
        """
        if self._account == "talos_sandbox":
            return "tal-87.sandbox.talostrading.com"
        else:
            hdbg.dfatal(
                "Incorrect account type. Supported account types: 'talos_sandbox'."
            )

    def create_order(
        self,
        exchanges: List[str],
        quantity: float,
        timestamp: str,
        symbol: str,
        trading_currency: str,
        order_type: str,
        price: float,
        side: float,
    ) -> Dict[str, Any]:
        """
        Create an order.
        """
        # TODO(Danya): Adapt to `oms.order.Order` type,
        #  e.g. convert Order to a supported Talos format.
        # TODO(Danya): Add assertions for order types and trading strategies.
        # TODO(Danya): Connect to `strategy` parameter?
        order = {
            "ClOrdID": self.get_order_id(),
            # E.g. `["binance", "coinbase"]`.
            "Markets": exchanges,
            "OrderQty": quantity,
            # E.g. "BTC-USDT".
            "Symbol": symbol,
            # E.g. "BTC".
            "Currency": trading_currency,
            # E.g. 2019-10-20T15:00:00.000000Z
            "TransactTime": timestamp,
            # E.g. "Limit".
            "OrdType": order_type,
            "TimeInForce": "GoodTillCancel",
            "Price": price,
            "Side": side,
        }
        return order

    def submit_orders(
        self,
        orders: List[Dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Submit and log orders given by the model.
        """
        wall_clock_timestamp = self.get_talos_current_utc_timestamp()
        _LOG.debug("Submitting %d orders", len(orders))
        for order in orders:
            _LOG.debug("Submitting order %s", order["ClOrdID"])
            _ = self._submit_order(order, wall_clock_timestamp)

    @staticmethod
    def get_order_id():
        """
        Get an order ID in UUID4 format.
        """
        return str(uuid.uuid4())

    @staticmethod
    def get_talos_current_utc_timestamp():
        """
        Return the current UTC timestamp in Talos-acceptable format.

        Example: 2019-10-20T15:00:00.000000Z
        """
        utc_datetime = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S.000000Z"
        )
        return utc_datetime

    def _submit_order(
        self, order: Dict[str, Any], wall_clock_timestamp: str
    ) -> int:
        """
        Submit a single order.
        """
        parts = [
            "POST",
            wall_clock_timestamp,
            "tal-87.sandbox.talostrading.com",
            "/v1/orders",
        ]
        # TODO(Danya): Make it customizable/dependent on `self._strategy`
        path = "/v1/orders"
        body = json.dumps(order)
        parts.append(body)
        # Enciode request with secret key.
        signature = self.calculate_signature(parts)
        headers = {
            "TALOS-KEY": self.api_keys["apiKey"],
            "TALOS-SIGN": signature,
            "TALOS-TS": wall_clock_timestamp,
        }
        # Create a POST request.
        url = f"https://{self.endpoint}{path}"
        r = requests.post(url=url, data=body, headers=headers)
        # TODO(Danya): Return a receipt instead of a status code.
        if r.status_code != 200:
            # TODO(Danya): Remove Exception.
            Exception(f"{r.status_code}: {r.text}")
        return r.status_code