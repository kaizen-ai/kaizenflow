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
import urllib
from typing import Any, Dict, List

import requests

import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


class TalosBroker(ombroker.AbstractBroker):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._api_keys = hsecret.get_secret(self._account)
        self._endpoint = self.get_endpoint()

    def calculate_signature(self, parts: List[str]) -> str:
        """
        Encode the request using Talos secret key.

        Requires parts of the API request provided as a list, e.g.:
        [
        "POST",
        str(utc_datetime),
        "tal-87.sandbox.talostrading.com",
        "/v1/orders",
        ]

        :param parts: parts of the GET or POST request
        :returns: an encoded string
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
            endpoint = "tal-87.sandbox.talostrading.com"
        else:
            hdbg.dfatal(
                "Incorrect account type. Supported account types: 'talos_sandbox'."
            )
        return endpoint

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

        Note: Currently acts a placeholder to demonstrate
        the format of Talos order.
        """
        # TODO(Danya): Adapt to `oms.order.Order` type,
        #  e.g. convert Order to a supported Talos format.
        # TODO(Danya): Add assertions for order types and trading strategies.
        # TODO(Danya): Connect to `strategy` parameter?
        # TODO(Danya): Pass the order information as a config.
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

    def submit_order(
        self,
        orders: List[Dict[str, Any]],
        *,
    ) -> None:
        """
        Submit and log multiple orders given by the model.
        """
        # TODO(Danya): Merge with `market_data` wall clock time
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
        # TODO(Danya): Merge with wall_clock_timestamp method.
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
            "TALOS-KEY": self._api_keys["apiKey"],
            "TALOS-SIGN": signature,
            "TALOS-TS": wall_clock_timestamp,
        }
        # Create a POST request.
        url = f"https://{self._endpoint}{path}"
        r = requests.post(url=url, data=body, headers=headers)
        # TODO(Danya): Return a receipt instead of a status code.
        if r.status_code != 200:
            # TODO(Danya): Remove Exception.
            Exception(f"{r.status_code}: {r.text}")
        return r.status_code

    def get_orders(self, start_timestamp: str, end_timestamp: str,):
        utc_datetime = self.get_talos_current_utc_timestamp()
        query = {"StartDate": start_timestamp, "EndDate": end_timestamp}
        query_string = urllib.parse.urlencode(query)
