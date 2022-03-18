"""
An implementation of broker class for Talos API.

Import as:

import oms.talos_broker as otalbrok
"""

import json
import logging
import urllib
from typing import Any, Dict, List, Optional

import requests

import helpers.hsecrets as hsecret
import oms.broker as ombroker
import oms.oms_talos_utils as oomtauti

_LOG = logging.getLogger(__name__)


class TalosBroker(ombroker.AbstractBroker):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # TODO(Danya): Provide a working example of MarketData for testing.
        self._api_keys = hsecret.get_secret(self._account)
        # Talos request endpoint.
        self._endpoint = oomtauti.get_endpoint(self._account)
        # Path for order request.
        self._order_path = "/v1/orders"

    @staticmethod
    def create_order(
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
            "ClOrdID": oomtauti.get_order_id(),
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
    ) -> None:
        """
        Submit and log multiple orders given by the model.
        """
        # TODO(Danya): Merge with `market_data` wall clock time
        wall_clock_timestamp = oomtauti.get_talos_current_utc_timestamp()
        _LOG.debug("Submitting %d orders", len(orders))
        for order in orders:
            _LOG.debug("Submitting order %s", order["ClOrdID"])
            self._submit_orders([order], wall_clock_timestamp)

    def get_orders(
        self,
        *,
        start_timestamp: Optional[str] = "",
        end_timestamp: Optional[str] = "",
        order_id: Optional[str] = "",
    ) -> Dict[str, str]:
        """
        Get current orders by date and order id.

        Example of order data:
        """
        # TODO(Danya): Add specific order data.
        wall_clock_time = oomtauti.get_talos_current_utc_timestamp()
        query = {
            "StartDate": start_timestamp,
            "EndDate": end_timestamp,
            "OrderID": order_id,
        }
        query_string = urllib.parse.urlencode(query)
        # TODO(Danya): Factor out authorization.
        parts = [
            "GET",
            wall_clock_time,
            self._endpoint,
            self._order_path,
            query_string,
        ]
        signature = oomtauti.calculate_signature(
            self._api_keys["secretKey"], parts
        )
        headers = {
            "TALOS-KEY": self._api_keys["publicKey"],
            "TALOS-SIGN": signature,
            "TALOS-TS": wall_clock_time,
        }
        url = f"https://{self._endpoint}{self._order_path}?{query_string}"
        r = requests.get(url=url, headers=headers)
        if r.status_code == 200:
            data = r.json()
        else:
            raise Exception(f"{r.status_code}: {r.text}")
        return data

    def get_fills(self, order_id_list: List[str]) -> Dict[str, str]:
        """
        Get fill status from unique order ids. The possible values are:

        - New
        - PartiallyFilled
        - Filled
        - Canceled
        - PendingCancel
        - Rejected
        - PendingNew
        - PendingReplace
        - DoneForDay

        Example of an output:
        {('ce871d61-a1f6-4993-8f81-a6d8f872be53', 'Canceled'),
        ('e38ec070-30b7-49d4-a301-619c2d3ed20e', 'DoneForDay')}

        :param order_id_list: values of `OrderID` from Talos universe
        :return: mappings of `OrderID` and order status
        """
        # Create dictionary that will store the order status.
        fill_status_dict: Dict[str, str] = {}
        # Initiate the loop for every `OrderID` in the list.
        for order_id in order_id_list:
            # Imitation of script input parameters.
            # Common elements of both GET and POST requests.
            utc_datetime = oomtauti.get_talos_current_utc_timestamp()
            parts = [
                "GET",
                utc_datetime,
                self._endpoint,
                f"{self._order_path}/{order_id}",
            ]
            signature = oomtauti.calculate_signature(
                self._api_keys["secret"], parts
            )
            headers = {
                "TALOS-KEY": self._api_keys["apiKey"],
                "TALOS-SIGN": signature,
                "TALOS-TS": utc_datetime,
            }
            # Create a GET request.
            url = f"https://{self._endpoint}{self._order_path}/{order_id}"
            r = requests.get(url=url, headers=headers)
            body = r.json()
            # Specify order information.
            ord_summary = body["data"]
            # Save the general order status.
            fills_general = ord_summary[0]["OrdStatus"]
            # Writing these values into the dictionary.
            fill_status = {order_id: fills_general}
            fill_status_dict = fill_status_dict | fill_status.items()
        return fill_status_dict

    def _submit_orders(
        self,
        orders: List[Dict[str, Any]],
        wall_clock_timestamp: str,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Submit a single order.
        """
        parts = [
            "POST",
            wall_clock_timestamp,
            self._endpoint,
            self._order_path,
        ]
        # TODO(Danya): Make it customizable/dependent on `self._strategy`
        for order in orders:
            body = json.dumps(order)
            parts.append(body)
            # Enciode request with secret key.
            signature = oomtauti.calculate_signature(
                self._api_keys["secretKey"], parts
            )
            headers = {
                "TALOS-KEY": self._api_keys["apiKey"],
                "TALOS-SIGN": signature,
                "TALOS-TS": wall_clock_timestamp,
            }
            # Create a POST request.
            url = f"https://{self._endpoint}{self._order_path}"
            r = requests.post(url=url, data=body, headers=headers)
            # TODO(Danya): Return a receipt instead of a status code.
            if r.status_code != 200:
                # TODO(Danya): Remove Exception.
                Exception(f"{r.status_code}: {r.text}")

    def _wait_for_accepted_orders(
        self,
        file_name: str,
    ) -> None:
        raise NotImplementedError