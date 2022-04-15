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

import im_v2.talos.utils as imv2tauti
import oms.broker as ombroker
import oms.oms_talos_utils as oomtauti

_LOG = logging.getLogger(__name__)


class TalosBroker(ombroker.AbstractBroker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Path for order request.
        self._order_path = "/v1/orders"
        self._api = imv2tauti.TalosApiBuilder(self._account)
        self._endpoint = self._api.get_endpoint()
        self._api_keys = self._api._api_keys

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
        wall_clock_timestamp = imv2tauti.get_talos_current_utc_timestamp()
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
        wall_clock_time = imv2tauti.get_talos_current_utc_timestamp()
        # Create initial request parts and headers.
        parts = self._api.build_parts("GET", wall_clock_time, self._order_path)
        headers = self._api.build_headers(parts, wall_clock_time)
        # Create an URL.
        query = {
            "StartDate": start_timestamp,
            "EndDate": end_timestamp,
            "OrderID": order_id,
        }
        url = self.build_url(query=query)
        # Submit a GET request and return data.
        r = requests.get(url=url, headers=headers)
        if r.status_code == 200:
            data = r.json()
        else:
            raise Exception(f"{r.status_code}: {r.text}")
        return data

    def build_url(
        self,
        *,
        query: Optional[Dict[str, Any]] = None,
        order_id: Optional[str] = None,
    ) -> str:
        """
        Build a request URL.

        The function builds an initial part of the url
        and then adds optional extra parameters, e.g. a query.

        :param query: a query in dict form
        :param order_id: a UUID of requested order
        :return: full URL for the query
        """
        url = f"https://{self._endpoint}{self._order_path}"
        if order_id:
            url += f"/{order_id}"
        if query:
            query_string = urllib.parse.urlencode(query)
            url += f"?{query_string}"
        return url

    # TODO(Danya): Implement a method for getting fills since last exec.
    def get_fills(self, order_ids: List[str]) -> Dict[str, str]:
        """
        Get fill status from unique order ids.

        The output is a dictionary where key is `OrderID` and the value is the order status, which
        has possible values:
        - New
        - PartiallyFilled
        - Filled
        - Canceled
        - PendingCancel
        - Rejected
        - PendingNew
        - PendingReplace
        - DoneForDay

        E.g.,
        ```
        {'19eaff5c-c01f-4360-8b7e-c8d0028625e3': 'Rejected',
         '81a341c1-8e2c-4027-b0ea-26fb1166549c': 'DoneForDay'}
        ```

        :param order_ids: values of `OrderID` from values of Talos' `OrderIDs`
        :return: mappings of `OrderID` to order status
        """
        # Create dictionary that will store the order status.
        fill_status_dict: Dict[str, str] = {}
        # Process each `OrderId` querying the interface to get its status.
        for order_id in order_ids:
            # Imitation of script input parameters.
            # Common elements of both GET and POST requests.
            utc_datetime = imv2tauti.get_talos_current_utc_timestamp()
            parts = self._api.build_parts("GET", utc_datetime, self._order_path)
            headers = self._api.build_headers(parts, utc_datetime)
            # Create a GET request.
            url = self.build_url(order_id=order_id)
            r = requests.get(url=url, headers=headers)
            body = r.json()
            # Specify order information.
            ord_summary = body["data"]
            # Save the general order status.
            # The output of 'ord_summary' contains information about orders and its executions.
            # The idea is to extract the order status from it.
            fills_general = ord_summary[0]["OrdStatus"]
            # Update the dictionary.
            fill_status_dict[order_id] = fills_general
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
        parts = self._api.build_parts(
            "POST", wall_clock_timestamp, self._order_path
        )
        # TODO(Danya): Make it customizable/dependent on `self._strategy`
        for order in orders:
            body = json.dumps(order)
            parts.append(body)
            # Enciode request with secret key.
            headers = self._api.build_headers(parts, wall_clock_timestamp)
            # Create a POST request.
            url = self.build_url()
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
