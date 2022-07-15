"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import ccxt
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hsecrets as hsecret
import im_v2.common.universe as ivcu
import oms

_LOG = logging.getLogger(__name__)


class CcxtBroker(oms.Broker):
    def __init__(
        self,
        exchange_id: str,
        universe_version: str,
        mode: str,
        contract_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        """
        Constructor.

        :param exchange_id: name of the exchange to initialize the broker for
        :param universe_version: version of the universe to use
        :param mode: supported values: "test", "prod".
            - If "test", launches the broker in sandbox environment (not
              supported for every exchange),
            - If "prod" launches with production API.
        :param contract_type: "spot" or "futures"
        """
        self._exchange_id = exchange_id
        hdbg.dassert_in(mode, ["prod", "test"])
        self._mode = mode
        #
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._contract_type = contract_type
        #
        self._exchange = self._log_into_exchange()
        self._assert_order_methods_presence()
        # Enable mapping back from asset ids when placing orders.
        self._asset_id_to_symbol_mapping = self._build_asset_id_to_symbol_mapping(
            universe_version
        )
        # Will be used to determine timestamp since when to fetch orders.
        self.last_order_execution_ts: Optional[pd.Timestamp] = None

    def get_fills(
        self, order_responses: Dict[str, Tuple[Any, Any]]
    ) -> List[oms.Fill]:
        """
        Return list of fills from the last order execution.

        The input is a mapping of exchange IDs to a tuple of
        exchange order responses and orders themselves (see output of `_submit_orders`).

        :param order_responses: dictionary of orders and exchange responses
        :return: a list of filled orders
        """
        filled_order_ids = []
        # Separate orders by asset_id.
        order_asset_ids = set()
        for response in order_responses.values():
            order_asset_ids.add(response[0]["asset_id"])
        _LOG.debug("Asset IDs: %s", order_asset_ids)
        # Get filled orders since the last submission ts.
        for asset_id in order_asset_ids:
            symbol = self._asset_id_to_symbol_mapping[asset_id]
            # TODO(Danya): Since orders are executed immediately, this condition
            #  returns only the latest order to be submitted. Think of changing the timestamp source.
            #  Should we select all orders and then filter by IDs given to the method?
            since = hdateti.convert_timestamp_to_unix_epoch(
                    self.last_order_execution_ts,
                )
            # Select all closed orders.
            filled_order_responses = self._exchange.fetchClosedOrders(
                since=since,
                symbol=symbol,
            )
            _LOG.debug("Closed orders since %s: %s", since, filled_order_responses)
            # Filter only filled ones.
            filled_order_responses = [
                resp
                for resp in filled_order_responses
                if resp["info"]["status"] == "FILLED"
            ]
            # Select exchange IDs of the filled orders and save.
            filled_order_ids_tmp = [resp["id"] for resp in filled_order_responses]
            filled_order_ids.extend(filled_order_ids_tmp)
        fills: List[oms.Fill] = []
        # Convert to Fill objects.
        for order_id in filled_order_ids:
            order_resp = order_responses[order_id][0]
            order = order_responses[order_id][1]
            end_timestamp = order.end_timestamp
            num_shares = order.diff_num_shares
            price = order_resp["price"]
            fill = oms.Fill(order, end_timestamp, num_shares, price=price)
            fills.append(fill)
        return fills

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into the exchange and return the corresponding `ccxt.Exchange`
        object.
        """
        # Select credentials for provided exchange.
        if self._mode == "test":
            secrets_id = self._exchange_id + "_sandbox"
        else:
            secrets_id = self._exchange_id
        exchange_params = hsecret.get_secret(secrets_id)
        # Enable rate limit.
        exchange_params["rateLimit"] = True
        # Log into futures/spot market.
        if self._contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(ccxt, self._exchange_id)
        exchange = ccxt_exchange(exchange_params)
        if self._mode == "test":
            exchange.set_sandbox_mode(True)
            _LOG.warning("Running in sandbox mode")
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        return exchange

    def _assert_order_methods_presence(self) -> None:
        """
        Assert that the requested exchange supports all methods necessary to
        make placing/fetching orders possible.
        """
        methods = ["createOrder", "fetchClosedOrders"]
        abort = False
        for method in methods:
            if not self._exchange.has[method]:
                _LOG.error(
                    "Method %s is unsupported for %s.", method, self._exchange_id
                )
                abort = True
        if abort:
            raise ValueError(
                f"The {self._exchange_id} exchange is not fully supported for placing orders."
            )

    def _build_asset_id_to_symbol_mapping(
        self, universe_version: str
    ) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.
        """
        # Get full symbol universe.
        full_symbol_universe = ivcu.get_vendor_universe(
            "CCXT", "trade", version=universe_version, as_full_symbol=True
        )
        # Filter symbols of the exchange corresponding to this instance.
        full_symbol_universe = list(
            filter(
                lambda s: s.startswith(self._exchange_id), full_symbol_universe
            )
        )
        # Build mapping.
        asset_id_to_full_symbol_mapping = (
            ivcu.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        # Change mapped values to be symbol only (more convevient when placing orders)
        asset_id_to_symbol_mapping = {
            id_: ivcu.parse_full_symbol(fs)[1].replace("_", "/")
            for id_, fs in asset_id_to_full_symbol_mapping.items()
        }
        return asset_id_to_symbol_mapping

    # TODO(Danya): We can check here that the order was accepted and just don't do
    # anything in the wait_for_accepted_orders. At this point we even don't have to
    # write in the DB.
    async def _submit_orders(
        self,
        orders: List[oms.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool = False,
    ) -> str:
        """
        Submit the orders to the exchange.

        The output is exchange-given order ID mapped to exchange response and order.

        Example of the output:

        {'3057818287': ({'info': {'orderId': '3057818287',
                        'symbol': 'BTCUSDT',
                        'status': 'FILLED',
                        'clientOrderId': '***REMOVED***',
                        'price': '0',
                        'avgPrice': '20045.90000',
                        'origQty': '0.010',
                        'executedQty': '0.010',
                        'cumQty': '0.010',
                        'cumQuote': '200.45900',
                        'timeInForce': 'GTC',
                        'type': 'MARKET',
                        'reduceOnly': False,
                        'closePosition': False,
                        'side': 'BUY',
                        'positionSide': 'BOTH',
                        'stopPrice': '0',
                        'workingType': 'CONTRACT_PRICE',
                        'priceProtect': False,
                        'origType': 'MARKET',
                        'updateTime': '1657087032135'},
                        'id': '3057818287',
                        'clientOrderId': '***REMOVED***',
                        'timestamp': None,
                        'datetime': None,
                        'lastTradeTimestamp': None,
                        'symbol': 'BTC/USDT',
                        'type': 'market',
                        'timeInForce': 'IOC',
                        'postOnly': False,
                        'side': 'buy',
                        'price': 20045.9,
                        'stopPrice': None,
                        'amount': 0.01,
                        'cost': 200.459,
                        'average': 20045.9,
                        'filled': 0.01,
                        'remaining': 0.0,
                        'status': 'closed',
                        'fee': None,
                        'trades': [],
                        'fees': [],
                        'client_oid': 0,
                        'asset_id': 1467591036},
                        <oms.order.Order at 0x7f9344b74d30>)}

        :param orders: orders to be submitted
        :return: an exchange response
        """
        # Add an order to the submitted orders table.
        submitted_order_id = self._get_next_submitted_order_id()
        # Get a name for the file to be saved locally.
        local_file_name = self._get_file_name(
            wall_clock_timestamp,
            submitted_order_id,
        )
        order_responses: Dict[str, Dict[str, str]] = {}
        # Create a submitted orders row.
        orders_as_txt = oms.orders_to_string(orders)
        # Submit orders to exchange.
        if dry_run:
            _LOG.warning("Not submitting orders because of dry_run")
        else:
            for order in orders:
                symbol = self._asset_id_to_symbol_mapping[order.asset_id]
                side = "buy" if order.diff_num_shares > 0 else "sell"
                # Get the order response.
                order_resp = self._exchange.createOrder(
                    symbol=symbol,
                    type=order.type_,
                    side=side,
                    amount=abs(order.diff_num_shares),
                )
                # Add internal order ID to the response.
                order_resp["client_oid"] = order.order_id
                # Add symbol id to the response.
                order_resp["asset_id"] = order.asset_id
                order_responses[order_resp["id"]] = tuple([order_resp, order])
                _LOG.info(order_resp)
                hio.to_file(local_file_name, orders_as_txt)
                # Add execution time of the order.
                execution_ts = int(order_resp["info"]["updateTime"])
                self.last_order_execution_ts = (
                    hdateti.convert_unix_epoch_to_timestamp(execution_ts)
                )
        return order_responses

    def _get_file_name(
        self,
        curr_timestamp: pd.Timestamp,
        order_id: int,
    ) -> str:
        """
        Get the S3 file name for orders.

        The file should look like:
        ccxt_submitted_orders/20220704000000/order.0.20220704_190819.txt

        :param curr_timestamp: timestamp for the file name
        :param order_id: internal ID of the order
        :return: order file name
        """
        dst_dir = "ccxt_submitted_orders/"
        # E.g., "ccxt_submitted_orders/YYYYMMDD000000/<filename>.txt"
        date_dir = curr_timestamp.strftime("%Y%m%d")
        date_dir += "0" * 6
        # Add a timestamp for readability.
        curr_timestamp_as_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"order_{order_id}.{curr_timestamp_as_str}.txt"
        file_name = os.path.join(dst_dir, date_dir, file_name)
        _LOG.debug("file_name=%s", file_name)
        return file_name

    async def _wait_for_accepted_orders(self, file_name: str) -> None:
        _ = file_name
