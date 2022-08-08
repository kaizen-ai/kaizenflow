"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import logging
from typing import Any, Dict, List, Optional

import ccxt
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret
import im_v2.common.universe.full_symbol as imvcufusy
import im_v2.common.universe.universe as imvcounun
import im_v2.common.universe.universe_utils as imvcuunut
import oms.broker as ombroker
import oms.order as omorder

_LOG = logging.getLogger(__name__)


class CcxtBroker(ombroker.Broker):
    def __init__(
        self,
        exchange_id: str,
        universe_version: str,
        mode: str,
        portfolio_id: str,
        contract_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        """
        Constructor.

        :param exchange: name of the exchange to initialize the broker for
        :param universe_version: version of the universe to use
        :param mode: supported values: "test", "prod", if "test", launches the broker
         in sandbox environment (not supported for every exchange),
         if "prod" launches with production API.
        :param contract_type: "spot" or "futures"
        """
        hdbg.dassert_in(mode, ["prod", "test"])
        self._mode = mode
        self._exchange_id = exchange_id
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._contract_type = contract_type
        self._exchange = self._log_into_exchange()
        self._assert_order_methods_presence()
        # Enable mapping back from asset ids when placing orders.
        self._asset_id_to_symbol_mapping = self._build_asset_id_to_symbol_mapping(
            universe_version
        )
        self._symbol_to_asset_id_mapping = {
            symbol: asset
            for asset, symbol in self._asset_id_to_symbol_mapping.items()
        }
        # Will be used to determine timestamp since when to fetch orders.
        self.last_order_execution_ts: Optional[pd.Timestamp] = None
        # TODO(Juraj): not sure how to generalize this coinbasepro-specific parameter.
        self._portfolio_id = portfolio_id

    @staticmethod
    def convert_ccxt_order_to_oms_order(
        ccxt_order: Dict[Any, Any]
    ) -> omorder.Order:
        """
        Convert sent CCXT orders to oms.Order class.

        Example of an input:

        {'info': {'orderId': '3101620940',
                'symbol': 'BTCUSDT',
                'status': 'FILLED',
                'clientOrderId': '***REMOVED***',
                'price': '0',
                'avgPrice': '23480.20000',
                'origQty': '0.001',
                'executedQty': '0.001',
                'cumQuote': '23.48020',
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
                'time': '1659465769012',
                'updateTime': '1659465769012'},
        'id': '3101620940',
        'clientOrderId': '***REMOVED***',
        'timestamp': 1659465769012,
        'datetime': '2022-08-02T18:42:49.012Z',
        'lastTradeTimestamp': None,
        'symbol': 'BTC/USDT',
        'type': 'market',
        'timeInForce': 'IOC',
        'postOnly': False,
        'side': 'buy',
        'price': 23480.2,
        'stopPrice': None,
        'amount': 0.001,
        'cost': 23.4802,
        'average': 23480.2,
        'filled': 0.001,
        'remaining': 0.0,
        'status': 'closed',
        'fee': None,
        'trades': [],
        'fees': [],
        'asset_id': 1467591036}
        """
        asset_id = ccxt_order["asset_id"]
        type_ = "market"
        # Select creation and start date.
        creation_timestamp = hdateti.convert_unix_epoch_to_timestamp(
            ccxt_order["timestamp"]
        )
        start_timestamp = creation_timestamp
        # Get an offset end timestamp.
        #  Note: `updateTime` is the timestamp of the latest order status change,
        #  so for filled orders this is a moment when the order is filled.
        end_timestamp = hdateti.convert_unix_epoch_to_timestamp(
            int(ccxt_order["info"]["updateTime"])
        )
        # Add 1 minute to end timestamp.
        # This is done since in CCXT testnet the orders are filled instantaneously.
        end_timestamp += pd.DateOffset(minutes=1)
        # Get the amount of shares filled.
        curr_num_shares = float(ccxt_order["info"]["origQty"])
        diff_num_shares = ccxt_order["filled"]
        oms_order = omorder.Order(
            creation_timestamp,
            asset_id,
            type_,
            start_timestamp,
            end_timestamp,
            curr_num_shares,
            diff_num_shares,
        )
        return oms_order

    def get_fills(self) -> List[ombroker.Fill]:
        """
        Return list of fills from the last order execution.

        :return: a list of filled orders
        """
        # Load previously sent orders from class state.
        sent_orders = self._sent_orders
        fills: List[ombroker.Fill] = []
        _LOG.info("Inside asset_ids")
        asset_ids = [sent_order.asset_id for sent_order in sent_orders]
        if self.last_order_execution_ts:
            # Load orders for each given symbol.
            for asset_id in asset_ids:
                symbol = self._asset_id_to_symbol_mapping[asset_id]
                orders = self._exchange.fetch_orders(
                    since=hdateti.convert_timestamp_to_unix_epoch(
                        self.last_order_execution_ts,
                    ),
                    symbol=symbol,
                )
                # Select closed orders.
                for order in orders:
                    if order["status"] == "closed":
                        # Select order matching to CCXT exchange id.
                        filled_order = [
                            order
                            for sent_order in sent_orders
                            if sent_order.ccxt_id == order["id"]
                        ][0]
                        # Assign an `asset_id` to the filled order.
                        filled_order["asset_id"] = asset_id
                        # Convert CCXT `dict` order to oms.Order.
                        #  TODO(Danya): bind together self._sent_orders and CCXT response
                        #  so we can avoid this conversion while keeping the fill status.
                        filled_order = self.convert_ccxt_order_to_oms_order(
                            filled_order
                        )
                        # Create a Fill object.
                        fill = ombroker.Fill(
                            filled_order,
                            hdateti.convert_unix_epoch_to_timestamp(
                                order["timestamp"]
                            ),
                            num_shares=order["amount"],
                            price=order["price"],
                        )
                        fills.append(fill)
        return fills

    def get_total_balance(self) -> Dict[str, float]:
        """
        Fetch total available balance via CCXT.

        Example of total balance output:

        {'BNB': 0.0, 'USDT': 5026.22494667, 'BUSD': 1000.10001}

        :return: total balance
        """
        hdbg.dassert(self._exchange.has["fetchBalance"], msg="")
        # Fetch all balance data.
        balance = self._exchange.fetchBalance()
        # Select total balance.
        total_balance = balance["total"]
        return total_balance

    def get_open_positions(self) -> List[Dict[Any, Any]]:
        """
        Select all open futures positions.

        Selects all possible positions and filters out those
        with a non-0 amount.
        Example of an output:

        [{'info': {'symbol': 'BTCUSDT',
            'positionAmt': '-0.200',
            'entryPrice': '23590.549',
            'markPrice': '23988.40000000',
            'unRealizedProfit': '-79.57020000',
            'liquidationPrice': '68370.47429432',
            'leverage': '20',
            'maxNotionalValue': '250000',
            'marginType': 'cross',
            'isolatedMargin': '0.00000000',
            'isAutoAddMargin': 'false',
            'positionSide': 'BOTH',
            'notional': '-4797.68000000',
            'isolatedWallet': '0',
            'updateTime': '1659028521933'},
            'symbol': 'BTC/USDT',
            'contracts': 0.2,
            'contractSize': 1.0,
            'unrealizedPnl': -79.5702,
            'leverage': 20.0,
            'liquidationPrice': 68370.47429432,
            'collateral': 9092.72600745,
            'notional': 4797.68,
            'markPrice': 23988.4,
            'entryPrice': 23590.549,
            'timestamp': 1659028521933,
            'initialMargin': 239.884,
            'initialMarginPercentage': 0.05,
            'maintenanceMargin': 47.9768,
            'maintenanceMarginPercentage': 0.01,
            'marginRatio': 0.0053,
            'datetime': '2022-07-28T17:15:21.933Z',
            'marginType': 'cross',
            'side': 'short',
            'hedged': False,
            'percentage': -33.17}]

        :return: open positions at the exchange.
        """
        hdbg.dassert(
            self._contract_type == "futures",
            "Open positions can be fetched only for futures contracts.",
        )
        # Fetch all open positions.
        positions = self._exchange.fetchPositions()
        open_positions = []
        for position in positions:
            # Get the quantity of assets on short/long positions.
            position_amount = float(position["info"]["positionAmt"])
            if position_amount != 0:
                open_positions.append(position)
        return open_positions

    @staticmethod
    def _convert_currency_pair_to_ccxt_format(currency_pair: str) -> str:
        """
        Convert full symbol to CCXT format.

        Example: "BTC_USDT" -> "BTC/USDT"
        """
        currency_pair = currency_pair.replace("_", "/")
        return currency_pair

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

    async def _submit_orders(
        self,
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> List[omorder.Order]:
        """
        Submit orders.
        """
        self.last_order_execution_ts = pd.Timestamp.now()
        sent_orders: List[str] = []
        for order in orders:
            # TODO(Juraj): perform bunch of assertions for order attributes.
            symbol = self._asset_id_to_symbol_mapping[order.asset_id]
            side = "buy" if order.diff_num_shares > 0 else "sell"
            order_resp = self._exchange.createOrder(
                symbol=symbol,
                type=order.type_,
                side=side,
                amount=abs(order.diff_num_shares),
                # id = order.order_id,
                # id=order.order_id,
                # TODO(Juraj): maybe it is possible to somehow abstract this to a general behavior
                # but most likely the method will need to be overriden per each exchange
                # to accomodate endpoint specific behavior.
                params={
                    "portfolio_id": self._portfolio_id,
                    "client_oid": order.order_id,
                },
            )
            order.ccxt_id = order_resp["id"]
            sent_orders.append(order)
            _LOG.info(order_resp)
        # Save sent CCXT orders to class state.
        self._sent_orders = sent_orders
        return None

    def _build_asset_id_to_symbol_mapping(
        self, universe_version: str
    ) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.

        Example:

        {
            1528092593: 'BAKE/USDT',
            8968126878: 'BNB/USDT',
            1182743717: 'BTC/BUSD',
        }
        """
        # Get full symbol universe.
        full_symbol_universe = imvcounun.get_vendor_universe(
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
            imvcuunut.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        asset_id_to_symbol_mapping: Dict[int, str] = {}
        for asset_id, symbol in asset_id_to_full_symbol_mapping.items():
            # Select currency pair.
            currency_pair = imvcufusy.parse_full_symbol(symbol)[1]
            # Transform to CCXT format, e.g. 'BTC_USDT' -> 'BTC/USDT'.
            ccxt_symbol = self._convert_currency_pair_to_ccxt_format(
                currency_pair
            )
            asset_id_to_symbol_mapping[asset_id] = ccxt_symbol
        return asset_id_to_symbol_mapping

    async def _wait_for_accepted_orders(
        self,
        file_name: str,
    ) -> None:
        _ = file_name

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into coinbasepro and return the corresponding `ccxt.Exchange`
        object.
        """
        # Select credentials for provided exchange.
        if self._mode == "test":
            secrets_id = self._exchange_id + "_sandbox"
        elif self._mode == "debug_test1":
            # TODO(Danya): Temporary mode for running debug script.
            #  See CMTask2575.
            secrets_id = self._exchange_id + "_debug_test1"
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
