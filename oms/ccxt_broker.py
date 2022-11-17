"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import ccxt
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hlogging as hloggin
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsecrets as hsecret
import im_v2.common.universe.full_symbol as imvcufusy
import im_v2.common.universe.universe as imvcounun
import im_v2.common.universe.universe_utils as imvcuunut
import market_data as mdata
import oms.broker as ombroker
import oms.hsecrets as omssec
import oms.order as omorder

_LOG = logging.getLogger(__name__)

# Max number of order submission retries.
_MAX_ORDER_SUBMIT_RETRIES = 3


# #############################################################################
# CcxtBroker
# #############################################################################


class CcxtBroker(ombroker.Broker):
    def __init__(
        self,
        exchange_id: str,
        # TODO(gp): move this to `Broker` and assign a default value or wire it
        #  everywhere. IMO default value is a better approach.
        universe_version: str,
        # TODO(gp): move this to Broker.
        stage: str,
        account_type: str,
        portfolio_id: str,
        contract_type: str,
        # TODO(gp): @all *args should go first according to our convention of
        #  appending params to the parent class constructor.
        secret_identifier: omssec.SecretIdentifier,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Constructor.

        :param exchange_id: name of the exchange to initialize the broker for
            (e.g., Binance)
        :param universe_version: version of the universe to use
        :param stage:
            - "preprod" preproduction stage
            - "local" debugging stage
        :param account_type:
            - "trading" launches the broker in trading environment
            - "sandbox" launches the broker in sandbox environment (not supported for
              every exchange)
        :param contract_type: "spot" or "futures"
        :param secret_identifier: a SecretIdentifier holding a full name of secret to look for in
         AWS SecretsManager
        """
        super().__init__(*args, **kwargs)
        self.max_order_submit_retries = _MAX_ORDER_SUBMIT_RETRIES
        self._exchange_id = exchange_id
        #
        hdbg.dassert_in(stage, ["local", "preprod"])
        self.stage = stage
        hdbg.dassert_in(account_type, ["trading", "sandbox"])
        self._account_type = account_type
        _LOG.debug("secret_identifier=%s", secret_identifier)
        self._secret_identifier = secret_identifier
        _LOG.warning("secret_identifier=%s", secret_identifier)
        # TODO(Juraj): not sure how to generalize this coinbasepro-specific parameter.
        self._portfolio_id = portfolio_id
        #
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._contract_type = contract_type
        #
        self._exchange = self._log_into_exchange()
        self._assert_order_methods_presence()
        # TODO(gp): @all -> Move this to the `Broker` class.
        # Enable mapping back from asset ids when placing orders.
        self._universe_version = universe_version
        self._asset_id_to_symbol_mapping = (
            self._build_asset_id_to_symbol_mapping()
        )
        self._symbol_to_asset_id_mapping = {
            symbol: asset
            for asset, symbol in self._asset_id_to_symbol_mapping.items()
        }
        # Set minimal order limits.
        self.market_info = self._get_market_info()
        # Extract info about max leverage.
        self._get_symbol_to_max_leverage_mapping()
        # Used to determine timestamp since when to fetch orders.
        self.last_order_execution_ts: Optional[pd.Timestamp] = None
        # Set up empty sent orders for the first run of the system.
        self._sent_orders = None

    def get_fills(self) -> List[ombroker.Fill]:
        """
        Return list of fills from the last order execution.

        :return: a list of filled orders
        """
        # Load previously sent orders from class state.
        sent_orders = self._sent_orders
        fills: List[ombroker.Fill] = []
        if sent_orders is None:
            return fills
        _LOG.info("Inside asset_ids")
        asset_ids = [sent_order.asset_id for sent_order in sent_orders]
        _LOG.info(hprint.to_str("asset_ids"))
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
                    _LOG.debug(hprint.to_str("order"))
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
                        filled_order = self._convert_ccxt_order_to_oms_order(
                            filled_order
                        )
                        _LOG.debug(hprint.to_str("filled_order"))
                        # Create a Fill object.
                        fill = ombroker.Fill(
                            filled_order,
                            hdateti.convert_unix_epoch_to_timestamp(
                                order["timestamp"]
                            ),
                            num_shares=filled_order.diff_num_shares,
                            price=order["price"],
                        )
                        fills.append(fill)
                        _LOG.debug(hprint.to_str("fill"))
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
        with a non-zero amount.
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
        _LOG.debug("fetched_positions=%s", positions)
        open_positions = []
        for position in positions:
            _LOG.debug("fetched_position=%s", position)
            # Get the quantity of assets on short/long positions.
            position_amount = float(position["info"]["positionAmt"])
            _LOG.debug("After rounding: fetched_position=%s", position)
            if position_amount != 0:
                open_positions.append(position)
        return open_positions

    def get_fills_for_time_period(
        self, start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp
    ) -> List[Dict[str, Any]]:
        """
        Get a list of fills for given time period in JSON format.

        The time period is treated as [a, b].
        Note that in case of longer time periods (>24h) the pagination
        is done by day, which can lead to more data being downloaded than expected.

        Example of output:
        {'info': {'symbol': 'ETHUSDT',
                 'id': '2271885264',
                 'orderId': '8389765544333791328',
                 'side': 'SELL',
                 'price': '1263.68',
                 'qty': '0.016',
                 'realizedPnl': '-3.52385454',
                 'marginAsset': 'USDT',
                 'quoteQty': '20.21888',
                 'commission': '0.00808755',
                 'commissionAsset': 'USDT',
                 'time': '1663859837554',
                 'positionSide': 'BOTH',
                 'buyer': False,
                 'maker': False},
        'timestamp': 1663859837554,
        'datetime': '2022-09-22T15:17:17.554Z',
        'symbol': 'ETH/USDT',
        'id': '2271885264',
        'order': '8389765544333791328',
        'type': None,
        'side': 'sell',
        'takerOrMaker': 'taker',
        'price': 1263.68,
        'amount': 0.016,
        'cost': 20.21888,
        'fee': {'cost': 0.00808755, 'currency': 'USDT'},
        'fees': [{'currency': 'USDT', 'cost': 0.00808755}]}
        """
        hdbg.dassert_isinstance(start_timestamp, pd.Timestamp)
        hdbg.dassert_isinstance(end_timestamp, pd.Timestamp)
        hdbg.dassert_lte(start_timestamp, end_timestamp)
        symbols = list(self._symbol_to_asset_id_mapping.keys())
        fills = []
        start_timestamp = hdateti.convert_timestamp_to_unix_epoch(start_timestamp)
        end_timestamp = hdateti.convert_timestamp_to_unix_epoch(end_timestamp)
        # Get conducted trades (fills) symbol by symbol.
        for symbol in symbols:
            # Download all trades if period is less than 24 hours.
            # TODO(Danya): Maybe return a dataframe so we can trim the df
            #  at the output and avoid downloading extra data?
            if end_timestamp - start_timestamp < 86400000:
                _LOG.debug(
                    "Downloading period=%s, %s", start_timestamp, end_timestamp
                )
                symbol_fills = self._exchange.fetchMyTrades(
                    symbol=symbol,
                    since=start_timestamp,
                    params={"endTime": end_timestamp},
                )
                fills.extend(symbol_fills)
            # Download day-by-day for longer time periods.
            else:
                symbol_fills = []
                for timestamp in range(
                    start_timestamp, end_timestamp + 1, 86400000
                ):
                    _LOG.debug("Downloading period=%s, %s", timestamp, 86400000)
                    day_fills = self._exchange.fetchMyTrades(
                        symbol=symbol,
                        since=timestamp,
                        params={"endTime": timestamp + 86400000},
                    )
                    symbol_fills.extend(day_fills)
            # Add the asset ids to each fill.
            asset_id = self._symbol_to_asset_id_mapping[symbol]
            symbol_fills_with_asset_ids = []
            for item in symbol_fills:
                _LOG.debug("symbol_fill=%s", item)
                # Get the position of the full symbol field to paste the asset id after it.
                hdbg.dassert_in("symbol", item.keys())
                position = list(item.keys()).index("symbol") + 1
                items = list(item.items())
                items.insert(position, ("asset_id", asset_id))
                symbol_fills_with_asset_ids.append(dict(items))
                _LOG.debug("after transformation: symbol_fill=%s", item)
            fills.extend(symbol_fills_with_asset_ids)
        return fills

    @staticmethod
    def _convert_currency_pair_to_ccxt_format(currency_pair: str) -> str:
        """
        Convert full symbol to CCXT format.

        Example: "BTC_USDT" -> "BTC/USDT"
        """
        currency_pair = currency_pair.replace("_", "/")
        return currency_pair

    @staticmethod
    def _check_binance_code_error(e: Exception, error_code: int) -> bool:
        """
        Check if the exception matches the expected error code.

        Example of a Binance exception:
        {"code":-4131,
        "msg":"The counterparty's best price does not meet the PERCENT_PRICE filter limit."}

        :param e: Binance exception raised by CCXT
        :param error_code: expected error code, e.g. -4131
        :return: whether error code is contained in error message
        """
        error_message = str(e)
        error_regex = f'"code":{error_code},'
        return bool(re.search(error_regex, error_message))

    def _convert_ccxt_order_to_oms_order(
        self, ccxt_order: Dict[Any, Any]
    ) -> omorder.Order:
        """
        Convert sent CCXT orders to oms.Order class.

        Example of an input `ccxt_order`:
        ```
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
        ```
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
        # This is done since market orders are filled instantaneously.
        end_timestamp += pd.DateOffset(minutes=1)
        # Get the amount of shares in the filled order.
        diff_num_shares = ccxt_order["filled"]
        if ccxt_order["side"] == "sell":
            diff_num_shares = -diff_num_shares
        #
        # Get the amount of shares in the open position.
        #  Note: This corresponds to the amount of assets
        #  before the filled order has been executed.
        #
        symbol = self._asset_id_to_symbol_mapping[asset_id]
        # Select current position amount.
        curr_num_shares = self._exchange.fetch_positions([symbol])[0]
        _LOG.debug(
            "Before order has been filled: curr_num_shares=%s", curr_num_shares
        )
        curr_num_shares = float(curr_num_shares["info"]["positionAmt"])
        # Convert the retrieved string and round.
        amount_precision = self.market_info[asset_id]["amount_precision"]
        curr_num_shares = round(curr_num_shares, amount_precision)
        # Calculate position before the order has been filled.
        curr_num_shares = curr_num_shares - diff_num_shares
        _LOG.debug(
            "After order has been filled: curr_num_shares=%s", curr_num_shares
        )
        oms_order = omorder.Order(
            creation_timestamp,
            asset_id,
            type_,
            start_timestamp,
            end_timestamp,
            curr_num_shares,
            diff_num_shares,
        )
        _LOG.debug("After CCXT to OMS transform:")
        _LOG.debug("oms_order=%s", str(oms_order))
        return oms_order

    # TODO(Grisha): extend the functionality to work with different
    # leverage tiers.
    def _get_symbol_to_max_leverage_mapping(self) -> Dict[str, int]:
        """
        Get a maximum leverage for each coin.

        On binance leverage depends on a coin and on
        a position size.

        For now it is assumed that all positions belong to the lowest
        tier.
        """
        symbols = list(self._symbol_to_asset_id_mapping.keys())
        # See more more about the output format:
        # https://docs.ccxt.com/en/latest/manual.html#leverage-tiers-structure.
        leverage_info = self._exchange.fetchLeverageTiers(symbols)
        symbol_to_max_leverage = {}
        for symbol in symbols:
            # Select the lowest tier.
            tier_0_leverage_info = leverage_info[symbol][0]
            # Convert max leverage to int, raw value is a float.
            max_leverage = int(tier_0_leverage_info["maxLeverage"])
            symbol_to_max_leverage[symbol] = max_leverage
        return symbol_to_max_leverage

    def _get_market_info(self) -> Dict[int, Any]:
        """
        Load market information from the given exchange and map to asset ids.

        Currently the following data is saved:
        - minimal order limits (notional and quantity)
        - asset quantity precision (for rounding of orders)

        The numbers are determined by loading the market metadata from CCXT.

        Example:
        {'active': True,
        'base': 'ADA',
        'baseId': 'ADA',
        'contract': True,
        'contractSize': 1.0,
        'delivery': False,
        'expiry': None,
        'expiryDatetime': None,
        'feeSide': 'get',
        'future': True,
        'id': 'ADAUSDT',
        'info': {'baseAsset': 'ADA',
                'baseAssetPrecision': '8',
                'contractType': 'PERPETUAL',
                'deliveryDate': '4133404800000',
                'filters': [{'filterType': 'PRICE_FILTER',
                            'maxPrice': '25.56420',
                            'minPrice': '0.01530',
                            'tickSize': '0.00010'},
                            {'filterType': 'LOT_SIZE',
                            'maxQty': '10000000',
                            'minQty': '1',
                            'stepSize': '1'},
                            {'filterType': 'MARKET_LOT_SIZE',
                            'maxQty': '10000000',
                            'minQty': '1',
                            'stepSize': '1'},
                            {'filterType': 'MAX_NUM_ORDERS', 'limit': '200'},
                            {'filterType': 'MAX_NUM_ALGO_ORDERS', 'limit': '10'},
                            {'filterType': 'MIN_NOTIONAL', 'notional': '10'},
                            {'filterType': 'PERCENT_PRICE',
                            'multiplierDecimal': '4',
                            'multiplierDown': '0.9000',
                            'multiplierUp': '1.1000'}],
                'liquidationFee': '0.020000',
                'maintMarginPercent': '2.5000',
                'marginAsset': 'USDT',
                'marketTakeBound': '0.10',
                'onboardDate': '1569398400000',
                'orderTypes': ['LIMIT',
                                'MARKET',
                                'STOP',
                                'STOP_MARKET',
                                'TAKE_PROFIT',
                                'TAKE_PROFIT_MARKET',
                                'TRAILING_STOP_MARKET'],
                'pair': 'ADAUSDT',
                'pricePrecision': '5',
                'quantityPrecision': '0',
                'quoteAsset': 'USDT',
                'quotePrecision': '8',
                'requiredMarginPercent': '5.0000',
                'settlePlan': '0',
                'status': 'TRADING',
                'symbol': 'ADAUSDT',
                'timeInForce': ['GTC', 'IOC', 'FOK', 'GTX'],
                'triggerProtect': '0.0500',
                'underlyingSubType': ['HOT'],
                'underlyingType': 'COIN'},
        'inverse': False,
        'limits': {'amount': {'max': 10000000.0, 'min': 1.0},
                    'cost': {'max': None, 'min': 10.0},
                    'leverage': {'max': None, 'min': None},
                    'market': {'max': 10000000.0, 'min': 1.0},
                    'price': {'max': 25.5642, 'min': 0.0153}},
        'linear': True,
        'lowercaseId': 'adausdt',
        'maker': 0.0002,
        'margin': False,
        'option': False,
        'optionType': None,
        'percentage': True,
        'precision': {'amount': 0, 'base': 8, 'price': 4, 'quote': 8},
        'quote': 'USDT',
        'quoteId': 'USDT',
        'settle': 'USDT',
        'settleId': 'USDT',
        'spot': False,
        'strike': None,
        'swap': True,
        'symbol': 'ADA/USDT',
        'taker': 0.0004,
        'tierBased': False,
        'type': 'future'}
        """
        minimal_order_limits: Dict[int, Any] = {}
        # Load market information from CCXT.
        exchange_markets = self._exchange.load_markets()
        for asset_id, symbol in self._asset_id_to_symbol_mapping.items():
            minimal_order_limits[asset_id] = {}
            currency_market = exchange_markets[symbol]
            limits = currency_market["limits"]
            # Get the minimal amount of asset in the order.
            amount_limit = limits["amount"]["min"]
            minimal_order_limits[asset_id]["min_amount"] = amount_limit
            # Set the minimal cost of asset in the order.
            #  Note: the notional limit can differ between symbols
            #  and subject to fluctuations, so it is set manually to 10.
            notional_limit = 10.0
            minimal_order_limits[asset_id]["min_cost"] = notional_limit
            # Set the rounding precision for amount of the asset.
            amount_precision = currency_market["precision"]["amount"]
            minimal_order_limits[asset_id]["amount_precision"] = amount_precision
        return minimal_order_limits

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

    async def _submit_single_order(
        self, order: omorder.Order
    ) -> Optional[omorder.Order]:
        """
        Submit a single order.

        :param order: order to be submitted

        :return: order with ccxt ID appended if the submission was successful, None otherwise.
        """
        submitted_order: Optional[omorder.Order] = None
        symbol = self._asset_id_to_symbol_mapping[order.asset_id]
        side = "buy" if order.diff_num_shares > 0 else "sell"
        position_size = abs(order.diff_num_shares)
        # Get max leverage for a given order.
        max_leverage = self._symbol_to_max_leverage[symbol]
        # TODO(Juraj): separate the retry logic from the code that does the work.
        for _ in range(self.max_order_submit_retries):
            try:
                # Make sure that leverage is within the acceptable range
                # before submitting the order.
                _LOG.debug(
                    "Max leverage for symbol=%s and position size=%s is set to %s",
                    symbol,
                    position_size,
                    max_leverage,
                )
                self._exchange.setLeverage(max_leverage, symbol)
                _LOG.debug("Submitting order=%s", str(order))
                order_resp = self._exchange.createOrder(
                    symbol=symbol,
                    type="market",
                    side=side,
                    amount=position_size,
                    # id = order.order_id,
                    # id=order.order_id,
                    # TODO(Juraj): maybe it is possible to somehow abstract this to a general behavior
                    # but most likely the method will need to be overriden per each exchange
                    # to accommodate endpoint specific behavior.
                    params={
                        "portfolio_id": self._portfolio_id,
                        "client_oid": order.order_id,
                    },
                )
                _LOG.debug("CCXT order response order_resp=%s", order_resp)
                submitted_order = order
                submitted_order.ccxt_id = order_resp["id"]
                # If the submission was successful, don't retry.
                break
            except Exception as e:
                # Check the Binance API error
                if isinstance(e, ccxt.ExchangeNotAvailable):
                    # If there is a temporary server error, wait for
                    # a set amount of seconds and retry.
                    time.sleep(3)
                    continue
                if self._check_binance_code_error(e, -4131):
                    # If the error is connected to liquidity, continue submitting orders.
                    _LOG.warning(
                        "PERCENT_PRICE error has been raised. \
                        The Exception was:\n%s\nContinuing...",
                        e,
                    )
                    break
                else:
                    raise e
        return submitted_order

    async def _submit_orders(
        self,
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> Tuple[str, pd.DataFrame]:
        """
        Submit orders.
        """
        self.last_order_execution_ts = pd.Timestamp.now()
        sent_orders: List[omorder.Order] = []
        for order in orders:
            sent_order = await self._submit_single_order(order)
            # If order was submitted successfully append it to
            # the list of sent orders.
            if sent_order:
                sent_orders.append(sent_order)
        # Save sent CCXT orders to class state.
        self._sent_orders = sent_orders
        # TODO(Grisha): what should we use as `receipt` for CCXT? For equities IIUC
        # we use a file name.
        submitted_order_id = self._get_next_submitted_order_id()
        receipt = f"filename_{submitted_order_id}.txt"
        # Combine all orders in a df.
        order_dicts = [order.to_dict() for order in sent_orders]
        order_df = pd.DataFrame(order_dicts)
        _LOG.debug("order_df=%s", hpandas.df_to_str(order_df))
        return receipt, order_df

    def _build_asset_id_to_symbol_mapping(
        self,
    ) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.

        E.g.,
        ```
        {
            1528092593: 'BAKE/USDT',
            8968126878: 'BNB/USDT',
            1182743717: 'BTC/BUSD',
        }
        ```
        """
        # Get full symbol universe.
        full_symbol_universe = imvcounun.get_vendor_universe(
            "CCXT", "trade", version=self._universe_version, as_full_symbol=True
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
        Log into the exchange and return the `ccxt.Exchange` object.
        """
        secrets_id = str(self._secret_identifier)
        # Select credentials for provided exchange.
        exchange_params = hsecret.get_secret(secrets_id)
        # Enable rate limit.
        exchange_params["rateLimit"] = True
        # Log into futures/spot market.
        if self._contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(ccxt, self._exchange_id)
        exchange = ccxt_exchange(exchange_params)
        # TODO(Juraj): extract all exchange specific configs into separate function.
        if self._exchange_id == "binance":
            # Necessary option to avoid time out of sync error
            # (CmTask2670 Airflow system run error "Timestamp for this
            # request is outside of the recvWindow.")
            exchange.options["adjustForTimeDifference"] = True
        if self._account_type == "sandbox":
            exchange.set_sandbox_mode(True)
            _LOG.warning("Running in sandbox mode")
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        # CCXT registers the logger after it's built, so we need to reduce its
        # logger verbosity here.
        hloggin.shutup_chatty_modules()
        return exchange


def get_CcxtBroker_prod_instance1(
    market_data: mdata.MarketData,
    universe_version: str,
    strategy_id: str,
    secret_identifier: omssec.SecretIdentifier,
) -> CcxtBroker:
    """
    Build an `CcxtBroker` for production.
    """
    exchange_id = secret_identifier.exchange_id
    stage = secret_identifier.stage
    account_type = secret_identifier.account_type
    contract_type = "futures"
    portfolio_id = "ccxt_portfolio_1"
    broker = CcxtBroker(
        exchange_id,
        universe_version,
        stage,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier,
        strategy_id=strategy_id,
        market_data=market_data,
    )
    return broker


# #############################################################################
# SimulatedCcxtBroker
# #############################################################################


class SimulatedCcxtBroker(ombroker.SimulatedBroker):
    def __init__(
        self,
        *args: Any,
        stage: str,
        market_info: Dict[int, float],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.stage = stage
        self.market_info = market_info


def get_SimulatedCcxtBroker_instance1(
    market_data: pd.DataFrame,
) -> ombroker.SimulatedBroker:
    market_info = load_market_data_info()
    stage = "preprod"
    strategy_id = "C1b"
    broker = SimulatedCcxtBroker(
        strategy_id,
        market_data,
        stage=stage,
        market_info=market_info,
    )
    return broker


def subset_market_info(
    market_info: Dict[int, Dict[str, Union[float, int]]], info_type: str
) -> Dict[int, Union[float, int]]:
    """
    Return only the relevant information from market info, e.g., info about
    precision.
    """
    # It is assumed that every asset has the same info type structure.
    available_info = list(market_info.values())[0].keys()
    hdbg.dassert_in(info_type, available_info)
    market_info_keys = list(market_info.keys())
    _LOG.debug("market_info keys=%s", market_info_keys)
    asset_ids_to_decimals = {
        key: market_info[key][info_type] for key in market_info_keys
    }
    return asset_ids_to_decimals


def load_market_data_info() -> Dict[int, Dict[str, Union[float, int]]]:
    """
    Load pre-saved market info.

    The data looks like:
    {"6051632686":
        {"min_amount": 1.0, "min_cost": 10.0, "amount_precision": 3},
     ...
    """
    file_path = os.path.join(
        hgit.get_amp_abs_path(),
        "oms/test/outcomes/TestSaveMarketInfo/input/binance.market_info.json",
    )
    market_info = hio.from_json(file_path)
    # Convert to int, because asset ids are strings.
    market_info = {int(k): v for k, v in market_info.items()}
    return market_info
