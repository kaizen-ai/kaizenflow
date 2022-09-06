"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt_broker as occxbrok
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import ccxt
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret
import oms.secrets as omssec
import im_v2.common.universe.full_symbol as imvcufusy
import im_v2.common.universe.universe as imvcounun
import im_v2.common.universe.universe_utils as imvcuunut
import market_data as mdata
import oms.broker as ombroker
import oms.order as omorder

_LOG = logging.getLogger(__name__)

# Max number of order submission retries.
_MAX_ORDER_SUBMIT_RETRIES = 3


class CcxtBroker(ombroker.Broker):
    def __init__(
        self,
        exchange_id: str,
        universe_version: str,
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
        self._stage = stage
        hdbg.dassert_in(account_type, ["trading", "sandbox"])
        self._account_type = account_type
        self._secret_identifier = secret_identifier
        # TODO(Juraj): not sure how to generalize this coinbasepro-specific parameter.
        self._portfolio_id = portfolio_id
        #
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._contract_type = contract_type
        #
        self._exchange = self._log_into_exchange()
        self._assert_order_methods_presence()
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
        self.minimal_order_limits = self._get_minimal_order_limits()
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
                        filled_order = self._convert_ccxt_order_to_oms_order(
                            filled_order
                        )
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
        curr_num_shares = float(curr_num_shares["info"]["positionAmt"])
        # Calculate position before the order has been filled.
        curr_num_shares = curr_num_shares - diff_num_shares
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

    def _force_minimal_order(self, order: omorder.Order) -> omorder.Order:
        """
        Force a minimal possible order quantity.

        Changes the order to buy/sell the minimal possible quantity of an asset.
        Required for running the system in testnet.

        :param order: order to be submitted
        :return: an order with minimal quantity of asset
        """
        asset_limits = self.minimal_order_limits[order.asset_id]
        required_amount = asset_limits["min_amount"]
        min_cost = asset_limits["min_cost"]
        # Get the low price for the asset.
        low_price = self.get_low_market_price(order.asset_id)
        # Verify that the estimated total cost is above 10.
        if low_price * required_amount <= min_cost:
            # Set the amount of asset to above min cost.
            #  Note: the multiplication by 2 is done to give some
            #  buffer so the order does not go below
            #  the minimal amount of asset.
            required_amount = (min_cost / low_price) * 2
        if order.diff_num_shares < 0:
            order.diff_num_shares = -required_amount
        else:
            order.diff_num_shares = required_amount
        return order

    def _check_order_limit(self, order: omorder.Order) -> omorder.Order:
        """
        Check if the order matches the minimum quantity for the asset.

        The functions check both the flat amount of the asset and the total
        cost of the asset in the order. If the order amount does not match,
        the order is changed to be slightly above the minimal amount.

        :param order: order to be submitted
        """
        # Load all limits for the asset.
        asset_limits = self.minimal_order_limits[order.asset_id]
        min_amount = asset_limits["min_amount"]
        if abs(order.diff_num_shares) < min_amount:
            if order.diff_num_shares < 0:
                min_amount = -min_amount
            _LOG.warning(
                "Order: %s\nAmount of asset in order is below minimal: %s. Setting to min amount: %s",
                str(order),
                order.diff_num_shares,
                min_amount,
            )
            order.diff_num_shares = min_amount
        # Check if the order is not below minimal cost.
        #
        # Estimate the total cost of the order based on the low market price.
        #  Note: low price is chosen to account for possible price spikes.
        low_price = self.get_low_market_price(order.asset_id)
        total_cost = low_price * abs(order.diff_num_shares)
        # Verify that the order total cost is not below minimum.
        min_cost = asset_limits["min_cost"]
        if total_cost <= min_cost:
            # Set amount based on minimal notional price.
            required_amount = round(min_cost * 3 / low_price, 2)
            if order.diff_num_shares < 0:
                required_amount = -required_amount
            _LOG.warning(
                "Order: %s\nAmount of asset in order is below minimal base: %s. \
                    Setting to following amount based on notional limit: %s",
                str(order),
                min_cost,
                required_amount,
            )
            # Change number of shares to minimal amount.
            order.diff_num_shares = required_amount
        return order

    def get_low_market_price(self, asset_id: int) -> float:
        """
        Load the low price for the given ticker.
        """
        symbol = self._asset_id_to_symbol_mapping[asset_id]
        last_price = self._exchange.fetch_ticker(symbol)["low"]
        return last_price

    def _get_minimal_order_limits(self) -> Dict[int, Any]:
        """
        Load minimal amount and total cost for the given exchange.

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
        minimal_order_limits: Dict[str, Any] = {}
        # Load market information from CCXT.
        exchange_markets = self._exchange.load_markets()
        for asset_id, symbol in self._asset_id_to_symbol_mapping.items():
            minimal_order_limits[asset_id] = {}
            limits = exchange_markets[symbol]["limits"]
            # Get the minimal amount of asset in the order.
            amount_limit = limits["amount"]["min"]
            minimal_order_limits[asset_id]["min_amount"] = amount_limit
            # Set the minimal cost of asset in the order.
            #  Note: the notional limit can differ between symbols
            #  and subject to fluctuations, so it is set manually to 10.
            notional_limit = 10.0
            minimal_order_limits[asset_id]["min_cost"] = notional_limit
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
        # if self._stage == "local":
        #     # Reduce order to a minimal possible amount.
        #     #  This is done to avoid "Margin is insufficient" error
        #     #  in testnet.
        #     order = self._force_minimal_order(order)
        # # elif self._stage in ["preprod", "prod"]:
        # #     # Verify that order is not belo w the minimal amount.
        # #     order = self._check_order_limit(order)
        # # else:
        # #     raise ValueError(f"Stage `{self._stage}` is not valid!")
        symbol = self._asset_id_to_symbol_mapping[order.asset_id]
        side = "buy" if order.diff_num_shares > 0 else "sell"
        # TODO(Juraj): separate the retry logic from the code that does the work.
        for _ in range(self.max_order_submit_retries):
            try:
                order_resp = self._exchange.createOrder(
                    symbol=symbol,
                    type="market",
                    side=side,
                    amount=abs(order.diff_num_shares),
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
            _LOG.info("Submitting %s", str(order))
            sent_order = await self._submit_single_order(order)
            _LOG.info(str(sent_order))
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