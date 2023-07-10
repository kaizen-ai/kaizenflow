"""
An abstract base class class CCXT broker.

Import as:

import oms.ccxt.abstract_ccxt_broker as ocabccbr
"""

import abc
import asyncio
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import ccxt
import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hlogging as hloggin
import helpers.hprint as hprint
import helpers.hsecrets as hsecret
import helpers.hwall_clock_time as hwacltim
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu
import market_data as mdata
import oms.broker as ombroker
import oms.ccxt.ccxt_utils as occccuti
import oms.hsecrets as omssec
import oms.order as omorder

_LOG = logging.getLogger(__name__)

# Max number of order submission retries.
_MAX_ORDER_SUBMIT_RETRIES = 3

CcxtData = Dict[str, Any]

# #############################################################################
# AbstractCcxtBroker
# #############################################################################


class AbstractCcxtBroker(ombroker.Broker):
    def __init__(
        self,
        exchange_id: str,
        account_type: str,
        portfolio_id: str,
        contract_type: str,
        # TODO(gp): @all *args should go first according to our convention of
        #  appending params to the parent class constructor.
        secret_identifier: omssec.SecretIdentifier,
        *args: Any,
        # TODO(Grisha): consider passing StichedMarketData with OHLCV and bid /
        #  ask data instead of passing `market_data` and `bid_ask_im_client`
        #  separately.
        bid_ask_im_client: Optional[icdc.ImClient] = None,
        max_order_submit_retries: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """
        Constructor.

        :param exchange_id: name of the exchange to initialize the broker for
            (e.g., Binance)
        :param account_type:
            - "trading" launches the broker in trading environment
            - "sandbox" launches the broker in sandbox environment (not supported
               for every exchange)
        :param contract_type: "spot" or "futures"
        :param secret_identifier: a SecretIdentifier holding a full name of secret
            to look for in AWS SecretsManager
        :param bid_ask_im_client: ImClient that reads bid / ask data
            (required to calculate price for limit orders)
        :param max_order_submit_retries: maximum number of attempts to submit
            an order if the first try is unsuccessful
        :param *args: `ombroker.Broker` positional arguments
        :param **kwargs: `ombroker.Broker` keyword arguments
        """
        super().__init__(*args, **kwargs)
        if not self._log_dir:
            _LOG.warning(
                "No logging directory is provided, so not saving child orders or fills."
            )
        self.max_order_submit_retries = (
            max_order_submit_retries or _MAX_ORDER_SUBMIT_RETRIES
        )
        self._exchange_id = exchange_id
        #
        hdbg.dassert_in(account_type, ["trading", "sandbox"])
        self._account_type = account_type
        #
        self._secret_identifier = secret_identifier
        _LOG.warning("Using secret_identifier=%s", secret_identifier)
        self._portfolio_id = portfolio_id
        #
        hdbg.dassert_in(contract_type, ["spot", "futures"])
        self._contract_type = contract_type
        #
        self._exchange = self._log_into_exchange()
        # Enable mapping back from asset ids when placing orders.
        self.asset_id_to_ccxt_symbol_mapping = (
            self._build_asset_id_to_ccxt_symbol_mapping()
        )
        self.ccxt_symbol_to_asset_id_mapping = {
            symbol: asset
            for asset, symbol in self.asset_id_to_ccxt_symbol_mapping.items()
        }
        # Get info about the market.
        self.market_info = self._get_market_info()
        self.fees = self._get_trading_fee_info()
        # Store the timestamp of the previous (parent) orders.
        self.previous_parent_orders_timestamp: Optional[pd.Timestamp] = None
        # Store (parent) orders sent to the broker in the previous execution.
        # Initially the orders are empty.
        self._previous_parent_orders: Optional[List[omorder.Order]] = None
        # Initialize ImClient with bid / ask data if it is provided.
        self._bid_ask_im_client = bid_ask_im_client
        if self._bid_ask_im_client is not None:
            # Initialize MarketData with bid / ask data to calculate limit price.
            self._bid_ask_market_data = self._get_bid_ask_real_time_market_data()

    @staticmethod
    def log_oms_parent_orders(
        log_dir: str, 
        wall_clock_time: Callable, 
        oms_parent_orders: List[omorder.Order],
    ) -> None:
        """
        Log OMS parent orders before dividing them into child orders.

        The orders are logged before they are submitted, in the same format
        as they are passed from `TargetPositionAndOrderGenerator`.

        :param log_dir: dir to store logs in. The data structure looks like:
            ```
            {log_dir}/oms_parent_orders/oms_parent_orders_20230622-084000.20230622-084421.json
            ```
        :param wall_clock_time: the actual wall clock time of the running system
            for accounting
        :param oms_parent_orders: list of OMS parent orders
        """
        if log_dir:
            # Generate file name based on the bar timestamp.
            wall_clock_time = wall_clock_time().strftime("%Y%m%d-%H%M%S")
            bar_timestamp = hwacltim.get_current_bar_timestamp(as_str=True)
            oms_parent_orders_log_filename = (
                f"oms_parent_orders_{bar_timestamp}.{wall_clock_time}.json"
            )
            #
            enclosing_dir = os.path.join(log_dir, "oms_parent_orders")
            oms_parent_orders_log_filename = os.path.join(
                enclosing_dir, oms_parent_orders_log_filename
            )
            hio.create_enclosing_dir(
                oms_parent_orders_log_filename, incremental=True
            )
            # Serialize and save parent orders as JSON file.
            oms_parent_orders = [order.to_dict() for order in oms_parent_orders]
            hio.to_json(
                oms_parent_orders_log_filename, oms_parent_orders, use_types=True
            )
            _LOG.debug(hprint.to_str("oms_parent_orders_log_filename"))
        else:
            _LOG.debug("No log dir provided, skipping")

    @staticmethod
    def log_ccxt_fills(
        log_dir: str,
        wall_clock_time: Callable,
        ccxt_fills: List[CcxtData],
        ccxt_trades: List[CcxtData],
        oms_fills: List[ombroker.Fill],
    ) -> None:
        """
        Save fills and trades to separate files.

        The CCXT objects correspond to closed order (i.e., not in execution on
        the exchange any longer). They represent the cumulative fill for the
        closed order across all trades.

        The OMS fill objects correspond to `oms.Fill` objects before they are
        submitted upstream to the Portfolio (e.g., before child orders are
        merged into parent orders for accounting by the `Portfolio`).

        :param log_dir: dir to store logs in. The data structure looks like:
            ```
            {log_dir}/child_order_fills/ccxt_fills/ccxt_fills_20230515-112313.json
            {log_dir}/child_order_fills/ccxt_trades/ccxt_trades_20230515-112313.json
            {log_dir}/child_order_fills/oms_fills/oms_fills_20230515-112313.json
            ```
        :param wall_clock_time: the actual wall clock time of the running system
            for accounting
        :param ccxt_fills: list of CCXT fills loaded from CCXT
        :param ccxt_trades: list of CCXT trades corresponding to the CCXT fills
        :param oms_fills: list of `oms.Fill` objects
        """
        if log_dir:
            fills_log_dir = os.path.join(log_dir, "child_order_fills")
            hio.create_dir(fills_log_dir, incremental=True)
            # 1) Save CCXT fills, e.g.,
            # log_dir/child_order_fills/ccxt_fills/ccxt_fills_20230511-114405.json
            timestamp_str = wall_clock_time().strftime("%Y%m%d-%H%M%S")
            ccxt_fills_file_name = os.path.join(
                fills_log_dir, "ccxt_fills", f"ccxt_fills_{timestamp_str}.json"
            )
            _LOG.debug(hprint.to_str("ccxt_fills_file_name"))
            hio.to_json(ccxt_fills_file_name, ccxt_fills, use_types=True)
            # 2) Save CCXT trades, e.g.,
            # log_dir/child_order_fills/ccxt_trades/ccxt_trades_20230511-114405.json
            ccxt_trades_file_name = os.path.join(
                fills_log_dir, "ccxt_trades", f"ccxt_trades_{timestamp_str}.json"
            )
            _LOG.debug(hprint.to_str("ccxt_trades_file_name"))
            hio.to_json(ccxt_trades_file_name, ccxt_trades, use_types=True)
            # 3) Save OMS fills, e.g.,
            # log_dir/child_order_fills/oms_fills/oms_fills_20230511-114405.json
            if oms_fills:
                oms_fills = [fill.to_dict() for fill in oms_fills]
            oms_fills_file_name = os.path.join(
                fills_log_dir,
                "oms_fills",
                f"oms_fills_{timestamp_str}.json",
            )
            _LOG.debug(hprint.to_str("oms_fills_file_name"))
            hio.to_json(oms_fills_file_name, oms_fills, use_types=True)
        else:
            _LOG.debug("No log dir provided.")

    # TODO(Danya): Refactor to accept lists of OMS / CCXT child orders.
    @staticmethod
    def log_child_order(
        log_dir: str,
        wall_clock_time: Callable,
        oms_child_order: omorder.Order,
        ccxt_child_order_response: CcxtData,
        extra_info: CcxtData,
    ) -> None:
        """
        Log a child order with CCXT order info and additional parameters.

        :param log_dir: dir to store logs in
            OMS child order information is saved into a CSV file, while the
            corresponding order response from CCXT is saved into a JSON dir, in a
            format like:
                ```
                {log_dir}/oms_child_orders/...
                {log_dir}/ccxt_child_order_responses/...
                ```
        :param wall_clock_time: the actual wall clock time of the running system
            for accounting
        :param oms_child_order: an order to be logged
        :param ccxt_child_order_response: CCXT order structure from the exchange,
            corresponding to the child order
        :param extra_info: values to include into the logged order, for example
            {'bid': 0.277, 'ask': 0.279}
        """
        if log_dir:
            logged_oms_child_order = oms_child_order.to_dict()
            hdbg.dassert_not_intersection(
                logged_oms_child_order.keys(),
                extra_info.keys(),
                msg="There should be no overlapping keys",
            )
            logged_oms_child_order.update(extra_info)
            # Add CCXT id to the child order, -1 if there was no response.
            logged_oms_child_order["ccxt_id"] = logged_oms_child_order[
                "extra_params"
            ].get("ccxt_id", -1)
            incremental = True
            # Generate file name.
            wall_clock_time_str = wall_clock_time().strftime("%Y%m%d_%H%M%S")
            bar_timestamp = hwacltim.get_current_bar_timestamp(as_str=True)
            order_asset_id = logged_oms_child_order["asset_id"]
            # Save OMS child orders.
            oms_order_log_dir = os.path.join(log_dir, "oms_child_orders")
            hio.create_dir(oms_order_log_dir, incremental)
            oms_order_file_name = (
                f"{order_asset_id}_{bar_timestamp}.{wall_clock_time_str}.json"
            )
            oms_order_file_name = os.path.join(
                oms_order_log_dir, oms_order_file_name
            )
            hio.to_json(
                oms_order_file_name, logged_oms_child_order, use_types=True
            )
            _LOG.debug(
                "Saved OMS child orders log file %s",
                hprint.to_str("oms_order_file_name"),
            )
            # Save CCXT response.
            ccxt_log_dir = os.path.join(log_dir, "ccxt_child_order_responses")
            hio.create_dir(ccxt_log_dir, incremental)
            response_file_name = (
                f"{order_asset_id}_{bar_timestamp}.{wall_clock_time_str}.json"
            )
            response_file_name = os.path.join(ccxt_log_dir, response_file_name)
            hio.to_json(
                response_file_name, ccxt_child_order_response, use_types=True
            )
            _LOG.debug(
                "Saved CCXT child order response log file %s",
                hprint.to_str("response_file_name"),
            )
        else:
            _LOG.debug("No log dir provided, skipping")

    @abc.abstractmethod
    async def submit_twap_orders(
        self,
        parent_orders: List[omorder.Order],
        passivity_factor: float,
        *,
        execution_freq: Optional[str] = "1T",
    ) -> List[pd.DataFrame]:
        """
        Execute orders using the TWAP strategy.
        """
        ...

    def get_fills(self) -> List[ombroker.Fill]:
        """
        Return list of fills from the previous order execution.

        This is used by Portfolio to update its state given the fills.

        In case of child-less orders (e.g., market orders) we should return fills
        corresponding to the parent orders directly.
        In case of child orders (e.g., TWAP order) we need to roll up the fills
        for the children orders into corresponding fills for the parent orders.

        :return: a list of fills
        """
        fills: List[ombroker.Fill] = []
        # Get all parent orders sent to the `oms.Broker` in the previous
        # execution: each order passed to the Broker is considered a parent order.
        # Each parent order may correspond to 1 or more CCXT orders, and each
        # CCXT order is assigned an ID by the exchange if the submission was
        # successful.
        # - In case of market orders, one parent order corresponds to one
        #   "child" order
        # - In case of TWAP orders, one parent order corresponds to multiple child
        #   orders with separate CCXT IDs
        submitted_parent_orders = self._previous_parent_orders
        if submitted_parent_orders is None:
            _LOG.debug(
                "No parent orders sent in the previous execution: "
                "returning no fills"
            )
            return fills
        if self.previous_parent_orders_timestamp is None:
            # If there was no parent order in previous execution, then there is
            # no fill.
            _LOG.debug("No last order execution timestamp: returning no fills")
            return fills
        # Get asset ids for the orders sent in previous execution.
        asset_ids = [
            submitted_parent_order.asset_id
            for submitted_parent_order in submitted_parent_orders
        ]
        _LOG.debug(hprint.to_str("asset_ids"))
        hdbg.dassert_lt(0, len(asset_ids))
        #
        for parent_order in submitted_parent_orders:
            _LOG.debug("Getting fill for parent_order=%s", str(parent_order))
            asset_id = parent_order.asset_id
            parent_order_id = parent_order.order_id
            symbol = self.asset_id_to_ccxt_symbol_mapping[asset_id]
            # Get CCXT ID of the children orders corresponding to the parent
            # order.
            # TODO(gp): @danya it would be better to ensure that ccxt_id is
            #  always a list when we write, rather than when we read.
            child_order_ccxt_ids = parent_order.extra_params.get("ccxt_id", [])
            _LOG.debug(hprint.to_str("child_order_ccxt_ids"))
            # If there is only one associated CCXT ID, convert to list.
            if not isinstance(child_order_ccxt_ids, list):
                hdbg.dassert_type_is(child_order_ccxt_ids, int)
                child_order_ccxt_ids = [child_order_ccxt_ids]
            hdbg.dassert_isinstance(child_order_ccxt_ids, list)
            if not child_order_ccxt_ids:
                # If no child orders were accepted, consider `Fill` to be empty.
                _LOG.debug(
                    "No child orders found for parent order_id=%s",
                    parent_order_id,
                )
                continue
            # The CCXT method `fetch_orders()` can filter by time range and order
            # ID, however:
            # a) OMS `get_fills()` method currently does not take a time range
            # b) only one order ID is accepted by CCXT and querying for all
            #    the order IDs takes too long
            # Our current solution is to fetch the latest 500 orders for the
            # single parent order symbol using CCXT `fetch_orders()`.
            # TODO(Danya): There is a problem if we submit more than 500 child
            #  orders in a single interval.
            child_orders = self._exchange.fetch_orders(symbol=symbol, limit=500)
            # Filter child orders corresponding to this parent order.
            child_orders = [
                child_order
                for child_order in child_orders
                if int(child_order["id"]) in child_order_ccxt_ids
            ]
            _LOG.debug(hprint.to_str("child_orders"))
            # Calculate fill amount based on child orders.
            (
                parent_order_fill_signed_num_shares,
                parent_order_price,
            ) = occccuti.roll_up_child_order_fills_into_parent(
                parent_order, child_orders
            )
            # Skip the parent order if it has not been filled at all.
            if parent_order_fill_signed_num_shares == 0:
                _LOG.debug(
                    "Empty fill for parent order: %s",
                    hprint.to_str("parent_order child_orders"),
                )
                continue
            # Compute the timestamp for the `oms.Fill` as the latest child order
            # update time.
            # Update time is the time of the latest trade for the given order,
            # or the datetime of order submission if no trades were conducted.
            fill_timestamp = max(
                int(child_order["info"]["updateTime"])
                for child_order in child_orders
            )
            fill_timestamp = hdateti.convert_unix_epoch_to_timestamp(
                fill_timestamp
            )
            # Create a `oms.Fill` object.
            hdateti.dassert_has_UTC_tz(fill_timestamp)
            fill = ombroker.Fill(
                parent_order,
                fill_timestamp,
                parent_order_fill_signed_num_shares,
                parent_order_price,
            )
            _LOG.debug("fill=%s", str(fill))
            fills.append(fill)
        return fills

    def get_ccxt_trades(
        self,
        ccxt_orders: List[CcxtData],
    ) -> List[CcxtData]:
        """
        Get CCXT trades corresponding to passed CCXT orders.

        Each CCXT order can have several corresponding trades.

        The binance endpoint used is:
        https://binance-docs.github.io/apidocs/futures/en/#position-information-v2-user_data

        :param ccxt_orders: CCXT orders to load trades for
        :return: list of dicts with trades in the format described in
            https://docs.ccxt.com/#/?id=trade-structure
        """
        # Map currency symbols to the passed CCXT orders.
        symbol_to_order_mapping: Dict[str, List[CcxtData]] = {}
        for order in ccxt_orders:
            symbol = order["symbol"]
            if symbol in symbol_to_order_mapping:
                symbol_to_order_mapping[symbol].append(order)
            else:
                symbol_to_order_mapping[symbol] = [order]
        # Get trades for each symbol provided in the list of orders.
        # The trades are loaded for each symbol separately and then filtered
        # to leave only those corresponding to provided CCXT orders.
        trades = []
        for symbol, orders in symbol_to_order_mapping.items():
            symbol_trades = self._get_ccxt_trades_for_one_symbol(orders)
            trades.extend(symbol_trades)
        return trades

    def get_open_positions(self) -> Dict[str, float]:
        """
        Return all the open positions (with non-zero amount) from the exchange.

        :return: open positions in the format like:
            ```
            {
                "BTC/USDT": 0.001,
                "ETH/USDT": 10
            }
            ```
        """
        # Fetch all the open positions. The response from the exchange looks like:
        # ```
        # [{'info': {'symbol': 'BTCUSDT',
        #            'positionAmt': '-0.200',
        #            'entryPrice': '23590.549',
        #            'markPrice': '23988.40000000',
        #            'unRealizedProfit': '-79.57020000',
        #            'liquidationPrice': '68370.47429432',
        #            'leverage': '20',
        #            'maxNotionalValue': '250000',
        #            'marginType': 'cross',
        #            'isolatedMargin': '0.00000000',
        #            'isAutoAddMargin': 'false',
        #            'positionSide': 'BOTH',
        #            'notional': '-4797.68000000',
        #            'isolatedWallet': '0',
        #            'updateTime': '1659028521933'},
        #   'symbol': 'BTC/USDT',
        #   'contracts': 0.2,
        #   'contractSize': 1.0,
        #   'unrealizedPnl': -79.5702,
        #   'leverage': 20.0,
        #   'liquidationPrice': 68370.47429432,
        #   'collateral': 9092.72600745,
        #   'notional': 4797.68,
        #   'markPrice': 23988.4,
        #   'entryPrice': 23590.549,
        #   'timestamp': 1659028521933,
        #   'initialMargin': 239.884,
        #   'initialMarginPercentage': 0.05,
        #   'maintenanceMargin': 47.9768,
        #   'maintenanceMarginPercentage': 0.01,
        #   'marginRatio': 0.0053,
        #   'datetime': '2022-07-28T17:15:21.933Z',
        #   'marginType': 'cross',
        #   'side': 'short',
        #   'hedged': False,
        #   'percentage': -33.17}]
        positions = self._exchange.fetchPositions()
        # Map from symbol to the amount currently owned if different than zero,
        # e.g. `{'BTC/USDT': 0.01}`.
        open_positions: Dict[str, float] = {}
        for position in positions:
            # Get the quantity of assets on short/long positions.
            position_amount = float(position["info"]["positionAmt"])
            position_symbol = position["symbol"]
            if position_amount != 0:
                open_positions[position_symbol] = position_amount
        _LOG.debug(hprint.to_str("open_positions"))
        return open_positions

    def cancel_open_orders(self, currency_pair: str) -> None:
        """
        Cancel all the open orders for the given currency pair.
        """
        self._exchange.cancelAllOrders(currency_pair)

    def get_total_balance(self) -> Dict[str, float]:
        """
        Fetch total available balance from the exchange through CCXT.

        :return: total balance, e.g.,
            ```
            {
                'BNB': 0.0,
                'USDT': 5026.22494667,
                'BUSD': 1000.10001
            }
            ```
        """
        hdbg.dassert(self._exchange.has["fetchBalance"])
        # Fetch the balance data.
        balance = self._exchange.fetchBalance()
        # Select total balance.
        total_balance = balance["total"]
        # Verify the type of the return value is valid.
        hdbg.dassert_type_is(total_balance, dict)
        for k, v in total_balance.items():
            hdbg.dassert_type_is(k, str)
            hdbg.dassert_type_is(v, float)
        return total_balance

    # ////////////////////////////////////////////////////////////////////////
    # Private methods
    # ////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _check_binance_code_error(e: Exception, error_code: int) -> bool:
        """
        Check if the exception matches the expected error code.

        :param e: Binance's exception raised by CCXT
            ```
            {
                "code":-4131,
                "msg":"The counterparty's best price does not meet the PERCENT_PRICE filter limit."
            }
            ```
        :param error_code: expected error code, e.g. -4131
        :return: whether the error code is contained in error message
        """
        error_message = str(e)
        error_regex = f'"code":{error_code},'
        return bool(re.search(error_regex, error_message))

    # TODO(@Danya/@Grisha): Replace uses of currency pairs with full symbols
    #  for each public method.
    @staticmethod
    def _convert_currency_pair_to_ccxt_format(currency_pair: str) -> str:
        """
        Convert full symbol to CCXT format.

        Example: "BTC_USDT" -> "BTC/USDT"
        """
        currency_pair = currency_pair.replace("_", "/")
        return currency_pair

    # //////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _is_submitted_order(order: omorder.Order) -> bool:
        """
        Check if an order is submitted to CCXT.

        A submitted order has a valid `ccxt_id` and is not empty.
        """
        if order is None:
            # An empty order was not submitted.
            return False
        if "ccxt_id" not in order.extra_params:
            # Submitted order contains CCXT response in the form of `ccxt_id`.
            return False
        # Check that `ccxt_id` is valid.
        ccxt_id = order.extra_params["ccxt_id"]
        ret = True
        if not isinstance(ccxt_id, int):
            # ID must be an int.
            ret = False
        if ccxt_id < 0:
            # ID cannot be negative.
            ret = False
        return ret

    @abc.abstractmethod
    async def _submit_twap_child_orders(
        self,
        parent_orders_tmp: List[omorder.Order],
        parent_order_ids_to_child_order_shares: Dict[int, float],
        order_dfs: List[pd.DataFrame],
        execution_freq: pd.Timedelta,
        passivity_factor: float,
    ) -> List[pd.DataFrame]:
        ...

    @abc.abstractmethod
    async def _submit_orders(
        self,
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> Tuple[str, pd.DataFrame]:
        ...

    def _build_asset_id_to_ccxt_symbol_mapping(
        self,
    ) -> Dict[int, str]:
        """
        Build asset id to CCXT symbol mapping.

        :return: mapping like:
            ```
            {
                1528092593: 'BAKE/USDT',
                8968126878: 'BNB/USDT',
                1182743717: 'BTC/BUSD',
            }
            ```
        """
        # Get full symbol universe.
        vendor = "CCXT"
        mode = "trade"
        full_symbol_universe = ivcu.get_vendor_universe(
            vendor, mode, version=self._universe_version, as_full_symbol=True
        )
        # # Filter symbols of the exchange corresponding to this instance.
        # full_symbol_universe = [s for s in full_symbol_universe if
        #                        s.startswith(self._exchange_id)]
        full_symbol_universe = list(
            filter(
                lambda s: s.startswith(self._exchange_id), full_symbol_universe
            )
        )
        # Build asset_id -> symbol mapping.
        asset_id_to_full_symbol_mapping = (
            ivcu.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        asset_id_to_symbol_mapping: Dict[int, str] = {}
        for asset_id, symbol in asset_id_to_full_symbol_mapping.items():
            # Select currency pair.
            currency_pair = ivcu.parse_full_symbol(symbol)[1]
            # Transform to CCXT format, e.g. 'BTC_USDT' -> 'BTC/USDT'.
            ccxt_symbol = self._convert_currency_pair_to_ccxt_format(
                currency_pair
            )
            asset_id_to_symbol_mapping[asset_id] = ccxt_symbol
        return asset_id_to_symbol_mapping

    def _log_into_exchange(self) -> ccxt.Exchange:
        """
        Log into the exchange and return the `ccxt.Exchange` object.
        """
        secrets_id = str(self._secret_identifier)
        # Select credentials for provided exchange.
        exchange_params = hsecret.get_secret(secrets_id)
        # Disable rate limit.
        # Automatic rate limitation is disabled to control submission of orders.
        # If enabled, CCXT can reject an order silently, which we want to avoid.
        # See CMTask4113.
        exchange_params["rateLimit"] = False
        # Log into futures/spot market.
        if self._contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(ccxt, self._exchange_id)
        exchange = ccxt_exchange(exchange_params)
        # Set exchange properties.
        # TODO(Juraj): extract all exchange specific configs into separate function.
        if self._exchange_id == "binance":
            # Necessary option to avoid time out of sync error
            # (CmTask2670 Airflow system run error "Timestamp for this
            # request is outside of the recvWindow.")
            exchange.options["adjustForTimeDifference"] = True
        if self._account_type == "sandbox":
            _LOG.warning("Running in sandbox mode")
            exchange.set_sandbox_mode(True)
        hdbg.dassert(
            exchange.checkRequiredCredentials(),
            msg="Required credentials not passed",
        )
        # Assert that the requested exchange supports all methods necessary to
        # make placing/fetching orders possible.
        methods = ["createOrder", "fetchClosedOrders"]
        abort = False
        for method in methods:
            if not exchange.has[method]:
                _LOG.error(
                    "Method %s is unsupported for %s.", method, self._exchange_id
                )
                abort = True
        if abort:
            raise ValueError(
                f"The {self._exchange_id} exchange does not support all the"
                " required methods for placing orders."
            )
        # CCXT registers the logger after it's built, so we need to reduce its
        # logger verbosity.
        hloggin.shutup_chatty_modules()
        return exchange

    def _get_bid_ask_real_time_market_data(self) -> mdata.RealTimeMarketData2:
        """
        Get bid / ask realtime MarketData for calculating limit price.
        """
        # Build MarketData.
        asset_id_col = "asset_id"
        start_time_col_name = "start_timestamp"
        end_time_col_name = "end_timestamp"
        columns = None
        get_wall_clock_time = self._get_wall_clock_time
        asset_ids = list(self.asset_id_to_ccxt_symbol_mapping.keys())
        bid_ask_market_data = mdata.RealTimeMarketData2(
            self._bid_ask_im_client,
            asset_id_col,
            asset_ids,
            start_time_col_name,
            end_time_col_name,
            columns,
            get_wall_clock_time,
        )
        return bid_ask_market_data

    def _get_trading_fee_info(self) -> Dict[str, CcxtData]:
        """
        Get information on trading fees mapped to the `asset_id`.

        See CCXT docs: https://docs.ccxt.com/en/latest/manual.html#fees

        Information includes:
        - 'info': fee tier for the given asset
        - 'maker': maker fee
        - 'taker': taker fee
        - 'fee_side': fee side - 'quote', 'get', 'base' etc.
        - 'percentage': whether the provided number is a percentage

        Example of an output:
        {
            1467591036: {'info': {'feeTier': 0},
                        'symbol': 'BTC/USDT',
                        'maker': 0.0002,
                        'taker': 0.0004,
                        'fee_side': 'quote',
                        'percentage': True},
            1464553467: {'info': {'feeTier': 0},
                        'symbol': 'ETH/USDT',
                        'maker': 0.0002,
                        'taker': 0.0004,
                        'fee_side': 'quote',
                        'percentage': True},
        }
        """
        # Fetch fee info by symbol from the exchange.
        # Example of CCXT output:
        # {'trading': {'BTC/USDT': {'info': {'feeTier': 0},
        #                         'symbol': 'BTC/USDT',
        #                         'maker': 0.0002,
        #                         'taker': 0.0004},
        #             'ETH/USDT': {'info': {'feeTier': 0},
        #                         'symbol': 'ETH/USDT',
        #                         'maker': 0.0002,
        #                         'taker': 0.0004}}
        # 'delivery': {}}
        fee_info = self._exchange.fetchFees()
        fee_info = fee_info["trading"]
        # Filter symbols present in the broker universe.
        fee_info = {
            k: v
            for k, v in fee_info.items()
            if k in self.ccxt_symbol_to_asset_id_mapping.keys()
        }
        # Replace full symbols with asset_ids, e.g. 'BTC/USDT' -> 1467591036.
        fee_info = {
            self.ccxt_symbol_to_asset_id_mapping[k]: v
            for k, v in fee_info.items()
        }
        # Add information on the fee side and percentage.
        # These parameters are attributed to the market and are dependent on
        # contract type (spot, futures, etc.).
        # Example of CCXT output:
        # ```
        # {'trading': {'percentage': True,
        #   'feeSide': 'get',
        #   'tierBased': False,
        #   'taker': 0.001,
        #   'maker': 0.001},
        #  'funding': {'withdraw': {}, 'deposit': {}},
        #  'future': {'trading': {'feeSide': 'quote',
        #    'tierBased': True,
        #    'percentage': True,
        #    'taker': 0.0004,
        #    'maker': 0.0002,
        #    'tiers': {'taker': [[0.0, 0.0004],
        #      [250.0, 0.0004],
        #      [2500.0, 0.00035],
        #      [7500.0, 0.00032],],
        #     'maker': [[0.0, 0.0002],
        #      [250.0, 0.00016],
        #      [2500.0, 0.00014],
        #      [7500.0, 0.00012],]}}}}
        # ```
        if self._contract_type == "futures":
            # Get info on future markets.
            market_fees = self._exchange.fees["future"]["trading"]
        else:
            # Get info on spot markets.
            market_fees = self._exchange.fees["trading"]
        # Get information from CCXT.
        fee_side = market_fees["feeSide"]
        percentage = market_fees["percentage"]
        # Add fees and percentage to the mapping. These parameters are
        # market-specific and apply to all symbols.
        for asset_id in fee_info.keys():
            fee_info[asset_id]["fee_side"] = fee_side
            fee_info[asset_id]["percentage"] = percentage
        return fee_info

    async def _wait_for_accepted_orders(
        self,
        order_receipt: str,
    ) -> None:
        # Calls to CCXT to submit orders are blocking, so there is no need to wait
        # for order being accepted.
        _ = order_receipt

    def _get_market_info(self) -> Dict[int, Any]:
        """
        Load market information from the given exchange and map to asset ids.

        The following data is saved:
        - minimal order limits (notional and quantity)
        - asset quantity precision (for rounding of orders)
        - maximum allowed leverage for tier 0

        The numbers are determined by loading the market metadata from CCXT.
        """
        minimal_order_limits: Dict[int, Any] = {}
        symbols = list(self.ccxt_symbol_to_asset_id_mapping.keys())
        # Load market information from CCXT.
        exchange_markets = self._exchange.load_markets()
        # Download leverage information. See more about the output format:
        # https://docs.ccxt.com/en/latest/manual.html#leverage-tiers-structure.
        leverage_info = self._exchange.fetchLeverageTiers(symbols)
        _LOG.debug(hprint.to_str("leverage_info"))
        for asset_id, symbol in self.asset_id_to_ccxt_symbol_mapping.items():
            minimal_order_limits[asset_id] = {}
            currency_market = exchange_markets[symbol]
            # Example:
            # {'active': True,
            # 'base': 'ADA',
            # 'baseId': 'ADA',
            # 'contract': True,
            # 'contractSize': 1.0,
            # 'delivery': False,
            # 'expiry': None,
            # 'expiryDatetime': None,
            # 'feeSide': 'get',
            # 'future': True,
            # 'id': 'ADAUSDT',
            # 'info': {'baseAsset': 'ADA',
            #         'baseAssetPrecision': '8',
            #         'contractType': 'PERPETUAL',
            #         'deliveryDate': '4133404800000',
            #         'filters': [{'filterType': 'PRICE_FILTER',
            #                     'maxPrice': '25.56420',
            #                     'minPrice': '0.01530',
            #                     'tickSize': '0.00010'},
            #                     {'filterType': 'LOT_SIZE',
            #                     'maxQty': '10000000',
            #                     'minQty': '1',
            #                     'stepSize': '1'},
            #                     {'filterType': 'MARKET_LOT_SIZE',
            #                     'maxQty': '10000000',
            #                     'minQty': '1',
            #                     'stepSize': '1'},
            #                     {'filterType': 'MAX_NUM_ORDERS', 'limit': '200'},
            #                     {'filterType': 'MAX_NUM_ALGO_ORDERS', 'limit': '10'},
            #                     {'filterType': 'MIN_NOTIONAL', 'notional': '10'},
            #                     {'filterType': 'PERCENT_PRICE',
            #                     'multiplierDecimal': '4',
            #                     'multiplierDown': '0.9000',
            #                     'multiplierUp': '1.1000'}],
            #         'liquidationFee': '0.020000',
            #         'maintMarginPercent': '2.5000',
            #         'marginAsset': 'USDT',
            #         'marketTakeBound': '0.10',
            #         'onboardDate': '1569398400000',
            #         'orderTypes': ['LIMIT',
            #                         'MARKET',
            #                         'STOP',
            #                         'STOP_MARKET',
            #                         'TAKE_PROFIT',
            #                         'TAKE_PROFIT_MARKET',
            #                         'TRAILING_STOP_MARKET'],
            #         'pair': 'ADAUSDT',
            #         'pricePrecision': '5',
            #         'quantityPrecision': '0',
            #         'quoteAsset': 'USDT',
            #         'quotePrecision': '8',
            #         'requiredMarginPercent': '5.0000',
            #         'settlePlan': '0',
            #         'status': 'TRADING',
            #         'symbol': 'ADAUSDT',
            #         'timeInForce': ['GTC', 'IOC', 'FOK', 'GTX'],
            #         'triggerProtect': '0.0500',
            #         'underlyingSubType': ['HOT'],
            #         'underlyingType': 'COIN'},
            # 'inverse': False,
            # 'limits': {'amount': {'max': 10000000.0, 'min': 1.0},
            #             'cost': {'max': None, 'min': 10.0},
            #             'leverage': {'max': None, 'min': None},
            #             'market': {'max': 10000000.0, 'min': 1.0},
            #             'price': {'max': 25.5642, 'min': 0.0153}},
            # 'linear': True,
            # 'lowercaseId': 'adausdt',
            # 'maker': 0.0002,
            # 'margin': False,
            # 'option': False,
            # 'optionType': None,
            # 'percentage': True,
            # 'precision': {'amount': 0, 'base': 8, 'price': 4, 'quote': 8},
            # 'quote': 'USDT',
            # 'quoteId': 'USDT',
            # 'settle': 'USDT',
            # 'settleId': 'USDT',
            # 'spot': False,
            # 'strike': None,
            # 'swap': True,
            # 'symbol': 'ADA/USDT',
            # 'taker': 0.0004,
            # 'tierBased': False,
            # 'type': 'future'}
            limits = currency_market["limits"]
            # Get the minimal amount of asset in the order.
            amount_limit = limits["amount"]["min"]
            minimal_order_limits[asset_id]["min_amount"] = amount_limit
            # Set the minimal cost of asset in the order.
            # The notional limit can differ between symbols and subject to
            # fluctuations, so it is set manually to 10.
            notional_limit = 10.0
            minimal_order_limits[asset_id]["min_cost"] = notional_limit
            # Set the rounding precision for amount of the asset.
            amount_precision = currency_market["precision"]["amount"]
            minimal_order_limits[asset_id]["amount_precision"] = amount_precision
            #
            # Assume that all the positions belong to the lowest tier.
            tier_0_leverage_info = leverage_info[symbol][0]
            max_leverage_float = tier_0_leverage_info["maxLeverage"]
            try:
                # Convert max leverage to int, raw value is a float.
                max_leverage = int(max_leverage_float)
            except ValueError as e:
                _LOG.warning(
                    "Max leverage=%s should be of int type", max_leverage_float
                )
                raise e
            minimal_order_limits[asset_id]["max_leverage"] = max_leverage
        return minimal_order_limits

    def _get_ccxt_trades_for_one_symbol(
        self, ccxt_orders: List[CcxtData]
    ) -> List[CcxtData]:
        """
        Get CCXT trades for orders that share the same symbol.

        This is a helper handling loading of trades from CCXT
        for a batch of orders via a single API call.

        :param ccxt_orders: list of dicts of CCXT order with the same symbol, e.g. 'BTC/USDT'
        :return: list of dicts of trades for provided orders
        """
        # Get the symbol of the provided orders.
        symbol = list(set([ccxt_order["symbol"] for ccxt_order in ccxt_orders]))
        # Verify that it is the same for all provided orders.
        hdbg.dassert_eq(len(symbol), 1)
        symbol = symbol[0]
        # Get conducted trades for that symbol.
        #
        # Example of output of `fetchMyTrades()`
        # ```
        # {'info': {'symbol': 'ETHUSDT',
        #           'id': '2271885264',
        #           'orderId': '8389765544333791328',
        #           'side': 'SELL',
        #           'price': '1263.68',
        #           'qty': '0.016',
        #           'realizedPnl': '-3.52385454',
        #           'marginAsset': 'USDT',
        #           'quoteQty': '20.21888',
        #           'commission': '0.00808755',
        #           'commissionAsset': 'USDT',
        #           'time': '1663859837554',
        #           'positionSide': 'BOTH',
        #           'buyer': False,
        #           'maker': False},
        #  'timestamp': 1663859837554,
        #  'datetime': '2022-09-22T15:17:17.554Z',
        #  'symbol': 'ETH/USDT',
        #  'id': '2271885264',
        #  'order': '8389765544333791328',
        #  'type': None,
        #  'side': 'sell',
        #  'takerOrMaker': 'taker',
        #  'price': 1263.68,
        #  'amount': 0.016,
        #  'cost': 20.21888,
        #  'fee': {'cost': 0.00808755, 'currency': 'USDT'},
        #  'fees': [{'currency': 'USDT', 'cost': 0.00808755}]}
        # ```
        symbol_trades = self._exchange.fetchMyTrades(symbol, limit=1000)
        # Filter the trades based on CCXT order IDs.
        #
        # Select CCXT IDs and filter orders by them.
        # By default, the trades for symbol are loaded for the past 7 days.
        # To get trades corresponding to a Broker session, the trades are
        # filtered out by order ID. It is assumed that the input CCXT orders
        # are already closed orders for the given Broker iteration.
        ccxt_order_ids = set([ccxt_order["id"] for ccxt_order in ccxt_orders])
        symbol_trades = filter(
            lambda trade: trade["info"]["orderId"] in ccxt_order_ids,
            symbol_trades,
        )
        # Add the asset ids to each fill.
        asset_id = self.ccxt_symbol_to_asset_id_mapping[symbol]
        symbol_trades_with_asset_ids = []
        for symbol_fill in symbol_trades:
            _LOG.debug(hprint.to_str("symbol_fill"))
            # Get the position of the full symbol field
            # to paste the asset id after it.
            hdbg.dassert_in("symbol", symbol_fill.keys())
            idx = list(symbol_fill.keys()).index("symbol") + 1
            # Add asset id.
            symbol_fill = list(symbol_fill.items())
            symbol_fill.insert(idx, ("asset_id", asset_id))
            _LOG.debug(hprint.to_str("symbol_fill"))
            symbol_trades_with_asset_ids.append(dict(symbol_fill))
            
        return symbol_trades_with_asset_ids

    async def _submit_single_order_to_ccxt_with_retry(
        self,
        order: omorder.Order,
        *,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        wait_time_in_secs: int = 3,
    ) -> Tuple[omorder.Order, CcxtData]:
        """
        Submit a single order to CCXT using a retry mechanism.

        This function tries several times to submit the order, and if it fails
        it raises.

        :param order: order to be submitted
        :param order_type: 'market' or 'limit'
        :param limit_price: limit order price if `order_type` = 'limit'
        :param wait_time_in_secs: how long to wait between resubmissions

        :return: if order submission is successful, a tuple with 2 elements:
         - order updated with CCXT order id if the submission was successful
         - CCXT order structure: https://docs.ccxt.com/en/latest/manual.html#order-structure
        """
        hdbg.dassert_in(order_type, ["market", "limit"])
        _LOG.debug("Submitting order=%s", order)
        # Extract the info from the order.
        symbol = self.asset_id_to_ccxt_symbol_mapping[order.asset_id]
        side = "buy" if order.diff_num_shares > 0 else "sell"
        hdbg.dassert_ne(order.diff_num_shares, 0)
        position_size = abs(order.diff_num_shares)
        # Get max leverage for the order based on the asset id.
        max_leverage = self.market_info[order.asset_id]["max_leverage"]
        submitted_order = order
        submitted_order.extra_params["max_leverage"] = max_leverage
        #
        submitted_order.extra_params[
            "submit_single_order_to_ccxt.start.timestamp"
        ] = self.market_data.get_wall_clock_time()
        # Set up empty response in case the order is not submitted.
        ccxt_order_response: CcxtData = {}
        for num_attempt in range(self.max_order_submit_retries):
            _LOG.debug(
                "Order submission attempt: %s / %s",
                num_attempt + 1,
                self.max_order_submit_retries,
            )
            try:
                # Make sure that leverage is within the acceptable range before
                # submitting the order.
                _LOG.debug(
                    "Max leverage for symbol=%s and position size=%s is set to %s",
                    symbol,
                    position_size,
                    max_leverage,
                )
                self._exchange.setLeverage(max_leverage, symbol)
                _LOG.debug("Submitting order=%s", order)
                params = {
                    "portfolio_id": self._portfolio_id,
                    "client_oid": order.order_id,
                }
                # Create a reduce-only order.
                # Such an order can only reduce the current open position
                # and will never go over the current position size.
                if order.extra_params.get("reduce_only", False):
                    # Reduce-only order can be placed as a limit order,
                    # but since we only use it for quick liquidation,
                    # keeping this assertion here.
                    hdbg.dassert_eq(order_type, "market")
                    _LOG.debug("Creating reduceOnly order: %s", str(order))
                    ccxt_order_response = self._exchange.createReduceOnlyOrder(
                        symbol=symbol,
                        type=order_type,
                        side=side,
                        amount=position_size,
                        params=params,
                        price=0,
                    )
                elif order_type == "market":
                    # Simple market order.
                    ccxt_order_response = self._exchange.createOrder(
                        symbol=symbol,
                        type=order_type,
                        side=side,
                        amount=position_size,
                        params=params,
                    )
                elif order_type == "limit":
                    # Limit order.
                    hdbg.dassert_isinstance(limit_price, float)
                    if side == "buy":
                        ccxt_order_response = self._exchange.createLimitBuyOrder(
                            symbol=symbol,
                            amount=position_size,
                            price=limit_price,
                            params=params,
                        )
                    elif side == "sell":
                        ccxt_order_response = self._exchange.createLimitSellOrder(
                            symbol=symbol,
                            amount=position_size,
                            price=limit_price,
                            params=params,
                        )
                    else:
                        raise ValueError(f"Invalid side='{side}'")
                else:
                    raise ValueError(f"Invalid order_type='{order_type}'")
                _LOG.debug(hprint.to_str("ccxt_order_response"))
                # Assign CCXT order ID.
                ccxt_id = int(ccxt_order_response["id"])
                submitted_order.extra_params["ccxt_id"] = ccxt_id
                # If the submission was successful, don't retry.
                break
            except Exception as e:
                # Check the Binance API error.
                if isinstance(e, ccxt.ExchangeNotAvailable):
                    # If there is a temporary server error, wait a bit and retry.
                    _LOG.warning(
                        "Exception thrown:\n%s\nRetrying ...",
                        e,
                    )
                    _LOG.debug("Sleeping for %s secs", wait_time_in_secs)
                    await asyncio.sleep(wait_time_in_secs)
                    _LOG.debug("Sleeping for %s secs: done", wait_time_in_secs)
                    continue
                elif self._check_binance_code_error(e, -4131):
                    # If the error is related to liquidity, skip this order
                    # and keep submitting orders.
                    _LOG.warning(
                        "PERCENT_PRICE error, the exception was:\n%s\nContinuing...",
                        e,
                    )
                    break
                else:
                    # Unexpected error so we raise.
                    raise e
        # Log order stats.
        submitted_order.extra_params[
            "submit_single_order_to_ccxt.num_attempts"
        ] = num_attempt
        # TODO(gp): Log also reasons for order retries.
        submitted_order.extra_params[
            "submit_single_order_to_ccxt.end.timestamp"
        ] = self.market_data.get_wall_clock_time()
        return submitted_order, ccxt_order_response

    # TODO(gp): @danya there should be no reference to child (e.g., child_order
    #  -> order).
    async def _submit_single_order_to_ccxt_with_safe_retry(
        self,
        child_order: omorder.Order,
        *,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        wait_time_in_secs: int = 3,
    ) -> Tuple[omorder.Order, CcxtData]:
        # Initialize an empty response in case the order does not come through.
        #  Note: the reason is that in case a previous child order was accepted,
        #  it will be logged instead of an empty one, leading to discrepancy
        #  between the unsuccessfully sent child order and the CCXT response.
        ccxt_child_order_response: CcxtData = {}
        try:
            # Submit child order and receive CCXT response.
            (
                child_order,
                ccxt_child_order_response,
            ) = await self._submit_single_order_to_ccxt_with_retry(
                child_order,
                order_type=order_type,
                limit_price=limit_price,
                wait_time_in_secs=wait_time_in_secs,
            )
        except ccxt.ExchangeError as e:
            # TODO(Nina): add an exception that inherited from
            #  `BaseException`.
            # It might happen that Binance doesn't accept an order
            # (e.g., because the notional is too small).
            # TODO(gp): @Danya add a better exception if possible (or
            #  at least document what exception we are getting).
            _LOG.warning(
                "Order submission failed due to ExchangeError: %s", str(e)
            )
            child_order.extra_params["error_msg"] = str(e)
        except ccxt.NetworkError as e:
            # TODO(Nina): add an exception that inherited from
            #  `BaseException`.
            # Continue after errors arising due to a connectivity issue.
            _LOG.warning(
                "Order submission failed due to NetworkError: %s",
                str(e),
            )
            child_order.extra_params["error_msg"] = str(e)
        except Exception as e:
            _LOG.warning(
                "Order submission failed due to unknown error: %s",
                str(e),
            )
            child_order.extra_params["error_msg"] = str(e)
        return child_order, ccxt_child_order_response

    # ///////////////////////////////////////////////////////////////////////////

    async def _align_with_parent_order_start_timestamp(
        self, parent_order_start_time: pd.Timestamp
    ) -> None:
        """
        Wait until it's time to start executing the parent order.
        """
        # Get the actual start time for TWAP execution.
        current_timestamp = self.market_data.get_wall_clock_time()
        hdbg.dassert_lte(current_timestamp, parent_order_start_time)
        # Wait to align to execution_start.
        wait_in_secs_before_start = (
            parent_order_start_time - current_timestamp
        ).total_seconds()
        # Wait until the execution start time.
        if wait_in_secs_before_start > 1:
            _LOG.debug(
                "Waiting for %s seconds until %s",
                wait_in_secs_before_start,
                parent_order_start_time,
            )
            await asyncio.sleep(wait_in_secs_before_start)
            _LOG.debug("Waiting for %s seconds done", wait_in_secs_before_start)

    async def _align_with_next_child_order_start_timestamp(
        self, duration_in_secs: int
    ) -> None:
        """
        Wait until the next wave of child orders can be started.

        E.g., if we are trading a parent order over 5 min and child
        orders every minute, we need to wait to align on a minute grid.
        """
        # Leaving 0.5 seconds for safety to start slightly before the minute starts.
        sleep_time = (
            duration_in_secs - 0.5 - self.market_data.get_wall_clock_time().second
        )
        if sleep_time < 0:
            _LOG.warning(
                "sleep_time=%s since the submission of orders was slow: skipping the wait",
                sleep_time,
            )
            sleep_time = 0
        await asyncio.sleep(duration_in_secs)
        _LOG.debug("Waiting for %s seconds done", sleep_time)

    # ///////////

    # TODO(gp):
    #  - It will be used also by v2.
    #  - This can be made "static" and moved in utils or it can be
    #    in AbstractCcxtBroker
    def _calculate_twap_child_order_size(
        self,
        # TODO(gp): -> parent_orders
        orders: List[omorder.Order],
        # TODO(gp): parent_execution_start_timestamp?
        execution_start: pd.Timestamp,
        execution_end: pd.Timestamp,
        execution_freq: pd.Timedelta,
    ) -> Dict[int, float]:
        """
        Get size for the TWAP child orders corresponding to `orders`.

        The child orders are computed according to the TWAP logic:
        - child order are spaced evenly by `execution_freq`
        - it is assumed that for each asset the size of child order is uniform.

        :param orders: orders to be broken up into child orders
        :param execution_start: execution start for all the parent orders
        :param execution_end: execution end for all the parent orders
        :param execution_freq: how frequently the child orders are sent
        :return: mapping of parent order ID to child order signed size
            ```
            {
                0: 5.3,
                1: 0.4,
                2: -0.3,
            }
            ```
        """
        hdbg.dassert_lt(execution_start, execution_end)
        # From parent order.id to child order shares.
        # TODO(gp): -> parent_order_id_to_child_signed_num_shares?
        parent_order_id_to_child_order_shares: Dict[int, float] = {}
        for order in orders:
            # Extract info from parent order.
            parent_order_id = order.order_id
            hdbg.dassert_not_in(
                parent_order_id, parent_order_id_to_child_order_shares
            )
            diff_signed_num_shares = order.diff_num_shares
            asset_id = order.asset_id
            hdbg.dassert_ne(0, diff_signed_num_shares)
            # Get a number of orders to be executed in a TWAP.
            # TODO(Danya): May not be calculated correctly due to:
            #  a) improper rounding for frequency
            #  b) the DAG initialization for the first minute takes too much time
            num_child_orders = int(
                (execution_end - execution_start) / execution_freq
            )
            hdbg.dassert_lte(1, num_child_orders)
            _LOG.debug(hprint.to_str("num_child_orders"))
            # Get size of a single child order based on number of orders.
            child_order_diff_signed_num_shares = (
                diff_signed_num_shares / num_child_orders
            )
            hdbg.dassert_ne(0, child_order_diff_signed_num_shares)
            _LOG.debug(hprint.to_str("child_order_diff_signed_num_shares"))
            # Round to the allowable asset precision.
            amount_precision = self.market_info[asset_id]["amount_precision"]
            child_order_diff_signed_num_shares = round(
                child_order_diff_signed_num_shares,
                amount_precision,
            )
            # Update the map.
            parent_order_id_to_child_order_shares[
                parent_order_id
            ] = child_order_diff_signed_num_shares
        return parent_order_id_to_child_order_shares
