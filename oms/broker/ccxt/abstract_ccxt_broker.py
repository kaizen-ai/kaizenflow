"""
An abstract base class CCXT broker.

Import as:

import oms.broker.ccxt.abstract_ccxt_broker as obcaccbr
"""

import abc
import asyncio
import nest_asyncio
import logging
import os
import re
import numpy as np
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Awaitable

import ccxt
import ccxt.pro as ccxtpro
import pandas as pd

import dev_scripts.git.git_hooks.utils as dsgghout
import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hlogging as hloggin
import helpers.hnumpy as hnumpy
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hretry as hretry
import helpers.hsecrets as hsecret
import helpers.hwall_clock_time as hwacltim
import im_v2.common.data.client as icdc
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
import im_v2.common.universe as ivcu
import market_data as mdata
import oms.broker.broker as obrobrok
import oms.broker.ccxt.ccxt_utils as obccccut
import oms.hsecrets as omssec
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)

# Max number of order submission retries.
_MAX_EXCHANGE_REQUEST_RETRIES = 3
# Sleep time in sec between request retries.
# TODO(Juraj):
_REQUEST_RETRY_SLEEP_TIME_IN_SEC = 0.5

CcxtData = Dict[str, Any]

# Added because of "RuntimeError: This event loop is already running"
# https://stackoverflow.com/questions/46827007/runtimeerror-this-event-loop-is-already-running-in-python
# TODO(gp): Investigate if it's a limitation of `asyncio` or a "design error" on our
# side.
nest_asyncio.apply()


# #############################################################################
# AbstractCcxtBroker
# #############################################################################


class AbstractCcxtBroker(obrobrok.Broker):
    def __init__(
        self,
        *args: Any,
        exchange_id: str,
        account_type: str,
        portfolio_id: str,
        contract_type: str,
        secret_identifier: omssec.SecretIdentifier,
        # TODO(Grisha): consider passing StichedMarketData with OHLCV and bid /
        #  ask data instead of passing `market_data` and `bid_ask_im_client`
        #  separately.
        bid_ask_im_client: Optional[icdc.ImClient] = None,
        max_order_submit_retries: Optional[int] = _MAX_EXCHANGE_REQUEST_RETRIES,
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
        :param *args: `obrobrok.Broker` positional arguments
        :param **kwargs: `obrobrok.Broker` keyword arguments
        """
        super().__init__(*args, **kwargs)
        if not self._log_dir:
            _LOG.warning(
                "No logging directory is provided, so not saving orders or fills."
            )
        # TODO(Juraj): we use this in decorator whr
        self._max_exchange_request_retries = max_order_submit_retries
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
        # Both versions of exchange are utilized, sometimes it is beneficial
        # to use the sync version in order to avoid the need to propagate async/await
        # across caller functions.
        # TODO(Juraj): Rename to _async_exchange.
        self._async_exchange = self._log_into_exchange(async_=True)
        self._sync_exchange = self._log_into_exchange(async_=False)
        # Map from asset ids to CCXT symbols and vice versa (e.g., when placing
        # orders).
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
        # Store the timestamp and parent orders in the previous execution.
        # Initially the orders are empty.
        self.previous_parent_orders_timestamp: Optional[pd.Timestamp] = None
        self._previous_parent_orders: Optional[List[oordorde.Order]] = None
        # Initialize ImClient with bid / ask data if it is provided.
        self._bid_ask_im_client = bid_ask_im_client
        if self._bid_ask_im_client is not None:
            # Initialize MarketData with bid / ask data to calculate limit price.
            self._bid_ask_market_data = self._get_bid_ask_real_time_market_data()
        self._bid_ask_raw_data_reader = self._get_bid_ask_raw_data_reader()
        leverage = 1
        self._set_leverage_for_all_symbols(leverage)

    # TODO(gp): P0, @all can we remove the extra dir `child_order_fills`?
    @staticmethod
    def log_oms_ccxt_fills(
        log_dir: str,
        wall_clock_time: Callable,
        ccxt_fills: List[CcxtData],
        ccxt_trades: List[CcxtData],
        oms_fills: List[obrobrok.Fill],
    ) -> None:
        """
        Save fills and trades to separate files.

        :param log_dir: dir to store logs in. The data structure looks like:
            ```
            {log_dir}/child_order_fills/ccxt_fills/ccxt_fills_20230515-112313.json
            {log_dir}/child_order_fills/ccxt_trades/ccxt_trades_20230515-112313.json
            {log_dir}/child_order_fills/oms_fills/oms_fills_20230515-112313.json
            ```
        :param wall_clock_time: the actual wall clock time of the running system
            for accounting
        :param ccxt_fills: list of CCXT fills loaded from CCXT
            - The CCXT objects correspond to closed order (i.e., not in execution
              on the exchange any longer)
            - Represent the cumulative fill for the closed orders across all
              trades
        :param ccxt_trades: list of CCXT trades corresponding to the CCXT fills
        :param oms_fills: list of `oms.Fill` objects
            - The OMS fill objects correspond to `oms.Fill` objects before they
              are submitted upstream to the Portfolio (e.g., before child
              orders are merged into parent orders for accounting by the
              `Portfolio`).
        """
        if not log_dir:
            _LOG.debug("No log dir provided.")
            return
        fills_log_dir = os.path.join(log_dir, "child_order_fills")
        hio.create_dir(fills_log_dir, incremental=True)
        # 1) Save CCXT fills, e.g.,
        # log_dir/child_order_fills/ccxt_fills/ccxt_fills_20230511-114405.json
        timestamp_str = AbstractCcxtBroker.timestamp_to_str(wall_clock_time())
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

    # TODO(Danya): Refactor to accept lists of OMS / CCXT child orders.
    @staticmethod
    def log_child_order(
        log_dir: str,
        get_wall_clock_time: Callable,
        oms_child_order: oordorde.Order,
        ccxt_child_order_response: CcxtData,
        extra_info: CcxtData,
    ) -> None:
        """
        Log a child order with CCXT order info and additional parameters.

        :param log_dir: dir to store logs in
            - OMS child order information is saved into a CSV file, while the
            corresponding order response from CCXT is saved into a JSON dir, in a
            format like:
                ```
                {log_dir}/oms_child_orders/...
                {log_dir}/ccxt_child_order_responses/...
                ```
        :param get_wall_clock_time: the actual wall clock time of the running system
            for accounting
        :param oms_child_order: an order to be logged
        :param ccxt_child_order_response: CCXT order structure from the exchange,
            corresponding to the child order
        :param extra_info: values to include into the logged order, for example
            `{'bid': 0.277, 'ask': 0.279}`
        """
        if not log_dir:
            _LOG.debug("No log dir provided, skipping")
            return
        # Add extra_info.
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
        # Generate file name.
        wall_clock_time_str = AbstractCcxtBroker.timestamp_to_str(
            get_wall_clock_time()
        )
        bar_timestamp = hwacltim.get_current_bar_timestamp(as_str=True)
        order_asset_id = logged_oms_child_order["asset_id"]
        # 1) Save OMS child orders.
        incremental = True
        oms_order_log_dir = os.path.join(log_dir, "oms_child_orders")
        hio.create_dir(oms_order_log_dir, incremental)
        oms_order_file_name = (
            f"{order_asset_id}_{bar_timestamp}.{wall_clock_time_str}.json"
        )
        oms_order_file_name = os.path.join(oms_order_log_dir, oms_order_file_name)
        hio.to_json(oms_order_file_name, logged_oms_child_order, use_types=True)
        _LOG.debug(
            "Saved OMS child orders log file %s",
            hprint.to_str("oms_order_file_name"),
        )
        # 2) Save CCXT order response.
        ccxt_log_dir = os.path.join(log_dir, "ccxt_child_order_responses")
        hio.create_dir(ccxt_log_dir, incremental)
        response_file_name = (
            f"{order_asset_id}_{bar_timestamp}.{wall_clock_time_str}.json"
        )
        response_file_name = os.path.join(ccxt_log_dir, response_file_name)
        hio.to_json(response_file_name, ccxt_child_order_response, use_types=True)
        _LOG.debug(
            "Saved CCXT child order response log file %s",
            hprint.to_str("response_file_name"),
        )

    # ////////////////////////////////////////////////////////////////////////
    # Private methods
    # ////////////////////////////////////////////////////////////////////////

    @staticmethod
    def compare_latest_and_average_price(
        bid_ask_price_data: pd.DataFrame, max_deviation: float
    ) -> Tuple[Dict[str, float], float, float]:
        """
        Retrieve and compare latest and average bid/ask prices.

        If the latest price deviates from the average price by more than
        `max_deviation`, the average price is used as reference to calculate
        the limit price downstream; otherwise, the latest price is used.

        :param bid_ask_price_data: bid/ask prices for a certain asset
        :param max_deviation: threshold to compare the average and latest prices,
            given as a value in range (0, 1) to express a percentage difference
        :return:
            - information about the latest and average bid/ask prices
            - reference bid price
            - reference ask price
        """
        # Verify that the maximum deviation is in (0, 1).
        hdbg.dassert_lgt(
            0,
            max_deviation,
            1,
            lower_bound_closed=False,
            upper_bound_closed=False,
        )
        # Get the latest bid/ask price and their timestamp.
        bid_price_latest, ask_price_latest = bid_ask_price_data.iloc[-1]
        # Get the average bid/ask prices over the period.
        bid_price_mean, ask_price_mean = bid_ask_price_data.mean()
        #
        out_data = {
            "latest_bid_price": bid_price_latest,
            "latest_ask_price": ask_price_latest,
            "bid_price_mean": bid_price_mean,
            "ask_price_mean": ask_price_mean,
        }
        # Compare the prices using the `max_deviation` threshold.
        # Determine reference bid price.
        if (
            abs((bid_price_latest - bid_price_mean) / bid_price_latest)
            > max_deviation
        ):
            bid_price = bid_price_mean
            out_data["used_bid_price"] = "bid_price_mean"
            _LOG.debug(
                "Latest price differs more than %s percent from average price: %s",
                max_deviation * 100,
                hprint.to_str("bid_price_latest bid_price_mean"),
            )
        else:
            bid_price = bid_price_latest
            out_data["used_bid_price"] = "latest_bid_price"
        # Determine reference ask price.
        if (
            abs((ask_price_latest - ask_price_mean) / ask_price_latest)
            > max_deviation
        ):
            ask_price = ask_price_mean
            out_data["used_ask_price"] = "ask_price_mean"
            _LOG.debug(
                "Latest price differs more than %s percent from average price: %s",
                max_deviation * 100,
                hprint.to_str("ask_price_latest ask_price_mean"),
            )
        else:
            ask_price = ask_price_latest
            out_data["used_ask_price"] = "latest_ask_price"
        return out_data, bid_price, ask_price

    @staticmethod
    def get_latest_timestamps_from_bid_ask_data(
        bid_ask_price_data: pd.DataFrame,
    ) -> Dict[str, pd.Timestamp]:
        """
        Get timestamp data related to the bid/ask price. For the example of
        bid/ask data see `_calculate_limit_price`.

        The target timestamps are:
        - "binance_timestamp" timestamp of the bid/ask provided by the exchange.
        - "knowledge_timestamp": when the data was loaded into the DB
        - "end_download_timestamp": when the data was downloaded by the extractor.

        For an example of input data see `get_bid_ask_data_for_last_period`.

        :param bid_ask_price_data: bid/ask prices for a certain asset
        :return: dictionary of target timestamp values
        """
        # Get the latest data point.
        latest_bid_ask = bid_ask_price_data.iloc[-1]
        # Extract all timestamps related to the price data.
        end_download_timestamp_col = "end_download_timestamp"
        knowledge_timestamp_col = "knowledge_timestamp"
        #
        end_download_timestamp = latest_bid_ask[end_download_timestamp_col]
        knowledge_timestamp = latest_bid_ask[knowledge_timestamp_col]
        #
        exchange_timestamp = latest_bid_ask.name
        # Create a mapping to include in the output.
        out_data = {
            "exchange_timestamp": exchange_timestamp,
            "knowledge_timestamp": knowledge_timestamp,
            "end_download_timestamp": end_download_timestamp,
        }
        return out_data

    @staticmethod
    def timestamp_to_str(timestamp: pd.Timestamp) -> str:
        """
        Convert timestamp to string.

        :param timestamp: timestamp to convert
        :return: timestamp in string format e.g. `20230727-111057`.
        """
        # Convert timestamp to string.
        timestamp_format = "%Y%m%d-%H%M%S"
        return timestamp.strftime(timestamp_format)

    def get_bid_ask_data_for_last_period(self, last_period: str) -> pd.DataFrame:
        """
        Get raw bid/ask data for the given last period.

                Example of the output DataFrame (levels 3-10 omitted for readability):

                                         currency_pair exchange_id           end_download_timestamp              knowledge_timestamp  bid_size_l1  bid_size_l2  ... bid_price_l1  bid_price_l2   ...  ask_size_l1  ask_size_l2  ...  ask_price_l1  ask_price_l2 ...   ccxt_symbols    asset_id
        timestamp
        2023-08-11 12:49:52.835000+00:00      GMT_USDT     binance 2023-08-11 12:49:52.975836+00:00 2023-08-11 12:49:53.205151+00:00      38688.0     279499.0  ...        0.2033        0.2032  ...     232214.0     244995.0  ...       0.2034        0.2035  ...  GMT/USDT:USDT  1030828978
        2023-08-11 12:49:52.835000+00:00      GMT_USDT     binance 2023-08-11 12:49:53.482785+00:00 2023-08-11 12:49:55.324804+00:00      38688.0     279499.0  ...        0.2033        0.2032  ...     232214.0     244995.0  ...       0.2034        0.2035  ...  GMT/USDT:USDT  1030828978
        2023-08-11 12:49:52.845000+00:00      SOL_USDT     binance 2023-08-11 12:49:52.979713+00:00 2023-08-11 12:49:53.205151+00:00        258.0        467.0  ...       24.4110       24.4100  ...        629.0        151.0  ...       24.4120       24.4130 ...  SOL/USDT:USDT  2237530510

                :param last_period: period in pd.Timedelta-compatible string format,
                 e.g. '10S'.
                :return: bid/ask data for given period.
        """
        # Get the first and last timestamp of the period.
        end_timestamp = pd.Timestamp.utcnow()
        start_timestamp = end_timestamp - pd.Timedelta(last_period)
        # Load raw data.
        bid_ask_data = self._bid_ask_raw_data_reader.read_data(
            start_timestamp, end_timestamp
        )
        #
        # Filter loaded data to only the broker's universe symbols.
        # Convert currency pairs to full CCXT symbol format, e.g. 'BTC_USDT' -> 'BTC/USDT:USDT'
        bid_ask_data["ccxt_symbols"] = bid_ask_data["currency_pair"].apply(
            self._convert_currency_pair_to_ccxt_format
        )
        # Map CCXT symbols to asset IDs.
        bid_ask_data = bid_ask_data.loc[
            bid_ask_data["ccxt_symbols"].isin(self.ccxt_symbol_to_asset_id_mapping)
        ]
        bid_ask_data["asset_id"] = bid_ask_data["ccxt_symbols"].apply(
            lambda x: self.ccxt_symbol_to_asset_id_mapping[x]
        )
        # Convert original index from unix epoch to Timestamp, e.g.
        # 1691758182667 -> pd.Timestamp('2023-08-11 12:50:01.987000+0000', tz='UTC')
        bid_ask_data.index = bid_ask_data.index.to_series().apply(
            lambda x: hdateti.convert_unix_epoch_to_timestamp(x)
        )
        bid_ask_data = bid_ask_data.sort_index()
        return bid_ask_data

    # TODO(gp): Reorganize the format to be a bit regular
    # 1) always OMS data before than CCXT
    # 2) flatten the directory structure
    # 3) use the same format with `bar_timestamp` and `wall_clock_time`
    #    f"XYZ.{bar_timestamp}.{wall_clock_time}.json"`
    # 4) clarify when orders are parent or child

    # @staticmethod
    def log_oms_parent_orders(
        self,
        log_dir: str,
        wall_clock_time: Callable,
        oms_parent_orders: List[oordorde.Order],
    ) -> None:
        """
        Log OMS parent orders before dividing them into child orders.

        The orders are logged before they are submitted, in the same format
        as they are passed from `TargetPositionAndOrderGenerator`.

        :param log_dir: dir to store logs in. The data structure looks like:
            ```
            {log_dir}/oms_parent_orders/oms_parent_orders_{bar_timestamp}.{wallclock_time}.json
            # E.g.,
            {log_dir}/oms_parent_orders/oms_parent_orders_20230622-084000.20230622-084421.json
            ```
        :param wall_clock_time: the actual wall clock time of the running system
            for accounting
        :param oms_parent_orders: list of OMS parent orders
        """
        if not log_dir:
            _LOG.debug("No log dir provided, skipping")
            return
        # Generate file name based on the bar timestamp.
        # TODO(gp): P1, @all Factor out the logic to format the timestamps.
        wall_clock_time = self.timestamp_to_str(wall_clock_time())
        bar_timestamp = hwacltim.get_current_bar_timestamp(as_str=True)
        oms_parent_orders_log_filename = (
            f"oms_parent_orders_{bar_timestamp}.{wall_clock_time}.json"
        )
        # Create enclosing dir.
        os.path.join(log_dir, "oms_parent_orders")
        oms_parent_orders_log_filename = os.path.join(
            log_dir,
            "oms_parent_orders",
            f"oms_parent_orders_{bar_timestamp}.json",
        )
        # Create enclosing dir.
        hio.create_enclosing_dir(oms_parent_orders_log_filename, incremental=True)
        # Check if there is a parent order log for previous wave of child orders.
        # The parent order log contains information which is updated
        # with each new wave, e.g. CCXT IDs of bound child orders.
        hio.rename_file_if_exists(oms_parent_orders_log_filename, wall_clock_time)
        # Serialize and save parent orders as JSON file.
        oms_parent_orders = [order.to_dict() for order in oms_parent_orders]
        hio.to_json(
            oms_parent_orders_log_filename, oms_parent_orders, use_types=True
        )
        _LOG.debug(hprint.to_str("oms_parent_orders_log_filename"))

    @abc.abstractmethod
    async def submit_twap_orders(
        self,
        parent_orders: List[oordorde.Order],
        passivity_factor: float,
        *,
        execution_freq: Optional[str] = "1T",
    ) -> List[pd.DataFrame]:
        """
        Execute orders using the TWAP strategy.
        """
        ...
        
    @hretry.retry(
        # We cannot use `self` in the decorator and thus we use constants.
        num_attempts=_MAX_EXCHANGE_REQUEST_RETRIES,
        exceptions=[ccxt.NetworkError, ccxt.RequestTimeout],
        retry_delay_in_sec=_REQUEST_RETRY_SLEEP_TIME_IN_SEC,
    )
    async def get_fills_for_order(self, order: oordorde.Order) -> obrobrok.Fill:
        """
        Return fill for a parent order.
        
        :param order: parent order to fetch fills for.
        """
        _LOG.debug("Getting fills for order=%s", str(order))
        asset_id = order.asset_id
        order_id = order.order_id
        symbol = self.asset_id_to_ccxt_symbol_mapping[asset_id]
        # Get CCXT ID of the children orders corresponding to the parent
        # order.
        child_order_ccxt_ids = order.extra_params.get("ccxt_id", [])
        _LOG.debug(hprint.to_str("child_order_ccxt_ids"))
        hdbg.dassert_isinstance(child_order_ccxt_ids, list)
        if not child_order_ccxt_ids:
            # If no child orders were accepted, consider `Fill` to be empty.
            _LOG.debug(
                "No child orders found for parent order_id=%s",
                order_id,
            )
            return
        # The CCXT method `fetch_orders()` can filter by time range and order
        # ID, however:
        # - OMS `get_fills()` method currently does not take a time range
        # - only one order ID is accepted by CCXT and querying for all
        #   the order IDs takes too long
        # Our current solution is to fetch the latest 500 orders for the
        # single parent order symbol using CCXT `fetch_orders()`.
        # TODO(Danya): There is a problem if we submit more than 500 child
        #  orders in a single interval. Think about how to remove this limitation.
        child_orders = await self._async_exchange.fetch_orders(
            symbol=symbol, limit=500
        )
        # Filter child orders corresponding to this parent order.
        child_orders = [
            child_order
            for child_order in child_orders
            if int(child_order["id"]) in child_order_ccxt_ids
        ]
        _LOG.debug(hprint.to_str("child_orders"))
        # Calculate fill amount based on child orders.
        (
            order_fill_signed_num_shares,
            order_price,
        ) = obccccut.roll_up_child_order_fills_into_parent(
            order, child_orders
        )
        # Skip the parent order if it has not been filled at all.
        if order_fill_signed_num_shares == 0:
            _LOG.debug(
                "Empty fill for parent order: %s",
                hprint.to_str("order child_orders"),
            )
            return
        # Compute the timestamp for the `oms.Fill` as the latest child order
        # update time.
        # `updateTime` in CCXT data structure is the time of the latest trade
        # for the given order, or the datetime of order submission if no
        # trades were conducted.
        fill_timestamp = max(
            int(child_order["info"]["updateTime"])
            for child_order in child_orders
        )
        fill_timestamp = hdateti.convert_unix_epoch_to_timestamp(
            fill_timestamp
        )
        # Create a `oms.Fill` object.
        hdateti.dassert_has_UTC_tz(fill_timestamp)
        fill = obrobrok.Fill(
            order,
            fill_timestamp,
            order_fill_signed_num_shares,
            order_price,
        )
        _LOG.debug("fill=%s", str(fill))
        return fill

    # TODO(gp): P1, Maybe get_oms_fills for clarity?
    def get_fills(self) -> List[obrobrok.Fill]:
        """
        Return list of fills from the previous order execution.

        This is used by Portfolio to update its state given the fills.

        In case of child-less parent orders (e.g., market orders) we should
        return fills corresponding to the parent orders directly.
        In case of child orders (e.g., TWAP order) we need to roll up the fills
        for the children orders into corresponding fills for the parent orders.

        :return: list of OMS fills
        """
        fills: List[obrobrok.Fill] = []
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
        # Get asset ids for the (parent) orders sent in previous execution.
        asset_ids = [oms_order.asset_id for oms_order in submitted_parent_orders]
        _LOG.debug(hprint.to_str("asset_ids"))
        hdbg.dassert_lt(0, len(asset_ids))
        get_fills_tasks = [self.get_fills_for_order(parent_order) for parent_order in submitted_parent_orders]
        fills = asyncio.get_running_loop().run_until_complete(asyncio.gather(*get_fills_tasks))
        fills = [fill for fill in fills if fill is not None]
        return fills
    
    async def get_ccxt_trades(
        self,
        ccxt_orders: List[CcxtData],
    ) -> List[CcxtData]:
        """
        Get CCXT trades corresponding to passed CCXT orders.

        Each CCXT order can have 0, 1 or more trades.

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
        tasks = [
            self._get_ccxt_trades_for_one_symbol(orders)
            for _, orders in symbol_to_order_mapping.items()
        ]
        trades = await asyncio.gather(*tasks)
        # Each task returns a list of trades, flatten it via list comprehension.
        trades = [trade for symbol_trades in trades for trade in symbol_trades]
        _LOG.debug(
            "CCXT trades loaded timestamp=%s",
            self.market_data.get_wall_clock_time(),
        )
        return trades

    # TODO(Juraj): we cannot use self here.
    @hretry.retry(
        # TODO(Juraj): we cannot use self here.
        num_attempts=_MAX_EXCHANGE_REQUEST_RETRIES,
        exceptions=[ccxt.NetworkError, ccxt.RequestTimeout],
        retry_delay_in_sec=_REQUEST_RETRY_SLEEP_TIME_IN_SEC,
    )
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
        positions = {}
        for _ in range(self._max_exchange_request_retries):
            try:
                positions = self._sync_exchange.fetch_positions()
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                time.sleep(1)
                continue
            if positions:
                break
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

    async def cancel_open_orders_for_symbols(
        self, currency_pairs: List[str]
    ) -> None:
        """
        Cancel all open orders for a given list of symbols.

        :param: currency_pairs: pairs to cancer all orders for
        """
        # Binance does not allow cancelling open orders for all symbols in one API call.
        tasks = [
            self._async_exchange.cancel_all_orders(pair)
            for pair in currency_pairs
        ]
        await asyncio.gather(*tasks)

    def cancel_open_orders(self, currency_pair: str) -> None:
        """
        Cancel all the open orders for the given currency pair.
        """
        self._sync_exchange.cancel_all_orders(currency_pair)

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
        # Fetch the balance data.
        hdbg.dassert(self._sync_exchange.has["fetchBalance"])
        balance = self._sync_exchange.fetchBalance()
        # Select total balance.
        total_balance = balance["total"]
        # Verify the type of the return value is valid.
        hdbg.dassert_type_is(total_balance, dict)
        for k, v in total_balance.items():
            hdbg.dassert_type_is(k, str)
            hdbg.dassert_type_is(v, float)
        return total_balance

    def load_bid_ask_and_calculate_limit_price(
        self,
        asset_id: int,
        side: str,
        passivity_factor: float,
    ) -> float:
        """
        Load bid/ask data and calculate limit price for the order.
        """
        bid_ask_data = self.get_bid_ask_data_for_last_period("10S")
        price_dict = self._calculate_limit_price(
            bid_ask_data,
            asset_id,
            side,
            passivity_factor,
        )
        limit_price = price_dict["limit_price"]
        return limit_price

    def _get_bid_ask_raw_data_reader(self) -> imvcdcimrdc.RawDataReader:
        """
        Get raw data reader for the real-time bid/ask price data.

        Currently the DB signature is hardcoded.
        """
        bid_ask_db_signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7.ccxt.binance.v1_0_0"
        # TODO(Juraj): we need to assign correct stage, the default for reader is prod.
        bid_ask_raw_data_reader = imvcdcimrdc.RawDataReader(bid_ask_db_signature)
        return bid_ask_raw_data_reader

    @staticmethod
    def _get_ccxt_id_from_child_order(order: oordorde.Order) -> int:
        """
        Get ccxt_id from the order object.

        CCXT ID is assigned by the exchange in case of successful order
        submission, or assigned a default value of `-1` if the exchange
        could not accept the order for some reason.
        CCXT ID is not present if and only if the order submission
        was not attempted yet.

        :param order: order to retrieve CCXT ID from
        :return: CCXT ID value
        """
        # The ID is stored as a list since by default one parent order can correspond
        # to multiple CCXT child orders, as in the TWAP execution.
        hdbg.dassert_in("ccxt_id", order.extra_params)
        hdbg.dassert_isinstance(order.extra_params["ccxt_id"], list)
        hdbg.dassert_eq(len(order.extra_params["ccxt_id"]), 1)
        ccxt_id = order.extra_params["ccxt_id"][0]
        hdbg.dassert_isinstance(ccxt_id, int)
        if ccxt_id == -1:
            # ccxt_id of -1 is possible if the order submission has been tried,
            # but was not accepted by the exchange. In this case, a
            # corresponding error should be logged in order extra params.
            hdbg.dassert_in("error_msg", order.extra_params)
        return ccxt_id

    @staticmethod
    def _set_ccxt_id_to_child_order(
        order: oordorde.Order, ccxt_id: Union[int, str]
    ) -> oordorde.Order:
        # By default, in a TWAP execution context a single OMS parent order may
        # correspond to multiple CCXT order IDs.
        if isinstance(ccxt_id, str):
            # Check whether the ID is a numerical string.
            try:
                ccxt_id = [int(ccxt_id)]
            except ValueError:
                raise ValueError(f"Can't convert CCXT ID `{ccxt_id}` to integer")
        elif isinstance(ccxt_id, int):
            ccxt_id = [ccxt_id]
        else:
            raise ValueError(
                f"Invalid type for CCXT ID: `{type(ccxt_id)}` (must be int or str)"
            )
        order.extra_params["ccxt_id"] = ccxt_id
        return order

    @staticmethod
    def _update_stats_for_order(
        order: oordorde.Order,
        tag: str,
        value: Any,
        *,
        prefix: Optional[str] = None,
    ) -> None:
        if "stats" not in order.extra_params:
            order.extra_params["stats"] = {}
        if prefix is None:
            # Get the name of the calling function if no prefix was suggested.
            prefix = dsgghout.get_function_name(count=1)
        # Construct the full tag, e.g., "start" -> "submit_twap_orders::start".
        tag = f"{prefix}::{tag}"
        _LOG.debug("order=%s tag=%s value=%s", str(order), tag, value)
        order_stats = order.extra_params["stats"]
        hdbg.dassert_not_in(tag, order_stats)
        order_stats[tag] = value

    @staticmethod
    def _check_binance_code_error(e: Exception, error_code: int) -> bool:
        """
        Check if the exception `e` matches the expected error code
        `error_code`.

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

        Example: "BTC_USDT" -> "BTC/USDT:USDT"
        """
        # In the newer CCXT version symbols are specified using the following format:
        # BTC/USDT:USDT - the :USDT refers to the currency in which the futures contract
        # are settled.
        settlement_coin = currency_pair.split("_")[1]
        currency_pair = currency_pair.replace("_", "/")
        return f"{currency_pair}:{settlement_coin}"

    @staticmethod
    def _is_child_order_shares_correct(
        child_order_diff_num_shares: float,
    ) -> bool:
        """
        Verify that the child order amount is correct.

        :param child_order_diff_num_shares: `diff_num_shares`
          value of the child order.
        """
        # TODO(Danya): Add a check for meeting the notional limit.
        is_correct = child_order_diff_num_shares == 0
        return is_correct

    def _handle_exchange_exception(
        self, e: Exception, order: oordorde.Order
    ) -> oordorde.Order:
        """
        Handle the exception shown by the exchange during the order submission.

        The expected CCXT exceptions are:
        - `NetworkError` for problems with connectivity
        - `ExchangeError` for violating rules of the exchange, like
          going below the notional limit
        - `ExchangeNotAvailable` for problems on the exchange side

        All exceptions are handled the same way:
        - the order submission is skipped.
        - the order is assigned a `ccxt_id` == -1 to indicate that the
          submission was attempted, but unsuccessful.
        - the error message is logged in the `order.extra_params`
          for later accounting.

        :param e: exception shown during the submission
        :param order: order that was to be submitted
        :return: Order object with `ccxt_id` == -1 and appended error message
        """
        error_msg = str(e)
        _LOG.warning(
            "Order: %s\nOrder submission failed due to: %s", str(order), error_msg
        )
        self._set_ccxt_id_to_child_order(order, -1)
        order.extra_params["error_msg"] = error_msg
        return order

    # TODO(Danya): Add tests to this and constituent static functions.
    def _calculate_limit_price(
        self,
        bid_ask_data: pd.DataFrame,
        asset_id: int,
        side: str,
        passivity_factor: float,
        max_deviation: float = 0.01,
    ) -> Dict[str, float]:
        """
        Calculate limit price based on recent bid / ask data.

        The limit price is adjusted using the passivity factor.
        The function returns a dict with:
        - calculated limit price;
        - latest bid/ask prices;
        - mean bid/ask prices in the given data;
        - passivity factor;
        - whether limit bid/ask prices are calculated using latest
            price or average price

        :param bid_ask_data: recent bid / ask data,
            for an example of input data see `get_bid_ask_data_for_last_period`.
        :param asset_id: asset for which to get limit price
        :param side: "buy" or "sell"
        :param passivity_factor: value in [0,1] to calculate limit price
        :param max_deviation: see `compare_latest_and_average_price()`
        :return: limit price and price data
        """
        # Verify that the passivity factor is in [0,1].
        # TODO(Juraj): temporarily disable assertion.
        # hdbg.dassert_lgt(
        #    0,
        #    passivity_factor,
        #    1,
        #    lower_bound_closed=True,
        #    upper_bound_closed=True,
        # )
        # Declare columns with bid/ask prices and verify that they exist and contain
        # numeric data (i.e. data that can be converted to float).
        bid_price_col = "bid_price_l1"
        ask_price_col = "ask_price_l1"
        for col in [bid_price_col, ask_price_col]:
            hdbg.dassert_in(col, bid_ask_data.columns)
            hdbg.dassert_type_is(float(bid_ask_data.iloc[0][col]), float)
        # Declare columns with price timestamps, verify that they exist and contain
        # data that can be converted to timestamp.
        end_download_timestamp_col = "end_download_timestamp"
        knowledge_timestamp_col = "knowledge_timestamp"
        for col in [end_download_timestamp_col, knowledge_timestamp_col]:
            hdbg.dassert_in(col, bid_ask_data.columns)
            hdbg.dassert_type_is(
                pd.Timestamp(bid_ask_data.iloc[0][col]), pd.Timestamp
            )
        # Verify that data has datetime index.
        hpandas.dassert_time_indexed_df(
            bid_ask_data, allow_empty=False, strictly_increasing=False
        )
        # Verify that there is data for the specified asset.
        hdbg.dassert_in("asset_id", bid_ask_data.columns)
        hdbg.dassert_lt(
            0, len(bid_ask_data[bid_ask_data["asset_id"] == asset_id])
        )
        # Initialize price dictionary.
        price_dict: Dict[str, Any] = {}
        price_dict["passivity_factor"] = passivity_factor
        # Select bid/ask price data by requested asset id.
        bid_ask_data = bid_ask_data.loc[bid_ask_data["asset_id"] == asset_id]
        bid_ask_price_data = bid_ask_data[
            [
                bid_price_col,
                ask_price_col,
                end_download_timestamp_col,
                knowledge_timestamp_col,
            ]
        ]
        # Assert that datetime index is monotonically increasing to get the latest price.
        hpandas.dassert_increasing_index(bid_ask_price_data)
        # Retrieve and compare latest and average prices.
        # The difference between these prices determines the reference
        # bid and ask prices to use further.
        (
            latest_avg_prices_info,
            bid_price,
            ask_price,
        ) = self.compare_latest_and_average_price(
            bid_ask_price_data[[bid_price_col, ask_price_col]], max_deviation
        )
        price_dict.update(latest_avg_prices_info)
        # Retrieve the timestamp data related to the latest prices.
        price_timestamp_dict = self.get_latest_timestamps_from_bid_ask_data(
            bid_ask_price_data[
                [end_download_timestamp_col, knowledge_timestamp_col]
            ]
        )
        price_dict.update(price_timestamp_dict)
        # Adjust limit price based on passivity factor.
        # - limit_price in [bid,ask];
        # - if side == "buy":
        #   - passivity == 1 -> limit_price = bid
        # - limit_price in [bid,ask];
        # - if side == "buy":
        #   - passivity == 1 -> limit_price = bid
        #   - passivity == 0 -> limit_price = ask
        # - if side == "sell":
        #   - passivity == 1 -> limit_price = ask
        #   - passivity == 0 -> limit_price = bid
        # - passivity_factor == 0.5 is a midpoint in both cases.
        if side == "buy":
            limit_price = (bid_price * passivity_factor) + (
                ask_price * (1 - passivity_factor)
            )
        elif side == "sell":
            limit_price = (ask_price * passivity_factor) + (
                bid_price * (1 - passivity_factor)
            )
        else:
            raise ValueError(f"Invalid side='{side}'")
        price_dict["limit_price"] = limit_price
        return price_dict

    def _is_submitted_order(self, order: oordorde.Order) -> bool:
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
        ccxt_id = self._get_ccxt_id_from_child_order(order)
        ret = True
        if ccxt_id < 0:
            # A submitted order has a positive ccxt_id.
            ret = False
        return ret

    def _skip_child_order_if_needed(
        self,
        parent_order: oordorde.Order,
        child_order_diff_signed_num_shares: Dict[int, float],
    ) -> bool:
        """
        Verify that the child order should not be skipped based on its size.

        The child order is skipped if the amount is equal to 0.

        :param parent_order: parent order on which the child is based
        :param child_order_diff_signed_num_shares: size of the
          child order corresponding to the parent.
        :return: boolean value of the check
        """
        # Skip the child order if it is empty after rounding down.
        if self._is_child_order_shares_correct(
            child_order_diff_signed_num_shares
        ):
            _LOG.debug(
                "Child order for parent_order=%s not sent, %s",
                str(parent_order),
                hprint.to_str("child_order_diff_signed_num_shares"),
            )
            return True

    @abc.abstractmethod
    def _submit_twap_child_orders(
        self,
        parent_orders: List[oordorde.Order],
        parent_order_ids_to_child_order_shares: Dict[int, float],
        order_dfs: List[pd.DataFrame],
        execution_freq: pd.Timedelta,
        passivity_factor: float,
    ) -> List[pd.DataFrame]:
        """
        Given a set of parent orders, create and submit TWAP child orders.
        """
        ...

    def _is_submitted_order(self, order: oordorde.Order) -> bool:
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
        ccxt_id = self._get_ccxt_id_from_child_order(order)
        ret = True
        if not isinstance(ccxt_id, int):
            # ID must be an int.
            ret = False
        if ccxt_id < 0:
            # ID cannot be negative.
            ret = False
        return ret

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
        # Filter symbols of the exchange corresponding to this instance.
        exchange_symbol_universe = [
            s for s in full_symbol_universe if s.startswith(self._exchange_id)
        ]
        # Build asset_id -> symbol mapping.
        asset_id_to_full_symbol_mapping = (
            ivcu.build_numerical_to_string_id_mapping(exchange_symbol_universe)
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

    def _log_into_exchange(self, async_: bool) -> ccxt.Exchange:
        """
        Log into the exchange and return the `ccxt.Exchange` object.
        """
        secrets_id = str(self._secret_identifier)
        # Select credentials for provided exchange.
        # TODO(Juraj): the problem is that we are mixing DB stage and credential stage
        # and also the intended nomenclature for the secrets id not really valid/useful.
        if ".prod" in secrets_id:
            secrets_id = secrets_id.replace("prod", "preprod")
        exchange_params = hsecret.get_secret(secrets_id)
        # Disable rate limit.
        # Automatic rate limitation is disabled to control submission of orders.
        # If enabled, CCXT can reject an order silently, which we want to avoid.
        # See CMTask4113.
        exchange_params["rateLimit"] = False
        # Log into futures/spot market.
        if self._contract_type == "futures":
            exchange_params["options"] = {"defaultType": "future"}
        module = ccxtpro if async_ else ccxt
        # Create a CCXT Exchange class object.
        ccxt_exchange = getattr(module, self._exchange_id)
        exchange = ccxt_exchange(exchange_params)
        # TODO(Danya): Temporary fix, CMTask4971.
        # Set exchange properties.
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
        fee_info = self._sync_exchange.fetch_fees()
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
        market_fees = self._sync_exchange.fees["trading"]
        # Get information from CCXT.
        fee_side = market_fees["feeSide"]
        percentage = market_fees["percentage"]
        # Add fees and percentage to the mapping. These parameters are
        # market-specific and apply to all symbols.
        for asset_id in fee_info.keys():
            fee_info[asset_id]["fee_side"] = fee_side
            fee_info[asset_id]["percentage"] = percentage
        return fee_info

    def _update_stats_for_orders(self, parent_orders_single_iteration_copy):
        prefix = dsgghout.get_function_name(count=1)
        for parent_order in parent_orders_single_iteration_copy:
            self._update_stats_for_order(
                parent_order,
                "start",
                self.market_data.get_wall_clock_time(),
                prefix=prefix,
            )

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
        exchange_markets = self._sync_exchange.load_markets()
        # Download leverage information. See more about the output format:
        # https://docs.ccxt.com/en/latest/manual.html#leverage-tiers-structure.
        leverage_info = self._sync_exchange.fetch_leverage_tiers(symbols)
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

    # TODO(Juraj): we cannot use self here.
    @hretry.retry(
        # TODO(Juraj): we cannot use self here.
        num_attempts=_MAX_EXCHANGE_REQUEST_RETRIES,
        exceptions=[ccxt.NetworkError, ccxt.RequestTimeout],
        retry_delay_in_sec=_REQUEST_RETRY_SLEEP_TIME_IN_SEC,
    )
    async def _get_ccxt_trades_for_one_symbol(
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
        symbol_trades = {}
        symbol_trades = await self._async_exchange.fetchMyTrades(
            symbol, limit=1000
        )
        # Map from symbol t
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

    def _set_leverage_for_all_symbols(self, leverage: int) -> None:
        """
        Set leverage for all symbols.

        :param leverage: leverage to set
        """
        # TODO(Juraj): Should we check if the change was successful?
        # TODO(Juraj): Temporary hack to assist with random order generation.
        for symbol in self.ccxt_symbol_to_asset_id_mapping.keys():
            self._sync_exchange.setLeverage(leverage, symbol)

    async def _submit_single_order_to_ccxt_with_retry(
        self,
        order: oordorde.Order,
        *,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        wait_time_in_secs: int = 0.5,
    ) -> Tuple[oordorde.Order, CcxtData]:
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
        # TODO(Juraj): Log the actual leverage used in the order,
        # not only the maximum possible.
        submitted_order.extra_params["max_leverage"] = max_leverage
        #
        self._update_stats_for_order(
            submitted_order,
            f"start.timestamp",
            self.market_data.get_wall_clock_time(),
        )
        # Set up empty response in case the order is not submitted.
        ccxt_order_response: CcxtData = {}
        for num_attempt in range(self._max_exchange_request_retries):
            _LOG.debug(
                "Order submission attempt: %s / %s",
                num_attempt + 1,
                self._max_exchange_request_retries,
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
                    # TODO(Danya): Not sure if async-supported, but probably irrelevant,
                    # since it is only used to flatten accounts.
                    ccxt_order_response = (
                        await self._async_exchange.createReduceOnlyOrder(
                            symbol=symbol,
                            type=order_type,
                            side=side,
                            amount=position_size,
                            params=params,
                            price=0,
                        )
                    )
                elif order_type == "market":
                    # Simple market order.
                    ccxt_order_response = await self._async_exchange.create_order(
                        symbol=symbol,
                        type=order_type,
                        side=side,
                        amount=position_size,
                        params=params,
                    )
                elif order_type == "limit":
                    # Limit order.
                    hdbg.dassert_isinstance(limit_price, float)
                    ccxt_order_response = await self._async_exchange.create_order(
                        symbol=symbol,
                        type=order_type,
                        side=side,
                        amount=position_size,
                        price=limit_price,
                        params=params,
                    )
                else:
                    raise ValueError(f"Invalid order_type='{order_type}'")
                _LOG.debug(hprint.to_str("ccxt_order_response"))
                # Assign parent order CCXT ID.
                submitted_order = self._set_ccxt_id_to_child_order(
                    submitted_order, ccxt_order_response["id"]
                )
                # If the submission was successful, don't retry.
                break
            except Exception as e:
                submitted_order = self._handle_exchange_exception(
                    e, submitted_order
                )
                self._update_stats_for_order(
                    submitted_order, f"exception_on_retry.{num_attempt}", str(e)
                )
                # Check the Binance API error.
                if isinstance(e, (ccxt.ExchangeNotAvailable, ccxt.NetworkError)):
                    # If there is a temporary server error, wait a bit and retry.
                    _LOG.warning(
                        "Connectivity error:\n%s\nRetrying ...",
                        e,
                    )
                    await asyncio.sleep(wait_time_in_secs)
                    _LOG.debug("Sleeping for %s secs: done", wait_time_in_secs)
                continue
        # Log order stats.
        self._update_stats_for_order(
            submitted_order,
            f"num_attempts",
            num_attempt,
        )
        self._update_stats_for_order(
            submitted_order,
            f"end.timestamp",
            self.market_data.get_wall_clock_time(),
        )
        return submitted_order, ccxt_order_response

    # ///////////////////////////////////////////////////////////////////////////

    def _align_with_parent_order_start_timestamp(
        self, parent_order_start_time: pd.Timestamp
    ) -> None:
        """
        Wait until it's time to start executing the parent order.
        """
        # Get the actual start time for TWAP execution.
        current_timestamp = self.market_data.get_wall_clock_time()
        if current_timestamp < parent_order_start_time:
            _LOG.debug("Aligning time with parent order start time")
            hasynci.sync_wait_until(
                parent_order_start_time, self._get_wall_clock_time
            )

    # ///////////

    def _calculate_num_twap_child_order_waves(
        self,
        parent_order_execution_end_timestamp: pd.Timestamp,
        execution_freq: pd.Timedelta
    ) -> int:
        """
        Calculate number of child waves to run based on the current time and 
        execution end timestamp of the parent order, given an execution frequency:
        
        :param parent_order_execution_end_timestamp
        :execution_freq: frequency of order submission
        :return: number of child order waves
        """
        num_waves = int(
            np.ceil(
                (parent_order_execution_end_timestamp - self._get_wall_clock_time())
                / execution_freq
            )
        )
        return num_waves
    

    def _calculate_twap_child_order_size(
        self,
        parent_orders: List[oordorde.Order],
        num_waves: int
    ) -> Dict[int, float]:
        """
        Get size for the TWAP child orders corresponding to `parent_orders`.

        The child orders are computed according to the TWAP logic:
        - child order are spaced evenly by `execution_freq`
        - it is assumed that for each asset the size of child order is uniform.
        
        :param parent_orders: orders to be broken up into child orders
        :param num_waves: number of child order waves
        :return: mapping of parent order ID to child order signed size
            ```
            {
                0: 5.3,
                1: 0.4,
                2: -0.3,
            }
            ```
        """
        hdbg.dassert_lte(1, num_waves)
        # From parent order.id to child order shares.
        # TODO(gp): -> parent_order_id_to_child_signed_num_shares?
        parent_order_id_to_child_order_shares: Dict[int, float] = {}
        for order in parent_orders:
            # Extract info from parent order.
            parent_order_id = order.order_id
            hdbg.dassert_not_in(
                parent_order_id, parent_order_id_to_child_order_shares
            )
            diff_signed_num_shares = order.diff_num_shares
            asset_id = order.asset_id
            hdbg.dassert_ne(0, diff_signed_num_shares)
            # TODO(Juraj): CmTask5092.
            # Get size of a single child order based on number of parent orders.
            child_order_diff_signed_num_shares = (
                diff_signed_num_shares / num_waves
            )
            hdbg.dassert_ne(0, child_order_diff_signed_num_shares)
            _LOG.debug(hprint.to_str("child_order_diff_signed_num_shares"))
            # Round to the allowable asset precision.
            amount_precision = self.market_info[asset_id]["amount_precision"]
            child_order_diff_signed_num_shares_before_floor = child_order_diff_signed_num_shares
            child_order_diff_signed_num_shares = hnumpy.floor_with_precision(
                child_order_diff_signed_num_shares,
                amount_precision
            )
            if child_order_diff_signed_num_shares_before_floor != child_order_diff_signed_num_shares:
                _LOG.debug("Share amount changed due to precision limit:")
                _LOG.debug(
                    hprint.to_str(
                    "child_order_diff_signed_num_shares_before_floor \
                        child_order_diff_signed_num_shares amount_precision"
                    )
                )
            # Update the map.
            parent_order_id_to_child_order_shares[
                parent_order_id
            ] = child_order_diff_signed_num_shares
        return parent_order_id_to_child_order_shares

    async def _submit_orders(
        self,
        orders: List[oordorde.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        passivity_factor: Optional[float] = None,
        dry_run: bool,
    ) -> Tuple[str, pd.DataFrame]:
        """
        Submit orders to the actual OMS and wait for the orders to be accepted.
        """
        # self.previous_parent_orders_timestamp = self.market_data.get_wall_clock_time()
        self.previous_parent_orders_timestamp = wall_clock_timestamp
        # Submit the orders to CCXT one by one.
        sent_orders: List[oordorde.Order] = []
        for order in orders:
            if order.type_ == "limit":
                side = "buy" if order.diff_num_shares > 0 else "sell"
                limit_price = self.load_bid_ask_and_calculate_limit_price(
                    order.asset_id, side, passivity_factor
                )
            else:
                limit_price = None
            # Submit the order to CCXT.
            sent_order, _ = await self._submit_single_order_to_ccxt_with_retry(
                order,
                order_type=order.type_,
                limit_price=limit_price,
            )
            # If order was submitted successfully append it to the list of sent
            # orders.
            if self._is_submitted_order(sent_order):
                sent_orders.append(sent_order)
        # Save sent CCXT orders to class state.
        self._previous_parent_orders = sent_orders
        # The receipt is not really needed since the order is accepted right away,
        # and we don't need to wait for the order being accepted.
        submitted_order_id = self._get_next_submitted_order_id()
        receipt = f"order_{submitted_order_id}"
        # Combine all the order information in a dataframe.
        order_dicts = [order.to_dict() for order in sent_orders]
        order_df = pd.DataFrame(order_dicts)
        _LOG.debug("order_df=%s", hpandas.df_to_str(order_df))
        return receipt, order_df
