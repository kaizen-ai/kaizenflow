"""
An implementation of broker class for CCXT.

Import as:

import oms.ccxt.ccxt_broker_v1 as occcbrv1
"""

import asyncio
import logging
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import oms.ccxt.abstract_ccxt_broker as ocabccbr
import oms.ccxt.ccxt_utils as occccuti
import oms.order as omorder

_LOG = logging.getLogger(__name__)

# 1) Every time we submit an order to CCXT (parent or child) we get back a
#    `ccxt_order` object (aka `ccxt_order_response`)
#    - It's mainly a confirmation of the order
#    - The format is https://docs.ccxt.com/#/?id=order-structure
#    - The most important info is the callback CCXT ID of the order (this is
#      the only way to do it)
# 2) For each order (parent or child), we get back from CCXT 0 or more
#    `ccxt_trades`, each representing a partial fill (price, number of shares,
#    fees), e.g.,
#    - If we walk the book, we get multiple `ccxt_trades`
#    - If we match multiple trades even at the same level, we might get different
#      ccxt_trades
#    - The format is ...
# 3) `oms_fill` represents the fill of a parent order, since outside
#    the execution system (e.g., in `Portfolio`) we don't care about tracking
#    individual fills
#    - For a parent order we need to convert multiple `ccxt_trades` into a single
#      `oms_fill`
#    - Get all the trades combined to the parent order to get a single OMS
#      fill
#      - We use this v1
#    - Another way is to query the state of an order
#      - We use this in v2 prototype, but it's not sure that it's the final
#        approach
# 4) `ccxt_fill`
#    - When the orders close, we can ask CCXT to summarize the results of
#      an order
#    - We get some information about quantity, but we don't get fees and other
#      info (e.g., prices)
#    - We save this info to cross-check our data
#    - We can't use `ccxt_fill` to create an `oms_fill` because it doesn't contain
#      enough info about prices and fees
#    - We save this info to cross-check our data

# #############################################################################
# CcxtBroker
# #############################################################################


class CcxtBroker_v1(ocabccbr.AbstractCcxtBroker):
    """
    Version 1 of CcxtBroker.

    To be deprecated in favor of `CcxtBroker_v2`
    """

    async def submit_twap_orders(
        self,
        parent_orders: List[omorder.Order],
        passivity_factor: float,
        *,
        execution_freq: Optional[str] = "1T",
    ) -> List[pd.DataFrame]:
        """
        Execute orders using the TWAP strategy.

        All parent orders are assumed to spawn the same interval of time.

        Each parent order is broken up into smaller child limit orders which
        are submitted between `parent_order_start_time` and
        `parent_order_end_time` at the provided `execution_freq`, e.g. '1T' for 1
        min.
        If a limit order is not filled by the provided timestamp, the order is
        cancelled.

        :param parent_orders: parent orders for TWAP execution
            - Example of List[Order] input represented as a dataframe:
              ```
              order_id   creation_timestamp    asset_id       type_             start_timestamp               end_timestamp  curr_num_shares  diff_num_shares  tz
              0  2023-01-18 15:46:00.011061  3065029174  price@twap  2023-01-18 15:47:00.011061  2023-01-18 15:48:00.011061              0.0           2500.0 NaN
              1  2023-01-18 15:46:31.307954  3065029175  price@twap  2023-01-18 15:47:00.011061  2023-01-18 15:48:00.011061              0.0           2500.0 NaN
              2  2023-01-18 15:46:41.889681  3065029176  price@twap  2023-01-18 15:47:00.011061  2023-01-18 15:48:00.011061              0.0           2500.0 NaN
              ```
        :param passivity_factor: value used for limit price calculation
        :param execution_freq: how often to submit child orders for each parent
        :return: a list of DataFrames of sent orders
        """
        _LOG.debug(hprint.to_str("parent_orders"))
        _LOG.debug(hprint.to_str("passivity_factor execution_freq"))
        hdbg.dassert_type_is(passivity_factor, float)
        # Log parent orders.
        self.log_oms_parent_orders(self._log_dir, self._get_wall_clock_time, parent_orders)
        # Assume that all the parent orders are aligned in terms of start / end
        # time.
        # TODO(gp): @danya check that start_timestamp and end_timestamp are the
        #  same for all orders.
        order = parent_orders[0]
        # The delay is added for safety to give enough time for calculation and
        # avoid the strict assertions in MarketData.
        # TODO(gp): @danya -> execution_start_timestamp, execution_end_timestamp
        parent_order_start_time = order.start_timestamp + pd.Timedelta("1S")
        parent_order_end_time = order.end_timestamp
        hdbg.dassert_lt(parent_order_start_time, parent_order_end_time)
        # Get CCXT symbols for the parent orders.
        parent_orders_ccxt_symbols = [
            self.asset_id_to_ccxt_symbol_mapping[parent_order.asset_id]
            for parent_order in parent_orders
        ]
        #
        execution_freq = pd.Timedelta(execution_freq)
        # Get the size of child orders for each asset_id.
        asset_ids = [parent_order.asset_id for parent_order in parent_orders]
        hdbg.dassert_no_duplicates(asset_ids)
        parent_order_ids_to_child_order_shares = (
            self._calculate_twap_child_order_size(
                parent_orders,
                parent_order_start_time,
                parent_order_end_time,
                execution_freq,
            )
        )
        hdbg.dassert_lt(0, len(parent_order_ids_to_child_order_shares))
        # We need to add a variable to track submitted children orders. Make a
        # copy to not modify the input.
        # ccxt_id -> children_order_ccxt_ids
        # TODO(gp): @all not sure we need to copy the parent orders.
        parent_orders_tmp = parent_orders.copy()
        for parent_order in parent_orders_tmp:
            # Add a `ccxt_id` assigned by the exchange at order submission.
            hdbg.dassert_not_in("ccxt_id", parent_order.extra_params)
            parent_order.extra_params["ccxt_id"]: List[int] = []
        # Wait to align with the beginning of the TWAP interval.
        # TODO(gp): await ...
        self._align_with_parent_order_start_timestamp(parent_order_start_time)
        # Execute child orders.
        # TODO(gp): @all IMO it should be computed by _calculate_twap_child_order_size
        num_child_orders = int(
            (parent_order_end_time - parent_order_start_time) / execution_freq
        )
        _LOG.debug(hprint.to_str("num_child_orders"))
        order_dfs: List[pd.DataFrame] = []
        # TODO(gp): @danya convert this into a for loop on order_num with the
        #  pre-emptive break condition.
        order_num = 0
        while True:
            # TODO(gp): This function is cancelling and resubmitting so either
            #  we improve the name or we split the phases in two functions.
            order_dfs = await self._submit_twap_child_orders(
                parent_orders_tmp,
                parent_order_ids_to_child_order_shares,
                order_dfs,
                execution_freq,
                passivity_factor,
            )
            #
            order_num += 1
            # Break if all planned orders were submitted.
            if order_num >= num_child_orders:
                _LOG.warning("All orders were executed before time was up.")
                break
            # Break if the time is up.
            if self.market_data.get_wall_clock_time() >= parent_order_end_time:
                _LOG.debug(
                    "Time is up: current_time=%s parent_order_end_time=%s",
                    self.market_data.get_wall_clock_time(),
                    parent_order_end_time,
                )
                break
            # Wait until the start of the next wave of child orders.
            # TODO(gp): await ...
            self._align_with_next_child_order_start_timestamp(
                execution_freq.seconds
            )
        # Save the submitted parent orders into the class, e.g.,
        # ```
        # [Order: order_id=0 ... extra_params={'ccxt_id': [0, 1]},
        #    ...]
        # ```
        self._previous_parent_orders = parent_orders_tmp
        _LOG.debug(hprint.to_str("self._previous_parent_orders"))
        # Cancel open orders to mark the end of TWAP iteration.
        for ccxt_symbol in parent_orders_ccxt_symbols:
            self.cancel_open_orders(ccxt_symbol)
        # Get fills for the time period between orders.
        # TODO(Danya): Replace loading trades by timestamp with loading
        #  orders by order ID as we do in `get_fills` (CMTask4286).
        # TODO(Danya): From here unique to v1, should be unified with v2.
        fills_start_timestamp = parent_order_start_time
        fills_end_timestamp = parent_order_end_time
        _LOG.debug(
            hprint.to_str(
                "fills_start_timestamp fills_end_timestamp parent_orders_ccxt_symbols"
            )
        )
        # TODO(gp): @danya The function refers to fills but they are trades.
        fills = self._get_ccxt_trades_for_time_period(
            fills_start_timestamp,
            fills_end_timestamp,
            symbols=parent_orders_ccxt_symbols,
        )
        if not fills:
            _LOG.debug(
                "No fills found for symbols=%s, start_timestamp=%s, end_timestamp=%s",
                str(parent_orders_ccxt_symbols),
                fills_start_timestamp,
                fills_end_timestamp,
            )
        self._log_child_order_fills(
            self._log_dir,
            fills,
            fills_start_timestamp,
            fills_end_timestamp,
        )
        return order_dfs

    # #########################################################################
    # Private methods.
    # #########################################################################

    # TODO(gp): @danya v1 specific. It is a previous version of _log_ccxt_fills
    #  and it should be removed.
    @staticmethod
    def _log_child_order_fills(
        log_dir: str,
        fills: List[ocabccbr.CcxtData],
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> None:
        """
        Get fills and save them to a separate file.

        Example of a file name:
        "20230331-110000_20230331-110500.json"

        :param log_dir: dir to store logs in
        :param fills: JSON of fills loaded from CCXT
        :param start_timestamp: beginning of time range for fills
        :param end_timestamp: end of time range for fills
        """
        if not log_dir:
            # No log dir provided, nothing to do.
            return None
        fills_log_dir = os.path.join(log_dir, "child_order_fills")
        hio.create_dir(fills_log_dir, incremental=True)
        # Generate file names.
        start_timestamp_str = start_timestamp.strftime("%Y%m%d-%H%M%S")
        end_timestamp_str = end_timestamp.strftime("%Y%m%d-%H%M%S")
        file_name = os.path.join(
            fills_log_dir, f"{start_timestamp_str}_{end_timestamp_str}.json"
        )
        # Save fills JSON to file.
        hio.to_json(file_name, fills)

    def _get_ccxt_trades_for_time_period(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        symbols: Optional[List[str]] = None,
    ) -> List[ocabccbr.CcxtData]:
        """
        Get a list of CCXT trades for a given time period [a, b] in JSON
        format.

        This is used by objects other than oms.Portfolio (e.g., scripts) to
        retrieve all the CCXT trades corresponding to a period of time.

        Note that in case of long time periods (>24h) the pagination is done by
        day, which can lead to more data being downloaded than expected.

        :return: list of dictionary, each containing trade information
        """
        hdbg.dassert_isinstance(start_timestamp, pd.Timestamp)
        hdbg.dassert_isinstance(end_timestamp, pd.Timestamp)
        hdbg.dassert_lte(start_timestamp, end_timestamp)
        #
        start_timestamp = hdateti.convert_timestamp_to_unix_epoch(start_timestamp)
        end_timestamp = hdateti.convert_timestamp_to_unix_epoch(end_timestamp)
        # Get CCXT trades symbol by symbol.
        trades = []
        symbols = symbols or list(self.ccxt_symbol_to_asset_id_mapping.keys())
        day_in_millisecs = 24 * 60 * 60 * 1000
        for symbol in symbols:
            if end_timestamp - start_timestamp <= day_in_millisecs:
                # Download all trades if period is less than 24 hours.
                _LOG.debug(
                    "Downloading period=%s, %s", start_timestamp, end_timestamp
                )
                symbol_trades = self._exchange.fetchMyTrades(
                    symbol=symbol,
                    since=start_timestamp,
                    params={"endTime": end_timestamp},
                )
            else:
                # Download day-by-day for longer time periods.
                symbol_trades = []
                for timestamp in range(
                    start_timestamp, end_timestamp + 1, day_in_millisecs
                ):
                    _LOG.debug(
                        "Downloading period=%s, %s", timestamp, day_in_millisecs
                    )
                    day_trades = self._exchange.fetchMyTrades(
                        symbol=symbol,
                        since=timestamp,
                        params={"endTime": timestamp + day_in_millisecs},
                    )
                    symbol_trades.extend(day_trades)
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
            # Add the asset ids to each trade.
            asset_id = self.ccxt_symbol_to_asset_id_mapping[symbol]
            trades_with_asset_ids = []
            for symbol_trade in symbol_trades:
                _LOG.debug("symbol_trade=%s", symbol_trade)
                # Get the position of the full symbol field to paste the asset id
                # after it.
                hdbg.dassert_in("symbol", symbol_trade.keys())
                idx = list(symbol_trade.keys()).index("symbol") + 1
                # Add asset id.
                symbol_trade = list(symbol_trade.items())
                symbol_trade.insert(idx, ("asset_id", asset_id))
                _LOG.debug("after transformation: symbol_trade=%s", symbol_trade)
                # Accumulate.
                trades_with_asset_ids.append(dict(symbol_trade))
            # Accumulate.
            trades.extend(trades_with_asset_ids)
        return trades

    # //////////////////////////////////////////////////////////////////////////

    # This is the sequential v1 version.
    async def _submit_twap_child_orders(
        self,
        parent_orders_tmp: List[omorder.Order],
        parent_order_ids_to_child_order_shares: Dict[int, float],
        order_dfs: List[pd.DataFrame],
        execution_freq: pd.Timedelta,
        passivity_factor: float,
    ) -> List[pd.DataFrame]:
        """
        Given a set of parent orders, create and submit TWAP children orders.
        """
        # Get market data for the latest available 10 seconds.
        bid_ask_data = self._bid_ask_market_data.get_data_for_last_period(
            pd.Timedelta("10S")
        )
        _LOG.debug(hpandas.df_to_str(bid_ask_data))
        # Get all the open positions to determine `curr_num_shares`.
        open_positions = self.get_open_positions()
        for parent_order in parent_orders_tmp:
            # Get the total shares we want to achieve for the parent order during
            # the entire TWAP order.
            parent_order_id = parent_order.order_id
            child_order_diff_signed_num_shares = (
                parent_order_ids_to_child_order_shares[parent_order_id]
            )
            # Skip the child order if it is empty after rounding down.
            if child_order_diff_signed_num_shares == 0:
                _LOG.debug(
                    "Child order for parent_order=%s not sent, %s",
                    str(parent_order),
                    hprint.to_str("child_order_diff_signed_num_shares"),
                )
                continue
            asset_id = parent_order.asset_id
            currency_pair = self.asset_id_to_ccxt_symbol_mapping[asset_id]
            creation_timestamp = self.market_data.get_wall_clock_time()
            start_timestamp = creation_timestamp
            # TODO(Danya): Set end_timestamp as a cancellation time on the exchange.
            # TODO(Danya): make the end_timestamp round up to the nearest minute.
            end_timestamp = creation_timestamp + execution_freq
            # Cancel any open order.
            self.cancel_open_orders(currency_pair)
            # Get open positions.
            # If no open position is found, Binance doesn't return as a key,
            # so we count that as 0.
            curr_num_shares = open_positions.get(currency_pair, 0)
            _LOG.debug(hprint.to_str("currency_pair curr_num_shares"))
            type_ = "limit"
            _LOG.debug(
                hprint.to_str(
                    "creation_timestamp asset_id type_ start_timestamp"
                    " end_timestamp curr_num_shares"
                    " child_order_diff_signed_num_shares"
                )
            )
            # Create child order.
            child_order = omorder.Order(
                creation_timestamp,
                asset_id,
                type_,
                start_timestamp,
                end_timestamp,
                curr_num_shares,
                child_order_diff_signed_num_shares,
            )
            _LOG.debug("child_order=%s", str(child_order))
            # Calculate limit price.
            side = "buy" if child_order_diff_signed_num_shares > 0 else "sell"
            price_dict = occccuti.calculate_limit_price(
                bid_ask_data, asset_id, side, passivity_factor
            )
            _LOG.debug(hprint.to_str("price_dict"))
            (
                child_order,
                ccxt_child_order_response,
            ) = await self._submit_single_order_to_ccxt_with_safe_retry(
                child_order,
                order_type="limit",
                limit_price=price_dict["limit_price"],
            )
            is_submitted_order = self._is_submitted_order(child_order)
            if is_submitted_order:
                # Set last order execution Timestamp.
                self.previous_parent_orders_timestamp = (
                    child_order.creation_timestamp
                )
                # Update the parent order with child's order ccxt_id.
                child_order_ccxt_id = child_order.extra_params["ccxt_id"]
                hdbg.dassert_not_in(
                    child_order_ccxt_id, parent_order.extra_params["ccxt_id"]
                )
                parent_order.extra_params["ccxt_id"].append(child_order_ccxt_id)
                _LOG.debug(hprint.to_str("is_submitted_order"))
            else:
                _LOG.warning(
                    "Order is not submitted to CCXT, %s",
                    str(child_order),
                )
            # Log submitted child order with extra price info.
            self.log_child_order(
                self._log_dir,
                self._get_wall_clock_time,
                child_order,
                ccxt_child_order_response,
                price_dict,
            )
            order_dfs.append(child_order)
            # Sleep between child orders to avoid exceeding the rate limit.
            await asyncio.sleep(1)
        return order_dfs

    # //////////////////////////////////////////////////////////////////

    async def _submit_orders(
        self,
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ) -> Tuple[str, pd.DataFrame]:
        """
        Submit orders to the actual OMS and wait for the orders to be accepted.
        """
        # self.previous_parent_orders_timestamp = self.market_data.get_wall_clock_time()
        self.previous_parent_orders_timestamp = wall_clock_timestamp
        # Submit the orders to CCXT one by one.
        sent_orders: List[omorder.Order] = []
        for order in orders:
            sent_order, _ = await self._submit_single_order_to_ccxt_with_retry(
                order
            )
            # If order was submitted successfully append it to the list of sent
            # orders.
            # TODO(Grisha): consider storing `ccxt_id` in a list instead of
            #  an int to make a market order look like a parent TWAP order,
            #  i.e. "ccxt": 1 -> "ccxt_id": [1].
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
