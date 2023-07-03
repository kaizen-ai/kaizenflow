"""
Import as:

import oms.ccxt.ccxt_broker_v2 as occcbrv2
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.htimer as htimer
import helpers.hwall_clock_time as hwacltim
import oms.ccxt.abstract_ccxt_broker as ocabccbr
import oms.ccxt.ccxt_utils as occccuti
import oms.order as omorder

_LOG = logging.getLogger(__name__)


# #############################################################################
# CcxtBroker_v2
# #############################################################################


class CcxtBroker_v2(ocabccbr.AbstractCcxtBroker):
    # @staticmethod
    # async def custom_gather(coroutines: List[Coroutine]) -> Any:
    #     """
    #     Execute multiple coroutines.
    #     """
    #     values = await asyncio.gather(*coroutines)
    #     return values

    # TODO(gp): Implement this using the script run_ccxt_broker_v2.py
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
        self.log_oms_parent_orders(self._log_dir, self._get_wall_clock_time, parent_orders)
        _LOG.debug(hprint.to_str("passivity_factor execution_freq"))
        hdbg.dassert_type_is(passivity_factor, float)
        # Assume that all the parent orders are aligned in terms of start / end
        # time.
        # TODO(gp): @danya check that start_timestamp and end_timestamp are the
        #  same for all orders.
        order = parent_orders[0]
        # The delay is added for safety to give enough time for calculation and
        # avoid the strict assertions in MarketData.
        # TODO(gp): @danya -> execution_start_timestamp, execution_end_timestamp
        execution_start_timestamp = order.start_timestamp + pd.Timedelta("1S")
        execution_end_timestamp = order.end_timestamp
        hdbg.dassert_lt(execution_start_timestamp, execution_end_timestamp)
        #
        execution_freq = pd.Timedelta(execution_freq)
        # Get CCXT symbols for the parent orders.
        parent_orders_ccxt_symbols = [
            self.asset_id_to_ccxt_symbol_mapping[parent_order.asset_id]
            for parent_order in parent_orders
        ]
        # Get the size of child orders for each asset_id.
        # TODO(gp): @danya Assert that all asset_ids are different.
        # TODO(gp): @danya Verify that the number of assets is not zero.
        parent_order_ids_to_child_order_shares = (
            self._calculate_twap_child_order_size(
                parent_orders,
                execution_start_timestamp,
                execution_end_timestamp,
                execution_freq,
            )
        )
        _LOG.debug(
            "TWAP child order size calculated timestamp=%s",
            self.market_data.get_wall_clock_time(),
        )
        # We need to add a variable to track submitted orders. Make a copy to not
        # modify the input.
        parent_orders_single_iteration_copy = parent_orders.copy()
        for parent_order in parent_orders_single_iteration_copy:
            self._update_stats(
                parent_order,
                "submit_twap_orders::start",
                self.market_data.get_wall_clock_time(),
            )
            # Add a `ccxt_id` assigned by the exchange at order submission.
            hdbg.dassert_not_in("ccxt_id", parent_order.extra_params)
            parent_order.extra_params["ccxt_id"]: List[int] = []
        # Wait to align with the beginning of the TWAP interval.
        for parent_order in parent_orders_single_iteration_copy:
            self._update_stats(
                parent_order,
                "submit_twap_orders::align_with_parent_order.start",
                self.market_data.get_wall_clock_time(),
            )
        self._align_with_parent_order_start_timestamp(execution_start_timestamp)
        for parent_order in parent_orders_single_iteration_copy:
            self._update_stats(
                parent_order,
                "submit_twap_orders::align_with_parent_order.start",
                self.market_data.get_wall_clock_time(),
            )
        # TODO(gp): @danya convert this into a for loop on order_num with the
        #  pre-emptive break condition.
        while True:
            # TODO(Danya): Replace with v2 way of placing orders inside
            #  _submit_twap_child_orders which places the orders in parallel.
            child_orders = await self._submit_twap_child_orders(
                parent_orders_single_iteration_copy,
                parent_order_ids_to_child_order_shares,
                execution_freq,
                passivity_factor,
            )
            # Cancel orders after waiting for them to finish.
            # Note: the alignment of the waiting time for the next minute
            # is handled in `_submit_twap_child_order` coroutine.
            for ccxt_symbol in parent_orders_ccxt_symbols:
                self.cancel_open_orders(ccxt_symbol)
            # Log CCXT fills and trades.
            if self._log_dir is not None:
                ccxt_fills = self.get_ccxt_fills(child_orders)
                ccxt_trades = self.get_ccxt_trades(ccxt_fills)
                oms_fills=[]
                self.log_ccxt_fills(self._log_dir, self._get_wall_clock_time, ccxt_fills, ccxt_trades, oms_fills)
            _LOG.debug(
                "Orders cancelled timestamp=%s",
                self.market_data.get_wall_clock_time(),
            )
            # order_num += 1
            # # Break if all planned orders were submitted.
            # if order_num >= num_child_orders:
            #     _LOG.warning("All orders were executed before time was up.")
            #     break
            # # Break if the time is up.
            current_wall_clock_time = self.market_data.get_wall_clock_time()
            _LOG.debug(
                hprint.to_str(
                    "current_wall_clock_time execution_end_timestamp"
                )
            )
            if current_wall_clock_time >= execution_end_timestamp:
                _LOG.debug(
                    "Time is up: current_time=%s parent_order_end_time=%s",
                    self.market_data.get_wall_clock_time(),
                    execution_end_timestamp,
                )
                break
            # # Wait until the start of the next wave of child orders.
            # self._align_with_next_child_order_start_timestamp(
            #     execution_freq.seconds
            # )
        # Save the submitted parent orders into the class, e.g.,
        # ```
        # [Order: order_id=0 ... extra_params={'ccxt_id': [0, 1]},
        #    ...]
        # ```
        self._previous_parent_orders = parent_orders_single_iteration_copy
        _LOG.debug(hprint.to_str("self._previous_parent_orders"))
        # Cancel open orders to mark the end of TWAP iteration.
        return child_orders

    # TODO(gp): This was just a workaround to get the data to the script to
    #  do the logging. Moving fwd this is not needed.
    def get_ccxt_fills(
        self, orders: List[omorder.Order]
    ) -> Tuple[List[Dict[str, Any]]]:
        """
        Get fills from submitted orders in OMS and in CCXT formats.

        The fills are fetched based on the CCXT ID of the orders. If the
        order was not submitted, it is skipped.
        """
        ccxt_order_structures = []
        for order in orders:
            # Get the corresponding CCXT order structure from the exchange.
            ccxt_order_structure = self._get_ccxt_order_structure(order)
            if ccxt_order_structure is None:
                continue
            # Create a Fill object based on the exchange order structure.
            ccxt_order_structures.append(ccxt_order_structure)
        return ccxt_order_structures

    @staticmethod
    def _update_stats_for_order(
        order: omorder.Order, tag: str, value: Any
    ) -> None:
        _LOG.debug("order_id=%s -> tag=%s value=%s", order.id, tag, value)
        if "stats" not in order.extra_params:
            order.extra_params["stats"] = {}
        order_stats = order.extra_params["stats"]
        # TODO(gp): Re-enable and fix the problem.
        # hdbg.dassert_not_in(tag, order_stats)
        order_stats[tag] = value

    @staticmethod
    def _update_stats(order, tag, value):
        if "stats" not in order.extra_params:
            order.extra_params["stats"] = {}
        _LOG.debug("order=%s tag=%s value=%s", str(order), tag, value)
        order_stats = order.extra_params["stats"]
        # TODO(gp): Re-enable and fix the problem.
        # hdbg.dassert_not_in(tag, order_stats)
        order_stats[tag] = value

    def _update_stats_for_orders(self, parent_orders_single_iteration_copy):
        for parent_order in parent_orders_single_iteration_copy:
            self._update_stats(
                parent_order,
                "submit_twap_orders::start",
                self.market_data.get_wall_clock_time(),
            )

    # TODO(gp): Fix types.
    async def _submit_twap_child_order(
        self,
        parent_order_ids_to_child_order_shares,
        open_positions,
        execution_freq,
        passivity_factor,
        bid_ask_data,
        parent_order,
    ) -> omorder.Order:
        # Get the total shares we want to achieve for the parent order during
        # the entire TWAP order.
        self._update_stats(
            parent_order,
            "_submit_twap_child_order::start",
            self.market_data.get_wall_clock_time(),
        )
        parent_order_id = parent_order.order_id
        child_order_diff_signed_num_shares = (
            parent_order_ids_to_child_order_shares[parent_order_id]
        )
        # # Skip the child order if it is empty after rounding down.
        # if child_order_diff_signed_num_shares == 0:
        #     _LOG.debug(
        #         "Child order for parent_order=%s not sent, %s",
        #         str(parent_order),
        #         hprint.to_str("child_order_diff_signed_num_shares"),
        #     )
        #     continue
        asset_id = parent_order.asset_id
        currency_pair = self.asset_id_to_ccxt_symbol_mapping[asset_id]
        creation_timestamp = self.market_data.get_wall_clock_time()
        start_timestamp = creation_timestamp
        # TODO(Danya): Set end_timestamp as a cancellation time on the exchange.
        # TODO(Danya): make the end_timestamp round up to the nearest minute.
        end_timestamp = creation_timestamp + execution_freq
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
        self._update_stats(
            child_order,
            "child_order_created",
            self.market_data.get_wall_clock_time(),
        )
        # Calculate limit price.
        side = "buy" if child_order_diff_signed_num_shares > 0 else "sell"
        price_dict = occccuti.calculate_limit_price(
            bid_ask_data, asset_id, side, passivity_factor
        )
        self._update_stats(
            child_order,
            "child_order_limit_price_calculated",
            self.market_data.get_wall_clock_time(),
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
        self._update_stats(
            child_order,
            "child_order_submitted",
            self.market_data.get_wall_clock_time(),
        )
        # TODO(gp): Pass the execution frequency.
        await self._align_with_next_child_order_start_timestamp(60)
        self._update_stats(
            child_order,
            "aligned_with_next_child_order_start_timestamp",
            self.market_data.get_wall_clock_time(),
        )
        #
        is_submitted_order = self._is_submitted_order(child_order)
        if is_submitted_order:
            # Set last order execution Timestamp.
            self.previous_parent_orders_timestamp = child_order.creation_timestamp
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
        self._update_stats(
            parent_order,
            "child_order_id_added_to_parent_order",
            self.market_data.get_wall_clock_time(),
        )
        # TODO(Danya): Move above.
        # Here log CCXT trades and CCXT fills.
        # Log submitted child order with extra price info.
        self.log_child_order(
            self._log_dir,
            self._get_wall_clock_time,
            child_order,
            ccxt_child_order_response,
            price_dict,
        )
        self._update_stats(
            child_order,
            "child_order_logged",
            self.market_data.get_wall_clock_time(),
        )
        return child_order

    # This is the parallel v2 version.
    async def _submit_twap_child_orders(
        self,
        parent_orders_tmp: List[omorder.Order],
        parent_order_ids_to_child_order_shares: Dict[int, float],
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
        _LOG.debug(hprint.to_str("open_positions"))
        #
        coroutines = []
        for order in parent_orders_tmp:
            self._update_stats(
                order,
                "bid_ask_market_data_done",
                self.market_data.get_wall_clock_time(),
            )
            # We don't need an `await` because we are just creating a coroutine
            # that we will execute later.
            coroutine = self._submit_twap_child_order(
                parent_order_ids_to_child_order_shares,
                open_positions,
                execution_freq,
                passivity_factor,
                bid_ask_data,
                order,
            )
            self._update_stats(
                order,
                "order_coroutines_created_timestamp",
                self.market_data.get_wall_clock_time(),
            )
            coroutines.append(coroutine)
        # Submit all orders concurrently.
        with htimer.TimedScope(
            logging.DEBUG, "asyncio_order_submission_and_wait_time"
        ) as ts:
            # order_submissions= await custom_gather(coroutines)
            child_orders = await asyncio.gather(*coroutines)
            _LOG.debug(
                "all_coroutines_finished=%s",
                self.market_data.get_wall_clock_time(),
            )
        #
        # order_submission_time_scope=ts.get_result()
        return child_orders

    async def _submit_orders(
        self,
        orders: List[omorder.Order],
        wall_clock_timestamp: pd.Timestamp,
        *,
        dry_run: bool,
    ):
        raise ValueError("Do not get here")

    # TODO(gp): This was just a workaround to get the data to the script to
    #  do the logging. Moving fwd this is not needed.
    # def get_fills(
    #     self, orders: List[omorder.Order]
    # ) -> Tuple[List[ombroker.Fill], List[ocabccbr.CcxtData]]:
    #     """
    #     Get fills from submitted orders in OMS and in CCXT formats.
    #
    #     The fills are fetched based on the CCXT ID of the orders. If the
    #     order was not submitted, it is skipped.
    #     """
    #     fills = []
    #     ccxt_order_structures = []
    #     for order in orders:
    #         # Get the corresponding CCXT order structure from the exchange.
    #         ccxt_order_structure = self._get_ccxt_order_structure(order)
    #         if ccxt_order_structure is None:
    #             continue
    #         # Create a Fill object based on the exchange order structure.
    #         fill = self._get_fill(order, ccxt_order_structure)
    #         if fill is None:
    #             continue
    #         ccxt_order_structures.append(ccxt_order_structure)
    #         fills.append(fill)
    #     return fills, ccxt_order_structures

    # # TODO(gp): Specific of v2.
    # @staticmethod
    # def _get_fill(
    #     order: omorder.Order, ccxt_order: ocabccbr.CcxtData
    # ) -> Optional[ombroker.Fill]:
    #     """
    #     Create a `oms.Fill` object based on `oms.Order` and CCXT order
    #     structure.
    #     """
    #     # Get order update time.
    #     # In CCXT 'updateTime' represents the last time the order status was
    #     # changed, either by fill, partial fill, or cancellation.
    #     fill_timestamp = int(ccxt_order["info"]["updateTime"])
    #     fill_timestamp = hdateti.convert_unix_epoch_to_timestamp(fill_timestamp)
    #     fill_amount = float(ccxt_order["filled"])
    #     if fill_amount == 0:
    #         # If the order was not filled, skip the fill generation.
    #         oms_fill = None
    #     else:
    #         # Select total cost of the fill.
    #         # 'cost' is calculated as price * filled volume.
    #         fill_price = float(ccxt_order["cost"])
    #         oms_fill = ombroker.Fill(
    #             order,
    #             fill_timestamp,
    #             fill_amount,
    #             fill_price,
    #         )
    #     return oms_fill

    # TODO(Danya): Transfer from `run_ccxt_broker_v2.py` script.
    # async def _submit_orders(
    #     self,
    #     orders: List[omorder.Order],
    #     wall_clock_timestamp: pd.Timestamp,
    #     *,
    #     dry_run: bool,
    # ) -> Tuple[str, pd.DataFrame]:
    #     order_submission_time_log = {}
    #     # Prepare all orders to submit as coroutines.
    #     coroutines = []
    #     for order in orders:
    #         # TODO(Danya): Transfer from `run_ccxt_broker_v2.py`
    #         coroutine = send_order(bid_ask_data, order)
    #         coroutines.append(coroutine)
    #     order_submission_time_log[
    #         "order_coroutines_created_timestamp"
    #     ] = self.market_data.get_wall_clock_time()
    #     # Submit all orders concurrently.
    #     with htimer.TimedScope(
    #         logging.DEBUG, "asyncio_order_submission_and_wait_time"
    #     ) as ts:
    #         # order_submissions=asyncio.run(custom_gather(coroutines))
    #         order_submissions = await self.custom_gather(coroutines)
    #     order_submission_time_scope = ts.get_result()
    #     order_submission_time_log[
    #         "order_submission_time_scope"
    #     ] = order_submission_time_scope
    #     order_submission_time_log[
    #         "all_order_submission_ended_timestamp"
    #     ] = self.market_data.get_wall_clock_time()
    #     # Log submitted orders and order responses.
    #     submitted_orders = []
    #     for order_submission in order_submissions:
    #         submitted_order, order_response, price_dict = order_submission
    #         submitted_orders.append(submitted_order)
    #         wall_clock_time = self.market_data.get_wall_clock_time
    #         self.log_child_order(
    #             self._log_dir,
    #             self._get_wall_clock_time,
    #             submitted_order,
    #             order_response,
    #             price_dict,
    #         )

    # /////////////////////////////////////////////////////////////////////////////

    # TODO(gp): Specific of v2.
    def _get_ccxt_order_structure(
        self, order: omorder.Order
    ) -> Optional[ocabccbr.CcxtData]:
        """
        Get the CCXT order structure corresponding to the submitted order.
        """
        asset_id = order.asset_id
        symbol = self.asset_id_to_ccxt_symbol_mapping[asset_id]
        hdbg.dassert_in("ccxt_id", order.extra_params)
        #
        ccxt_id = order.extra_params["ccxt_id"]
        if ccxt_id == -1:
            _LOG.debug("Order=%s has no CCXT ID.", str(order))
            ccxt_order = None
        else:
            # Get the order status by its CCXT ID.
            ccxt_order = self._exchange.fetch_order(
                id=str(ccxt_id), symbol=symbol
            )
        return ccxt_order


# #############################################################################
