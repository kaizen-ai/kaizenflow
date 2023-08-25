"""
Import as:

import oms.broker.ccxt.ccxt_broker_v2 as obccbrv2
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.htimer as htimer
import oms.broker.ccxt.abstract_ccxt_broker as obcaccbr
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)


# #############################################################################
# CcxtBroker_v2
# #############################################################################


class CcxtBroker_v2(obcaccbr.AbstractCcxtBroker):
    async def submit_twap_orders(
        self,
        parent_orders: List[oordorde.Order],
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

        Invariants for submitting TWAP orders:
        - Cancelling orders from the most recent wave needs to happen at the beginning of the new wave
        (otherwise the TWAP orders do not have time to get filled)
        - Obtaining and logging fills and trades from the most recent wave must happen strictly after
        cancellation from the previous wave otherwise incorrect data can be logged
        - Obtaining bid/ask data for the current wave mustt happen as close to actual order submission as
        possible, in order to avoid using stale data

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
        # Log OMS parent orders before the submission of the first wave.
        self.log_oms_parent_orders(
            self._log_dir, self._get_wall_clock_time, parent_orders
        )
        _LOG.debug(hprint.to_str("passivity_factor execution_freq"))
        hdbg.dassert_type_is(passivity_factor, float)
        order = parent_orders[0]
        hdbg.dassert_all_attributes_are_same(
            parent_orders,
            "start_timestamp",
            msg="All parent orders must have the same start time",
        )
        hdbg.dassert_all_attributes_are_same(
            parent_orders,
            "end_timestamp",
            msg="All parent orders must have the same end time",
        )
        # The delay is added for safety to give enough time for calculation and
        # avoid the strict assertions in MarketData.
        # execution_start_timestamp = order.start_timestamp + pd.Timedelta("1S")
        execution_start_timestamp = order.start_timestamp
        execution_end_timestamp = order.end_timestamp
        hdbg.dassert_lt(execution_start_timestamp, execution_end_timestamp)
        #
        execution_freq = pd.Timedelta(execution_freq)
        # Get CCXT symbols for the parent orders.
        parent_orders_ccxt_symbols = [
            self.asset_id_to_ccxt_symbol_mapping[parent_order.asset_id]
            for parent_order in parent_orders
        ]
        for order in parent_orders:
            hdbg.dassert_ne(
                order.diff_num_shares,
                0,
                msg=f"Parent order: {str(order)}, cannot be zero size",
            )
        # Get the size of child orders for each asset_id.
        parent_orders_asset_ids = [order.asset_id for order in parent_orders]
        hdbg.dassert_no_duplicates(
            parent_orders_asset_ids,
            msg="All parent orders must have different asset_id",
        )
        # We need to add a variable to track submitted orders. Make a copy to not
        # modify the input.
        parent_orders_copy = parent_orders.copy()
        for parent_order in parent_orders_copy:
            self._update_stats_for_order(
                parent_order,
                "start",
                self.market_data.get_wall_clock_time(),
            )
            # Add a `ccxt_id` assigned by the exchange at order submission.
            hdbg.dassert_not_in("ccxt_id", parent_order.extra_params)
            parent_order.extra_params["ccxt_id"]: List[int] = []
        # Wait to align with the beginning of the TWAP interval.
        for parent_order in parent_orders_copy:
            self._update_stats_for_order(
                parent_order,
                "align_with_parent_order.start",
                self.market_data.get_wall_clock_time(),
            )
        self._align_with_parent_order_start_timestamp(execution_start_timestamp)
        for parent_order in parent_orders_copy:
            self._update_stats_for_order(
                parent_order,
                "align_with_parent_order.end",
                self.market_data.get_wall_clock_time(),
            )
        # Compute the number of waves of child order submissions.
        # E.g., for 5-minute interval for parent orders and one wave of child orders per minute
        # there are 5 waves.
        execution_freq = pd.Timedelta(execution_freq)
        num_waves = self._calculate_num_twap_child_order_waves(execution_end_timestamp, execution_freq)
        _LOG.debug(hprint.to_str("num_waves"))
        parent_order_ids_to_child_order_shares = (
            self._calculate_twap_child_order_size(
                parent_orders,
                num_waves
            )
        )
        _LOG.debug(
            "TWAP child order size calculated timestamp=%s",
            self.market_data.get_wall_clock_time(),
        )
        child_orders = []
        for wave_id in range(num_waves):
            child_orders_iter = await self._submit_twap_child_orders(
                parent_orders_copy,
                parent_order_ids_to_child_order_shares,
                execution_freq,
                passivity_factor,
                wave_id,
            )
            # Log parent orders state right after the submission to keep track
            # of the bound child order CCXT IDs.
            self.log_oms_parent_orders(
                self._log_dir, self._get_wall_clock_time, parent_orders_copy
            )
            child_orders.extend(child_orders_iter)
            # Cancel orders after waiting for them to finish.
            # Note: the alignment of the waiting time for the next minute
            # is handled in `_submit_twap_child_order` coroutine.
            hasynci.sync_wait_until(
                self._get_wall_clock_time().ceil(execution_freq),
                self._get_wall_clock_time,
            )
            _LOG.debug(
                "After syncing with next child wave=%s",
                self.market_data.get_wall_clock_time(),
            )
            await self.cancel_open_orders_for_symbols(parent_orders_ccxt_symbols)
            _LOG.debug(
                "Orders cancelled timestamp=%s",
                self.market_data.get_wall_clock_time(),
            )
            # Log CCXT fills and trades.
            if self._log_dir is not None:
                await self._log_last_wave_results(child_orders_iter)
            # Break if the time is up.
            current_wall_clock_time = self.market_data.get_wall_clock_time()
            _LOG.debug(
                hprint.to_str("current_wall_clock_time execution_end_timestamp")
            )
            if current_wall_clock_time >= execution_end_timestamp:
                _LOG.debug(
                    "Time is up: current_time=%s parent_order_end_time=%s",
                    self.market_data.get_wall_clock_time(),
                    execution_end_timestamp,
                )
                break
        else:
            _LOG.warning("All orders were executed before time was up.")
        # Save the submitted parent orders into the class, e.g.,
        # ```
        # [Order: order_id=0 ... extra_params={'ccxt_id': [0, 1]},
        #    ...]
        # ```
        self._previous_parent_orders = parent_orders_copy
        _LOG.debug(hprint.to_str("self._previous_parent_orders"))
        return child_orders

    async def get_ccxt_fills(
        self, orders: List[oordorde.Order]
    ) -> Tuple[List[Dict[str, Any]]]:
        """
        Get fills from submitted orders in OMS and in CCXT formats.

        The fills are fetched based on the CCXT ID of the orders. If the
        order was not submitted, it is skipped.
        """
        tasks = [self._get_ccxt_order_structure(order) for order in orders]
        ccxt_order_structures = await asyncio.gather(*tasks)
        # Filter out Nones.
        ccxt_order_structures = [
            i for i in ccxt_order_structures if i is not None
        ]
        _LOG.debug(
            "CCXT fills loaded timestamp=%s",
            self.market_data.get_wall_clock_time(),
        )
        return ccxt_order_structures

    # @staticmethod
    # async def custom_gather(coroutines: List[Coroutine]) -> Any:
    #     """
    #     Execute multiple coroutines.
    #     """
    #     values = await asyncio.gather(*coroutines)
    #     return values

    @staticmethod
    def _is_all_attributes_in_list_the_same(
        list_: List[Any], attribute_name: str, message: str = None
    ) -> Optional[bool]:
        """
        Check if all the elements in the list have the same attribute value.

        :param list_: list of objects
        :param attribute_name: name of the attribute to check
        :param message: message to raise if the check fails
        :return: True if all the elements in the list have the same attribute
        """
        unique_attribures = set(
            getattr(element, attribute_name) for element in list_
        )
        result = len(unique_attribures) == 1
        if message is not None:
            hdbg.dassert(result, message)
        else:
            return result

    async def _log_last_wave_results(
        self, child_orders: List[oordorde.Order]
    ) -> None:
        """
        Obtain fills and trades info from the previous wave and log it.
        """
        ccxt_fills = await self.get_ccxt_fills(child_orders)
        ccxt_trades = await self.get_ccxt_trades(ccxt_fills)
        self.log_oms_ccxt_fills(
            self._log_dir,
            self._get_wall_clock_time,
            ccxt_fills,
            ccxt_trades,
            [],
        )
        _LOG.debug(
            "CCXT fills and trades logging finished=%s",
            self.market_data.get_wall_clock_time(),
        )

    async def _submit_twap_child_order(
        self,
        parent_order_ids_to_child_order_shares: Dict[int, float],
        open_positions: Dict[str, float],
        execution_freq: pd.Timedelta,
        passivity_factor: float,
        bid_ask_data: pd.DataFrame,
        parent_order: oordorde.Order,
        iter_num: int,
    ) -> Optional[oordorde.Order]:
        # Get the total shares we want to achieve for the parent order during
        # the entire TWAP order.
        self._update_stats_for_order(
            parent_order,
            f"start.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        # Get the number of shares for the child order.
        child_order_diff_signed_num_shares = (
            parent_order_ids_to_child_order_shares[parent_order.order_id]
        )
        # Skip the child order if it is empty after rounding down.
        # Child orders skipped due to zero size cannot be constructed
        # as the Order object, so a None is returned.
        if self._skip_child_order_if_needed(
            parent_order, child_order_diff_signed_num_shares
        ):
            return None
        asset_id = parent_order.asset_id
        currency_pair = self.asset_id_to_ccxt_symbol_mapping[asset_id]
        creation_timestamp = self.market_data.get_wall_clock_time()
        execution_start_timestamp = creation_timestamp
        # TODO(Danya): Set end_timestamp as a cancellation time on the exchange.
        # TODO(Danya): make the end_timestamp round up to the nearest minute.
        execution_end_timestamp = (creation_timestamp + execution_freq).floor("T")
        # Get open positions.
        # If no open position is found, Binance doesn't return as a key,
        # so we count that as 0.
        curr_num_shares = open_positions.get(currency_pair, 0)
        _LOG.debug(hprint.to_str("currency_pair curr_num_shares"))
        type_ = "limit"
        _LOG.debug(
            hprint.to_str(
                "creation_timestamp asset_id type_ execution_start_timestamp"
                " execution_end_timestamp curr_num_shares"
                " child_order_diff_signed_num_shares"
            )
        )
        # Create child order.
        child_order = oordorde.Order(
            creation_timestamp,
            asset_id,
            type_,
            execution_start_timestamp,
            execution_end_timestamp,
            curr_num_shares,
            child_order_diff_signed_num_shares,
        )
        # Add the order_id of the parent OMS order.
        # Each child order should have only one corresponding parent order ID.
        parent_order_id_key = "oms_parent_order_id"
        hdbg.dassert_not_in(parent_order_id_key, child_order.extra_params)
        child_order.extra_params[parent_order_id_key] = parent_order.order_id
        # Transfer parent order timing logs related to the child order.
        # These include the getting the bid/ask data and open positions.
        _LOG.debug(parent_order.extra_params["stats"])
        self._update_stats_for_order(
            child_order,
            f"bid_ask_market_data.start.{iter_num}",
            parent_order.extra_params["stats"][
                f"_submit_twap_child_orders::bid_ask_market_data.start.{iter_num}"
            ],
        )
        self._update_stats_for_order(
            child_order,
            f"bid_ask_market_data.done.{iter_num}",
            parent_order.extra_params["stats"][
                f"_submit_twap_child_orders::bid_ask_market_data.done.{iter_num}"
            ],
        )
        self._update_stats_for_order(
            child_order,
            f"get_open_positions.done.{iter_num}",
            parent_order.extra_params["stats"][
                f"_submit_twap_child_orders::get_open_positions.done.{iter_num}"
            ],
        )

        self._update_stats_for_order(
            child_order,
            f"child_order.created.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        # Calculate limit price.
        side = "buy" if child_order_diff_signed_num_shares > 0 else "sell"
        price_dict = self._calculate_limit_price(
            bid_ask_data, asset_id, side, passivity_factor
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.limit_price_calculated.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        _LOG.debug(hprint.to_str("price_dict"))
        (
            child_order,
            ccxt_child_order_response,
        ) = await self._submit_single_order_to_ccxt_with_retry(
            child_order,
            order_type="limit",
            limit_price=price_dict["limit_price"],
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.submitted.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        # Here log CCXT trades and CCXT fills.
        # Log submitted child order with extra price info.
        self.log_child_order(
            self._log_dir,
            self._get_wall_clock_time,
            child_order,
            ccxt_child_order_response,
            price_dict,
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.logged.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        # TODO(Juraj): this is no longer correct log message.
        self._update_stats_for_order(
            child_order,
            f"aligned_with_next_child_order.start.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        #
        is_submitted_order = self._is_submitted_order(child_order)
        if is_submitted_order:
            # Set last order execution Timestamp.
            self.previous_parent_orders_timestamp = child_order.creation_timestamp
            # Update the parent order with child's order ccxt_id.
            child_order_ccxt_id = self._get_ccxt_id_from_child_order(child_order)
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
        self._update_stats_for_order(
            parent_order,
            f"child_order.id_added_to_parent_order.{iter_num}",
            self.market_data.get_wall_clock_time(),
        )
        return child_order

    async def _submit_twap_child_orders(
        self,
        parent_orders_tmp: List[oordorde.Order],
        parent_order_ids_to_child_order_shares: Dict[int, float],
        execution_freq: pd.Timedelta,
        passivity_factor: float,
        iter_num: int,
    ) -> List[oordorde.Order]:
        """
        Given a set of parent orders, create and submit TWAP child orders.

        The submission of the child orders happens in parallel.
        """
        # Get market data for the latest available 10 seconds.
        # Log both the start of data loading and end.
        get_bid_ask_start_timestamp = self.market_data.get_wall_clock_time()
        bid_ask_data = self.get_bid_ask_data_for_last_period("10S")
        get_bid_ask_end_timestamp = self.market_data.get_wall_clock_time()
        _LOG.debug(hpandas.df_to_str(bid_ask_data))
        # Get all the open positions to determine `curr_num_shares`.
        open_positions = self.get_open_positions()
        get_open_positions_timestamp = self.market_data.get_wall_clock_time()
        _LOG.debug(hprint.to_str("open_positions"))
        #
        coroutines = []
        for order in parent_orders_tmp:
            self._update_stats_for_order(
                order,
                f"bid_ask_market_data.start.{iter_num}",
                get_bid_ask_start_timestamp,
            )
            self._update_stats_for_order(
                order,
                f"bid_ask_market_data.done.{iter_num}",
                get_bid_ask_end_timestamp,
            )
            self._update_stats_for_order(
                order,
                f"get_open_positions.done.{iter_num}",
                get_open_positions_timestamp,
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
                iter_num,
            )
            self._update_stats_for_order(
                order,
                f"order_coroutines_created.{iter_num}",
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
        # Remove `None` values from the output.
        # A child order can be `None` in cases when the system
        # attempts to construct an empty order, which is forbidden
        # by the Order object constructor.
        child_orders = [
            child_order for child_order in child_orders if child_order is not None
        ]
        return child_orders

    async def _get_ccxt_order_structure(
        self, order: oordorde.Order
    ) -> Optional[obcaccbr.CcxtData]:
        """
        Get the CCXT order structure corresponding to the submitted order.
        """
        asset_id = order.asset_id
        symbol = self.asset_id_to_ccxt_symbol_mapping[asset_id]
        #
        ccxt_id = self._get_ccxt_id_from_child_order(order)
        if ccxt_id == -1:
            _LOG.debug("Order=%s has no CCXT ID.", str(order))
            ccxt_order = None
        else:
            # Get the order status by its CCXT ID.
            # ccxt_order = hasynci.run(self._exchange.fetch_order(
            #     id=str(ccxt_id), symbol=symbol), asyncio.get_event_loop(), close_event_loop=False)
            _LOG.debug(str(order))
            ccxt_order = await self._async_exchange.fetch_order(
                id=str(ccxt_id), symbol=symbol
            )
        return ccxt_order
