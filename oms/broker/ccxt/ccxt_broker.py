"""
Import as:

import oms.broker.ccxt.ccxt_broker as obccccbr
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import nest_asyncio
import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.htimer as htimer
import oms.broker.ccxt.abstract_ccxt_broker as obcaccbr
import oms.order.order as oordorde

_LOG = logging.getLogger(__name__)

# Added because of "RuntimeError: This event loop is already running"
# https://stackoverflow.com/questions/46827007/runtimeerror-this-event-loop-is-already-running-in-python
# TODO(gp): Investigate if it's a limitation of `asyncio` or a "design error" on our
# side.
nest_asyncio.apply()

# TODO(Grisha): propagate via SystemConfig.
# Minimum number of seconds required for wave completion.
_WAVE_COMPLETION_TIME_THRESHOLD = 4

# #############################################################################
# CcxtBroker
# #############################################################################


# TODO(Juraj): Effectively this is a BinanceCcxtBroker
# since it is so heavily overfitted.
class CcxtBroker(obcaccbr.AbstractCcxtBroker):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # TODO(Juraj): Rewrite using polymorphism ASAP.
        if self._exchange_id == "binance":
            leverage = 1
            self._set_leverage_for_all_symbols(leverage)

    async def get_ccxt_fills(
        self, orders: List[oordorde.Order]
    ) -> List[Dict[str, Any]]:
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
        _LOG.info(
            "CCXT fills loaded timestamp=%s",
            self.market_data.get_wall_clock_time(),
        )
        return ccxt_order_structures

    async def _submit_twap_orders(
        self,
        parent_orders: List[oordorde.Order],
        *,
        execution_freq: Optional[str] = "1T",
    ) -> Tuple[str, List[oordorde.Order]]:
        """
        Execute orders using the TWAP strategy.

        All parent orders are assumed to spawn the same interval of time.

        Each parent order is broken up into smaller child limit orders which are
        submitted between `parent_order_start_time` and `parent_order_end_time` at
        the provided `execution_freq`, e.g. '1T' for 1 min.
        If a limit order is not filled by the provided timestamp, the order is
        cancelled.

        Invariants for submitting TWAP orders:
        - Cancelling orders from the most recent wave needs to happen at the
          beginning of the new wave (otherwise the TWAP orders do not have time to
          get filled)
        - Obtaining and logging fills and trades from the most recent wave must
          happen strictly after cancellation from the previous wave otherwise
          incorrect data can be logged
        - Obtaining bid/ask data for the current wave must happen as close to
          actual order submission as possible, in order to avoid using stale data

        :param parent_orders: parent orders for TWAP execution
            - Example of List[Order] input represented as a dataframe:
              ```
              order_id   creation_timestamp    asset_id       type_             start_timestamp               end_timestamp  curr_num_shares  diff_num_shares  tz
              0  2023-01-18 15:46:00.011061  3065029174  price@twap  2023-01-18 15:47:00.011061  2023-01-18 15:48:00.011061              0.0           2500.0 NaN
              1  2023-01-18 15:46:31.307954  3065029175  price@twap  2023-01-18 15:47:00.011061  2023-01-18 15:48:00.011061              0.0           2500.0 NaN
              2  2023-01-18 15:46:41.889681  3065029176  price@twap  2023-01-18 15:47:00.011061  2023-01-18 15:48:00.011061              0.0           2500.0 NaN
              ```
        :param execution_freq: how often to submit child orders for each parent
        :return: a receipt and a list of sent Orders
        """
        _LOG.info(hprint.to_str("parent_orders"))
        # Resetting these attributes is needed for interaction with components
        # that work with concept of "bars". Calling submit_orders on an empty list
        # means that for the particular "bar" there were no parent orders.
        if not parent_orders:
            self.previous_parent_orders_timestamp = None
            self._previous_parent_orders = []
            return "", []
        # Log OMS parent orders before the submission of the first wave.
        self._logger.log_oms_parent_orders(
            self._get_wall_clock_time, parent_orders
        )
        _LOG.info(hprint.to_str("execution_freq"))
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
        parent_orders_ccxt_symbols: List[str] = []
        for parent_order in parent_orders:
            ccxt_symbol = self.asset_id_to_ccxt_symbol_mapping[
                parent_order.asset_id
            ]
            # Save the `ccxt_symbol` in the parent order.
            parent_order.extra_params["ccxt_symbol"] = ccxt_symbol
            parent_orders_ccxt_symbols.append(ccxt_symbol)
        # Verify that there are no empty parent orders.
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
        await self._align_with_parent_order_start_timestamp(
            execution_start_timestamp
        )
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
        num_waves = self._calculate_num_twap_child_order_waves(
            execution_start_timestamp, execution_end_timestamp, execution_freq
        )
        if hasattr(
            self._limit_price_computer, "_volatility_multiple"
        ) and isinstance(self._limit_price_computer._volatility_multiple, list):
            hdbg.dassert_lte(
                num_waves, len(self._limit_price_computer._volatility_multiple)
            )
        _LOG.info(hprint.to_str("num_waves"))
        quantity_computer = self._child_order_quantity_computer
        # Pass parameters to child quantity computer.
        quantity_computer.set_instance_params(
            parent_orders_copy, num_waves, self.market_info
        )
        child_orders = []
        # Track the previous wave ID to determine if more than one wave has
        # been skipped. Assumption is wave ID should start from 0.
        prev_wave_id = -1
        is_first_wave = True
        while True:
            # Calculate the wave ID to be used at the current time for each
            # order submission wave.
            wave_id = self._calculate_wave_id(
                num_waves, execution_end_timestamp, execution_freq
            )
            # TODO(Sameep): Log the skipped waves so that we can reconstruct
            # the missing waves in notebooks.
            # Check if any waves were skipped.
            if wave_id != prev_wave_id + 1:
                skipped_num_waves = wave_id - (prev_wave_id + 1)
                _LOG.warning(
                    "%s waves were skipped. Expected wave_id=%s, Current wave_id=%s",
                    skipped_num_waves,
                    prev_wave_id + 1,
                    wave_id,
                )
            prev_wave_id = wave_id
            wave_start_time = self._get_wall_clock_time()
            _LOG.info(hprint.to_str("wave_id wave_start_time"))
            # Get all the open positions to determine `curr_num_shares`.
            open_positions = self.get_open_positions()
            _LOG.info(hprint.to_str("open_positions"))
            get_open_positions_timestamp = self.market_data.get_wall_clock_time()
            # TODO(Danya): Factor out into a helper.
            for order in parent_orders_copy:
                self._update_stats_for_order(
                    order,
                    f"get_open_positions.done.{wave_id}",
                    get_open_positions_timestamp,
                )
            #
            quantity_computer.update_current_positions(open_positions)
            # Generate the quantities for the wave about to be submitted.
            parent_order_ids_to_child_order_shares = (
                quantity_computer.get_wave_quantities(is_first_wave)
            )
            # Since the wave ID is determined adaptively, the first wave may
            # not have a wave_id of 0. Therefore, we use a flag to identify
            # if the current wave is the first one and toggle the flag
            # accordingly.
            is_first_wave = False
            is_last_wave_duration = self.market_data.get_wall_clock_time() >= (
                execution_end_timestamp - pd.Timedelta(execution_freq)
            )
            if is_last_wave_duration:
                _LOG.warning(
                    "Exiting wave loop %s before bar end timestamp of %s to have enough time for all parent orders fills fetching and logging",
                    execution_freq,
                    execution_end_timestamp,
                )
                break
            if await self._skip_wave(
                wave_start_time,
                execution_freq,
                wave_id,
            ):
                # Skip the wave if the time left to complete the wave is less
                # than the threshold.
                continue
            child_orders_iter = await self._submit_twap_child_orders(
                parent_orders_copy,
                parent_order_ids_to_child_order_shares,
                execution_freq,
                wave_id,
            )
            # Log parent orders state right after the submission to keep track
            # of the bound child order CCXT IDs.
            self._logger.log_oms_parent_orders(
                self._get_wall_clock_time, parent_orders_copy
            )
            child_orders.extend(child_orders_iter)
            await self._cancel_orders_and_sync_with_next_wave_start(
                parent_orders_ccxt_symbols,
                execution_freq,
                wave_start_time,
            )
            # Log time of alignment with the next wave.
            next_wave_sync_timestamp = self.market_data.get_wall_clock_time()
            for child_order in child_orders_iter:
                self._update_stats_for_order(
                    child_order,
                    f"aligned_with_next_wave.end",
                    next_wave_sync_timestamp,
                )
            # Log CCXT fills and trades.
            if self._log_dir is not None:
                await self._log_last_wave_results(child_orders_iter)
            # Break if the time is up.
            current_wall_clock_time = self.market_data.get_wall_clock_time()
            _LOG.info(
                hprint.to_str("current_wall_clock_time execution_end_timestamp")
            )
            if current_wall_clock_time >= execution_end_timestamp:
                _LOG.info(
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
        if _LOG.isEnabledFor(logging.DEBUG):
            # The `oms.Order` object is enormous for long trading periods so
            # displaying it at the debug level.
            _LOG.debug(hprint.to_str("self._previous_parent_orders"))
        # TODO(Danya): Factor out the loading and logging of oms.Fills
        #  into a separate method.
        oms_fills = await self.get_fills_async()
        _LOG.info(
            "get_fills_async() is done, current time is %s",
            self.market_data.get_wall_clock_time(),
        )
        self._logger.log_oms_fills(self._get_wall_clock_time, oms_fills)
        _LOG.info(
            "log_oms_fills() is done, current time is %s",
            self.market_data.get_wall_clock_time(),
        )
        # The receipt is not really needed since the order is accepted right away,
        # and we don't need to wait for the order being accepted.
        submitted_order_id = self._get_next_submitted_order_id()
        receipt = f"order_{submitted_order_id}"
        return receipt, child_orders

    # @staticmethod
    # async def custom_gather(coroutines: List[Coroutine]) -> Any:
    #     """
    #     Execute multiple coroutines.
    #     """
    #     values = await asyncio.gather(*coroutines)
    #     return values

    # TODO(Sameep): disabled as part of CMTask5310. Maybe move to helpers.hdbg.
    # @staticmethod
    # def _is_all_attributes_in_list_the_same(
    #     list_: List[Any], attribute_name: str, message: str = None
    # ) -> Optional[bool]:
    #     """
    #     Check if all the elements in the list have the same attribute value.

    #     :param list_: list of objects
    #     :param attribute_name: name of the attribute to check
    #     :param message: message to raise if the check fails
    #     :return: True if all the elements in the list have the same attribute
    #     """
    #     unique_attribures = set(
    #         getattr(element, attribute_name) for element in list_
    #     )
    #     result = len(unique_attribures) == 1
    #     if message is not None:
    #         hdbg.dassert(result, message)
    #     else:
    #         return result

    async def _cancel_orders_and_sync_with_next_wave_start(
        self,
        parent_orders_ccxt_symbols: List[str],
        execution_freq: pd.Timedelta,
        wave_start_time: pd.Timestamp,
    ) -> None:
        """
        Wait until the end of current wave, cancel orders and sync to next wave
        start.

        The flow:
        - Wait right until the end of the current wave minus a small delta.
        - Cancel all orders from the most recent wave.
        - Double check that we are in sync to start the next wave.

        :param parent_orders_ccxt_symbols: symbols to cancel orders for
        :param execution_freq: wave execution frequency as pd.Timedelta
        :param wave_start_time: start time of the wave to cancel orders for
        """
        # This approach is safer than wave_start_time.ceil(execution_freq).
        # In case ceil was applied on a precisely rounded start timestamp
        # such as 12:00:00.00 it would simply return the same value.
        wait_until = (wave_start_time + pd.Timedelta(execution_freq)).floor(
            execution_freq
        )
        # In order to avoid occasional closing of an order "too late" (
        # after the end of the bar), we add a delay to complete the order
        # cancellation before the bar ends. See CmTask5129.
        # Note that the length of the delay increases with the distance
        # of the trading server from the exchange's servers.
        # TODO(Grisha): pass values via SystemConfig.
        # TODO(Grisha): should this be a function of `execution_frequency`?
        # Currently we assume `10S` execution window.
        early_cancel_delay = 0.2
        wait_until_modified = wait_until - pd.Timedelta(
            seconds=early_cancel_delay
        )
        await hasynci.async_wait_until(
            wait_until_modified,
            self._get_wall_clock_time,
        )
        if self._exchange_id == "cryptocom":
            self._cancel_all_open_orders_with_exception()
        else:
            await self.cancel_open_orders_for_symbols(parent_orders_ccxt_symbols)
        _LOG.info(
            "Orders cancelled timestamp=%s",
            self.market_data.get_wall_clock_time(),
        )
        # Wait again in case the order cancellation was faster than expected
        # and we are still in the same wave time-wise.
        await hasynci.async_wait_until(
            wait_until,
            self._get_wall_clock_time,
        )
        _LOG.info(
            "After syncing with next child wave=%s",
            self.market_data.get_wall_clock_time(),
        )

    async def _log_last_wave_results(
        self, child_orders: List[oordorde.Order]
    ) -> None:
        """
        Obtain fills and trades info from the previous wave and log it.
        """
        ccxt_fills = await self.get_ccxt_fills(child_orders)
        ccxt_trades = await self.get_ccxt_trades(ccxt_fills)
        self._logger.log_ccxt_fills(
            self._get_wall_clock_time,
            ccxt_fills,
        )
        self._logger.log_ccxt_trades(
            self._get_wall_clock_time,
            ccxt_trades,
        )
        _LOG.info(
            "CCXT fills and trades logging finished=%s",
            self.market_data.get_wall_clock_time(),
        )

    def _add_update_time_to_ccxt_response(
        self, child_order_ccxt_response: Dict[str, any]
    ) -> Dict[str, Any]:
        """
        Add "update_time" equal to current wall clock time.

        This action is necessary for child order responses from
        crypto.com. Unlike Binance, which automatically sends an
        "updateTime" field, crypto.com does not provide any timestamp
        information in the receipt of the order submission. This field
        is used in the CCXT logger. As an improvised solution we set an
        "update_time" field equal to current timestamp (slightly less
        accurate then the Binance behavior) but good enough. Technically
        we could choose any name for the field, e.g match the Binance
        interface "updateTime", however, crypto.com uses "update_time"
        elsewhere. For this reason we stick to the same field name to
        avoid confusion.

        :param child_order_ccxt_response: response dict to update
        :return: child order CCXT response with an "update_time" field
        """
        child_order_ccxt_response["info"][
            "update_time"
        ] = self.market_data.get_wall_clock_time()
        return child_order_ccxt_response

    async def _submit_twap_child_order(
        self,
        parent_order_ids_to_child_order_shares: Dict[int, float],
        open_positions: Dict[str, float],
        execution_freq: pd.Timedelta,
        bid_ask_data: pd.DataFrame,
        parent_order: oordorde.Order,
        wave_id: int,
    ) -> Optional[oordorde.Order]:
        # Get the total shares we want to achieve for the parent order during
        # the entire TWAP order.
        self._update_stats_for_order(
            parent_order,
            f"start.{wave_id}",
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
        execution_end_timestamp = (creation_timestamp + execution_freq).floor(
            execution_freq
        )
        # Get open positions.
        # If no open position is found, Binance doesn't return as a key,
        # so we count that as 0.
        curr_num_shares = open_positions.get(currency_pair, 0)
        _LOG.info(hprint.to_str("currency_pair curr_num_shares"))
        type_ = "limit"
        _LOG.info(
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
        self._update_stats_for_order(
            child_order,
            f"wave_id",
            wave_id,
        )
        # Add the order_id of the parent OMS order.
        # Each child order should have only one corresponding parent order ID.
        parent_order_id_key = "oms_parent_order_id"
        hdbg.dassert_not_in(parent_order_id_key, child_order.extra_params)
        child_order.extra_params[parent_order_id_key] = parent_order.order_id
        # Transfer parent order timing logs related to the child order.
        # These include the getting the bid/ask data and open positions.
        _LOG.info(parent_order.extra_params["stats"])
        self._update_stats_for_order(
            child_order,
            f"get_open_positions.done",
            parent_order.extra_params["stats"][
                f"_submit_twap_orders::get_open_positions.done.{wave_id}"
            ],
        )
        self._update_stats_for_order(
            child_order,
            f"bid_ask_market_data.start",
            parent_order.extra_params["stats"][
                f"_submit_twap_child_orders::bid_ask_market_data.start.{wave_id}"
            ],
        )
        self._update_stats_for_order(
            child_order,
            f"bid_ask_market_data.done",
            parent_order.extra_params["stats"][
                f"_submit_twap_child_orders::bid_ask_market_data.done.{wave_id}"
            ],
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.created",
            self.market_data.get_wall_clock_time(),
        )
        # Calculate limit price.
        side = "buy" if child_order_diff_signed_num_shares > 0 else "sell"
        # Filter data on asset_id.
        bid_ask_data = bid_ask_data[bid_ask_data["asset_id"] == asset_id]
        price_dict = self._get_limit_price_dict(
            bid_ask_data,
            side,
            self.market_info[asset_id]["price_precision"],
            execution_freq,
            wave_id=wave_id,
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.limit_price_calculated",
            self.market_data.get_wall_clock_time(),
        )
        _LOG.info(hprint.to_str("price_dict"))
        self._update_stats_for_order(
            child_order,
            f"child_order.submission_started",
            self.market_data.get_wall_clock_time(),
        )
        (
            child_order,
            ccxt_child_order_response,
        ) = await self._submit_single_order_to_ccxt(
            child_order,
            order_type=type_,
            limit_price=price_dict["limit_price"],
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.submitted",
            self.market_data.get_wall_clock_time(),
        )
        # Append basic order response if response received from exchange is empty.
        if not ccxt_child_order_response:
            ccxt_child_order_response[
                "symbol"
            ] = self.asset_id_to_ccxt_symbol_mapping[child_order.asset_id]
            ccxt_child_order_response["empty"] = True
            ccxt_child_order_response["id"] = -1
        # The second part of the if technically does not need to be there,
        # we keep it as a safety check.
        if (
            self._exchange_id == "cryptocom"
            and not ccxt_child_order_response.get("updateTime")
        ):
            ccxt_child_order_response = self._add_update_time_to_ccxt_response(
                ccxt_child_order_response
            )
        # Here log CCXT trades and CCXT fills.
        # Log submitted child order with extra price info.
        self._logger.log_child_order(
            self._get_wall_clock_time,
            child_order,
            ccxt_child_order_response,
            price_dict,
        )
        self._update_stats_for_order(
            child_order,
            f"child_order.logged",
            self.market_data.get_wall_clock_time(),
        )
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
            _LOG.info(hprint.to_str("is_submitted_order"))
        else:
            _LOG.warning(
                "Order is not submitted to CCXT, %s",
                str(child_order),
            )
        self._update_stats_for_order(
            parent_order,
            f"child_order.id_added_to_parent_order.{wave_id}",
            self.market_data.get_wall_clock_time(),
        )
        return child_order

    async def _submit_twap_child_orders(
        self,
        parent_orders_tmp: List[oordorde.Order],
        parent_order_ids_to_child_order_shares: Dict[int, float],
        execution_freq: pd.Timedelta,
        wave_id: int,
    ) -> List[oordorde.Order]:
        """
        Given a set of parent orders, create and submit TWAP child orders.

        The submission of the child orders happens in parallel.
        """
        # Log both the start of data loading and end.
        get_bid_ask_start_timestamp = self.market_data.get_wall_clock_time()
        bid_ask_data = self.get_bid_ask_data_for_last_period()
        get_bid_ask_end_timestamp = self.market_data.get_wall_clock_time()
        _LOG.info(hpandas.df_to_str(bid_ask_data, num_rows=None))
        #
        coroutines = []
        for order in parent_orders_tmp:
            self._update_stats_for_order(
                order,
                f"bid_ask_market_data.start.{wave_id}",
                get_bid_ask_start_timestamp,
            )
            self._update_stats_for_order(
                order,
                f"bid_ask_market_data.done.{wave_id}",
                get_bid_ask_end_timestamp,
            )
            # We don't need an `await` because we are just creating a coroutine
            # that we will execute later.
            coroutine = self._submit_twap_child_order(
                parent_order_ids_to_child_order_shares,
                self._cached_open_positions,
                execution_freq,
                bid_ask_data,
                order,
                wave_id,
            )
            self._update_stats_for_order(
                order,
                f"order_coroutines_created.{wave_id}",
                self.market_data.get_wall_clock_time(),
            )
            coroutines.append(coroutine)
        # Submit all orders concurrently.
        with htimer.TimedScope(
            logging.DEBUG, "asyncio_order_submission_and_wait_time"
        ) as ts:
            # order_submissions= await custom_gather(coroutines)
            child_orders = await asyncio.gather(*coroutines)
            _LOG.info(
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
            _LOG.info("Order=%s has no CCXT ID.", str(order))
            ccxt_order = None
        else:
            # Get the order status by its CCXT ID.
            # ccxt_order = hasynci.run(self._exchange.fetch_order(
            #     id=str(ccxt_id), symbol=symbol), asyncio.get_event_loop(), close_event_loop=False)
            _LOG.info(str(order))
            ccxt_order = await self._async_exchange.fetch_order(
                id=str(ccxt_id), symbol=symbol
            )
        return ccxt_order

    async def _skip_wave(
        self,
        wave_start_time: pd.Timestamp,
        execution_freq: pd.Timedelta,
        wave_id: int,
    ) -> bool:
        """
        Skip the wave if the time left to complete the wave is less than the
        threshold.

        :param wave_start_time: The start time of the wave.
        :param execution_freq: The frequency of execution.
        :param wave_id: The ID of the wave.
        :return: True if the wave should be skipped, False otherwise.
        """
        # Calculate the end timestamp of the wave.
        wave_end_time = (wave_start_time + execution_freq).floor(execution_freq)
        # Check if the time left to complete the wave is less than the
        # threshold.
        if (
            wave_end_time - self.market_data.get_wall_clock_time()
        ).total_seconds() < _WAVE_COMPLETION_TIME_THRESHOLD:
            _LOG.info(
                hprint.to_str(
                    "wave_start_time wave_end_time wave_id _WAVE_COMPLETION_TIME_THRESHOLD"
                )
            )
            _LOG.warning(
                "Skipping wave %s, time left is less than threshold.", wave_id
            )
            await hasynci.async_wait_until(
                wave_end_time,
                self._get_wall_clock_time,
            )
            _LOG.info(
                "After syncing with next child wave=%s",
                self.market_data.get_wall_clock_time(),
            )
            return True
        return False

    # TODO(Sameep): Add explanation of the approach in the docs.
    def _calculate_wave_id(
        self,
        total_num_waves: int,
        execution_end_timestamp: pd.Timestamp,
        execution_freq: pd.Timedelta,
    ) -> int:
        """
        Calculate the current wave ID using current time, execution end
        timestamp and the execution frequency.

        Example:
        total_num_waves = 720
        execution_end_timestamp = pd.Timestamp("5:00:00")
        execution_freq = pd.Timedelta(seconds=10)
        current_timestamp = pd.Timestamp("3:00:25")

        remaining_time = (execution_end_timestamp - current_timestamp.floor(execution_freq))
                       = 5:00:00 - 3:00:20 = 1:59:40
        remaining_num_waves = 1:59:40 / 10s = 718
        wave_id = 720 - 718 = 2

        :param num_waves: The number of waves to be executed.
        :param execution_end_timestamp: The timestamp when the execution of the
            parent order ends.
        :param execution_freq: The frequency of execution.
        :returns: wave id to be utilized at the current time
        """
        remaining_time = (
            execution_end_timestamp
            - self.market_data.get_wall_clock_time().floor(execution_freq)
        )
        remaining_num_waves = remaining_time // execution_freq
        wave_id = total_num_waves - remaining_num_waves
        hdbg.dassert_lte(0, wave_id)
        return wave_id


class CcxtCryptocomBroker(CcxtBroker):
    def _get_market_info(self) -> Dict[int, Any]:
        """
        Load market information from the given exchange and map to asset ids.

        The following data is saved:
        - minimal order limits (notional and quantity)
        - asset quantity precision (for rounding of orders)

        In contract with Binance, we do not load leverage information here.
        The related methods are not supported by CCXT as version 4.2.13.
        ```
        `>>> ccxtpro.cryptocom().has["fetchLeverageTiers"]
        False
        ```
        The numbers are determined by loading the market metadata from CCXT.
        """
        minimal_order_limits: Dict[int, Any] = {}
        # Load market information from CCXT.
        exchange_markets = self._sync_exchange.load_markets()
        self._logger.log_exchange_markets(
            self._get_wall_clock_time, exchange_markets, None
        )
        for asset_id, symbol in self.asset_id_to_ccxt_symbol_mapping.items():
            minimal_order_limits[asset_id] = {}
            currency_market = exchange_markets[symbol]
            # Example swap (perpetual future):
            # {
            #    "id": "BTCUSD-PERP",
            #    "lowercaseId": null,
            #    "symbol": "BTC/USD:USD",
            #    "base": "BTC",
            #    "quote": "USD",
            #    "settle": "USD",
            #    "baseId": "BTC",
            #    "quoteId": "USD",
            #    "settleId": "USD",
            #    "type": "swap",
            #    "spot": false,
            #    "margin": false,
            #    "swap": true,
            #    "future": false,
            #    "option": false,
            #    "index": null,
            #    "active": true,
            #    "contract": true,
            #    "linear": true,
            #    "inverse": false,
            #    "subType": "linear",
            #    "taker": 0.004,
            #    "maker": 0.004,
            #    "contractSize": 1,
            #    "expiry": null,
            #    "expiryDatetime": null,
            #    "strike": null,
            #    "optionType": null,
            #    "precision": {
            #        "amount": 0.0001,
            #        "price": 0.1,
            #        "cost": null,
            #        "base": null,s
            #        "quote": null
            #    },
            #    "limits": {
            #        "leverage": {
            #            "min": 1,
            #            "max": 100
            #        },
            #        "amount": {
            #            "min": null,
            #            "max": null
            #        },
            #        "price": {
            #            "min": null,
            #            "max": null
            #        },
            #        "cost": {
            #            "min": null,
            #            "max": null
            #        }
            #    },
            #    "created": null,
            #    "info": {
            #        "symbol": "BTCUSD-PERP",
            #        "inst_type": "PERPETUAL_SWAP",
            #        "display_name": "BTCUSD Perpetual",
            #        "base_ccy": "BTC",
            #        "quote_ccy": "USD",
            #        "quote_decimals": "1",
            #        "quantity_decimals": "4",
            #        "price_tick_size": "0.1",
            #        "qty_tick_size": "0.0001",
            #        "max_leverage": "100",
            #        "tradable": true,
            #        "expiry_timestamp_ms": "0",
            #        "beta_product": false,
            #        "underlying_symbol": "BTCUSD-INDEX",
            #        "contract_size": "1",
            #        "margin_buy_enabled": false,
            #        "margin_sell_enabled": false
            #    },
            #    "percentage": true,
            #    "tiers": {
            #        "maker": [
            #            [
            #                0,
            #                0.004
            #            ],
            #            [
            #                25000,
            #                0.0035
            #            ],
            #            [
            #                50000,
            #                0.0015
            #            ],
            #            [
            #                100000,
            #                0.001
            #            ],
            #            [
            #                250000,
            #                0.0009
            #            ],
            #            [
            #                1000000,
            #                0.0008
            #            ],
            #            [
            #                20000000,
            #                0.0007
            #            ],
            #            [
            #                100000000,
            #                0.0006
            #            ],
            #            [
            #                200000000,
            #                0.0004
            #            ]
            #        ],
            #        "taker": [
            #            [
            #                0,
            #                0.004
            #            ],
            #            [
            #                25000,
            #                0.0035
            #            ],
            #            [
            #                50000,
            #                0.0025
            #            ],
            #            [
            #                100000,
            #                0.0016
            #            ],
            #            [
            #                250000,
            #                0.00015
            #            ],
            #            [
            #                1000000,
            #                0.00014
            #            ],
            #            [
            #                20000000,
            #                0.00013
            #            ],
            #            [
            #                100000000,
            #                0.00012
            #            ],
            #            [
            #                200000000,
            #                0.0001
            #            ]
            #        ]
            #    }
            # }
            limits = currency_market["limits"]
            # Get the minimal amount of asset in the order.
            amount_limit = limits["amount"]["min"]
            # Set the minimal cost of asset in the order.
            # The notional limit can differ between symbols and subject to
            # fluctuations, so it is set manually to 10.
            notional_limit = 10.0
            minimal_order_limits[asset_id]["min_cost"] = notional_limit
            # Set the rounding precision for amount of the asset.
            amount_precision_decimal = currency_market["precision"]["amount"]
            # Crypto.com expresses precision in a decimal form, whereas Binance
            # uses integer notation as "number of decimal places", e.g.:
            # - If the lowest amount of asset you can buy is 0.01
            #   - Binance: amount = 2
            #   - Crypto.com amount = 0.01
            # Our interfaces later during child order quantity calculation expect
            # the integer notation, so convert the format early - here.
            # Convert the precision to string and count the number of decimal places.
            amount_precision = len(str(amount_precision_decimal).split(".")[1])
            minimal_order_limits[asset_id]["amount_precision"] = amount_precision
            # The amount limit is used in `_apply_cc_limits`
            # mathematical comparisons, it cannot be None.
            # The amount precision implies the minimum amount.
            amount_limit = (
                amount_precision_decimal if amount_limit is None else amount_limit
            )
            minimal_order_limits[asset_id]["min_amount"] = amount_limit
            # Set the rounding precision for price of the asset.
            price_precision = currency_market["precision"]["price"]
            # See above comment for amount precision that explains the format
            # conversion.
            price_precision = len(str(price_precision).split(".")[1])
            minimal_order_limits[asset_id]["price_precision"] = price_precision
        return minimal_order_limits

    def _get_open_positions_from_exchange(self):
        """
        Load open positions from the exchange and return as dict.

        Implementation of `get_open_positions`.
        """
        positions = []
        # Fetch all the open positions. The response from the exchange looks like:
        # ```
        # [
        #     {
        #         "info": {
        #             "account_id": "780cbc31-e427-5969-b3e6-635694755089",
        #             "quantity": "0.008",
        #             "cost": "24.7224",
        #             "open_pos_cost": "24.7224",
        #             "open_position_pnl": "-0.00988896",
        #             "session_pnl": "-0.0159588",
        #             "update_timestamp_ms": "1720605170161",
        #             "instrument_name": "ETHUSD-PERP",
        #             "type": "PERPETUAL_SWAP"
        #         },
        #         "id": null,
        #         "symbol": "ETH/USD:USD",
        #         "timestamp": 1720605170161,
        #         "datetime": "2024-07-10T09:52:50.161Z",
        #         "hedged": null,
        #         "side": "buy",
        #         "contracts": "0.008",
        #         "contractSize": 1.0,
        #         "entryPrice": null,
        #         "markPrice": null,
        #         "notional": null,
        #         "leverage": null,
        #         "collateral": 24.7224,
        #         "initialMargin": 24.7224,
        #         "maintenanceMargin": null,
        #         "initialMarginPercentage": null,
        #         "maintenanceMarginPercentage": null,
        #         "unrealizedPnl": -0.00988896,
        #         "liquidationPrice": null,
        #         "marginMode": null,
        #         "percentage": null,
        #         "marginRatio": null,
        #         "stopLossPrice": null,
        #         "takeProfitPrice": null
        #     }
        # ]
        # TODO(Juraj): factor out common code from base method.
        # The method differs only in one dict field name.
        _LOG.info("No cached value for open positions: accessing exchange")
        positions = self._sync_exchange.fetch_positions()
        self._logger.log_positions(self._get_wall_clock_time, positions)
        # Map from symbol to the amount currently owned if different than zero,
        # e.g. `{'BTC/USDT': 0.01}`.
        open_positions: Dict[str, float] = {}
        for position in positions:
            # Get the quantity of assets on short/long positions.
            position_amount = float(position["info"]["quantity"])
            position_symbol = position["symbol"]
            if position_amount != 0:
                open_positions[position_symbol] = position_amount
        return open_positions
