"""
Import as:

import oms.child_order_quantity_computer.dynamic_scheduling_child_order_quantity_computer as ocoqcdscoqc
"""
import logging
from typing import Dict, List

import helpers.hnumpy as hnumpy
import helpers.hprint as hprint
import oms.child_order_quantity_computer.child_order_quantity_computer as ocoqccoqc

_LOG = logging.getLogger(__name__)


class DynamicSchedulingChildOrderQuantityComputer(
    ocoqccoqc.AbstractChildOrderQuantityComputer
):
    """
    Place each child order wave with the remaining amount to fill.

    Given a certain amount like `(BTC, 10)` and num_waves=5, the first
    wave is placed for (BTC, 10); if during the first wave the order has
    been filled for 8 BTC, the second wave is placed for (BTC, 2). If
    during the second wave the order has been filled completely, no new
    orders are placed for the rest of the execution.
    """

    def __init__(self):
        super().__init__()
        self._target_positions = {}

    @staticmethod
    def _apply_share_precision(shares: float, amount_precision: int) -> float:
        """
        Round number of shares to the given precision.

        :param shares: number of shares to round
        :param amount_precision: precision to apply
        :return: rounded number of shares
        """
        shares_before_floor = shares
        shares = hnumpy.floor_with_precision(shares, amount_precision)
        if shares_before_floor != shares:
            _LOG.warning(
                "Share amount changed due to precision limit: "
                + hprint.to_str("shares_before_floor shares amount_precision"),
            )
        return shares

    def _get_wave_quantities(self, is_first_wave: bool) -> Dict[int, float]:
        """
        Return the child order quantity to be placed during the current wave.

        During the first wave, compute the target positions for parent orders
        and get the first child order quantity as equal to the parent order
        quantity.
        For the following waves, the child order quantities are computed as
        `target_position` - `open_position`. This is done for two reasons:
        - Extra API calls required to check the fill rate are expensive, and
        it is cheaper to calculate the fill rate from the open positions
        - We cannot rely only on open positions, since the bar can start with
        an unflattened account, and parent orders do not update information
        on current holdings in live fashion.

        :param is_first_wave: True if the current wave is the first one, False
            otherwise
        """
        next_wave_quantities: Dict[int, float] = {}
        for parent_order in self._parent_orders:
            parent_order_id = parent_order.order_id
            ccxt_symbol = parent_order.extra_params["ccxt_symbol"]
            # Get current open positions for an asset.
            # If no position is held, return 0.
            current_position = self._current_positions.get(ccxt_symbol, 0)
            # Round to the number allowable by the exchange.
            amount_precision = self.parent_order_id_to_amount_precision[
                parent_order_id
            ]
            if is_first_wave:
                # Compute the target positions based on current positions.
                # Use `target_positions` to track parent order fill rates
                # without extra API calls.
                diff_num_shares = parent_order.diff_num_shares
                self._target_positions[parent_order_id] = (
                    current_position + diff_num_shares
                )
                diff_num_shares = self._apply_share_precision(
                    diff_num_shares, amount_precision
                )
                # Assign to the output.
                next_wave_quantities[parent_order_id] = diff_num_shares
            else:
                # Calculate the order size based on current holdings.
                # Given the open positions, the next wave quantity is computed
                # as `target_position - current_position`.
                target_position = self._target_positions[parent_order_id]
                # Calculate the child order size based on missing amount
                # for a full fill.
                child_order_diff_signed_num_shares = (
                    target_position - current_position
                )
                child_order_diff_signed_num_shares = self._apply_share_precision(
                    child_order_diff_signed_num_shares, amount_precision
                )
                # Assign to the output.
                next_wave_quantities[
                    parent_order_id
                ] = child_order_diff_signed_num_shares
        return next_wave_quantities

    def _get_range_filter(self, expected_num_child_orders: int) -> List[int]:
        """
        Get the range of expected number of child orders.
        """
        return list(range(expected_num_child_orders + 1))
