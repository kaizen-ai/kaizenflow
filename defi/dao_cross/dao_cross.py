"""
Import as:

import defi.dao_cross.dao_cross as ddcrdacr
"""

import copy
import heapq
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import defi.dao_cross.order as ddacrord
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)

# TODO(gp): I'd call this file order_matching.py since this should be used for
#  both dao_cross and dao_swap.

def _get_transfer_df(transfers: Optional[List[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Get a table of all the token transfers.

    :param transfers: list of transfers, where each transfer is a dict with the
        following format:
        ```
        {
            "token": name of a token to transfer,
            "amount": transferred quantity,
            "from": wallet address to send transfer from,
            "to": deposit to send transfer to,
        }
        ```
    :return: table of transfers as df
    """
    if transfers:
        transfer_df = pd.DataFrame(transfers)
    else:
        transfer_df = pd.DataFrame(columns=["token", "amount", "from", "to"])
    return transfer_df


# TODO(gp): Extend to support swaps between A vs B and B vs A using the
#  order equivalence. We need to specify what tokens the clearing price refers to
#  (e.g., clearing_price, base_token, quote_token).
# TODO(gp): Ensure that all orders are compatible, i.e., involve the same tokens
#  A and B.
def match_orders(
    orders: List[ddacrord.Order],
    clearing_price: float,
) -> pd.DataFrame:
    """
    Implement orders matching given a clearing price.

    All orders are assumed to be

    :param orders: orders to match
    :param clearing_price: clearing price
    :return: transfers implemented to match orders
    """
    # Build buy and sell heaps.
    buy_heap = []
    sell_heap = []
    # TODO(Dan): Think of more asserts to check if orders are compatible.
    #  Get all the base and quote tokens from all the orders and make sure that
    #  there are only two of them.
    # tokens = []
    # for order in orders:
    #   if not tokens:
    #       tokens = set([order.base_token, order.quote_token])
    #   else:
    #       tmp_tokens = set([order.base_token, order.quote_token])
    #       if tmp_tokens != tokens:
    #           raise
    # Push orders to the heaps based on the action type and filtered by limit price.
    for order in orders:
        hdbg.dassert_type_is(order, ddacrord.Order)
        hdbg.dassert_lte(0, order.quantity)
        hdbg.dassert_type_is(order.timestamp, pd.Timestamp)
        if order.action == "buy":
            if order.limit_price >= clearing_price:
                heapq.heappush(buy_heap, order)
            else:
                _LOG.debug("Order not eligible for matching due to limit")
        elif order.action == "sell":
            if order.limit_price <= clearing_price:
                heapq.heappush(sell_heap, order)
            else:
                _LOG.debug("Order not eligible for matching due to limit")
        else:
            raise ValueError("Invalid action='%s'" % order.action)
    _LOG.debug("buy_heap=%s", buy_heap)
    _LOG.debug("sell_heap=%s", sell_heap)
    # Set a list to store transfers that perform matching.
    transfers = []
    # Successively compare `buy_heap` top with `sell_heap` top, matching
    # quantity until zero or queues empty.
    buy_order = None
    sell_order = None
    while (buy_heap or ddacrord.is_active_order(buy_order)) and (
        sell_heap or ddacrord.is_active_order(sell_order)
    ):
        # Pop 1 buy and 1 sell orders from the heaps for matching.
        if not buy_order or buy_order.quantity == 0:
            # Make a copy so that `match_orders()` does not alter state (and is
            # idempotent).
            buy_order = copy.copy(buy_heap.pop())
        if not sell_order or sell_order.quantity == 0:
            sell_order = copy.copy(sell_heap.pop())
        # Transfer quantity is equal to the min quantity among the matching
        # buy and sell orders.
        quantity = min(buy_order.quantity, sell_order.quantity)
        # Get base token transfer dict and add it to the transfers list.
        base_transfer = {
            "token": buy_order.base_token,
            "amount": quantity,
            "from": sell_order.wallet_address,
            "to": buy_order.deposit_address,
        }
        transfers.append(base_transfer)
        # Get quote token transfer dict and add it to the transfers list.
        quote_transfer = {
            "token": buy_order.quote_token,
            "amount": quantity * clearing_price,
            "from": buy_order.wallet_address,
            "to": sell_order.deposit_address,
        }
        transfers.append(quote_transfer)
        # Change orders quantities with respect to the implemented transfers.
        buy_order.quantity -= quantity
        sell_order.quantity -= quantity
    # Get DataFrame with the transfers implemented to match the passed orders.
    transfer_df = _get_transfer_df(transfers)
    return transfer_df
