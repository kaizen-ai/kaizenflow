"""
Import as:

import defi.dao_cross.order_matching as ddcrorma
"""

import copy
import heapq
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import defi.dao_cross.order as ddacrord
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


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
def match_orders(
    orders: List[ddacrord.Order],
    clearing_price: float,
) -> pd.DataFrame:
    """
    Implement orders matching given a clearing price.

    All orders are assumed to be compatible.

    :param orders: orders to match
    :param clearing_price: clearing price
    :return: transfers implemented to match orders
    """
    _LOG.debug(hprint.to_str("orders"))
    _LOG.debug(hprint.to_str("clearing_price"))
    hdbg.dassert_lt(0, len(orders))
    hdbg.dassert_container_type(orders, list, ddacrord.Order)
    hdbg.dassert_lt(0, clearing_price)
    # Build buy and sell heaps.
    buy_heap = []
    sell_heap = []
    # TODO(Dan): Get all the base and quote tokens from all the orders and make
    # sure that there are only two of them.
    # TODO(Dan): Think of more asserts to check if orders are compatible.
    # Push orders to the heaps based on the action type and filtered by limit price.
    for order in orders:
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
    # Pick first in priority buy and sell orders for matching.
    # Make a copy so that the func does not alter state (and is idempotent).
    buy_order = copy.copy(buy_heap.pop())
    sell_order = copy.copy(sell_heap.pop())
    # Successively compare `buy_heap` top with `sell_heap` top, matching
    # quantity until zero or queues empty.
    while (buy_heap or buy_order.is_active) and (
        sell_heap or sell_order.is_active
    ):
        # If an order is not active anymore, pop the next order
        # from the corresponding heaps for matching.
        if not buy_order.is_active:
            buy_order = copy.copy(buy_heap.pop())
        if not sell_order.is_active:
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
