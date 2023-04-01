"""
Import as:

import defi.dao_cross.dao_cross as ddcrdacr
"""

import copy
import heapq
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import defi.dao_cross.order as ddacrord

_LOG = logging.getLogger(__name__)


def match_orders(
    orders: List[ddacrord.Order],
    clearing_price: float,
) -> pd.DataFrame:
    """
    Implement DaoCross orders matching.

    :param orders: orders to match
    :param clearing_price: clearing price
    :return: transfers implemented to match orders
    """
    # Build buy and sell heaps.
    buy_heap = []
    sell_heap = []
    # Push orders to the heaps based on the action type and filtered by limit price.
    for order in orders:
        # TODO(Dan): Think of more asserts to check if orders are compatible.
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
    while (buy_heap or ddacrord.is_active_order(buy_order)) and (sell_heap or ddacrord.is_active_order(sell_order)):
        # Pop 1 buy and 1 sell orders from the heaps for matching.
        if not buy_order or buy_order.quantity == 0:
            # Make a copy so that `match_orders()` does not alter state (and is idempotent).
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
            "to": buy_order.deposit_address
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
    transfer_df = get_transfer_df(transfers)
    return transfer_df


def get_transfer_df(
    transfers: Optional[List[Dict[str, Any]]]
) -> pd.DataFrame:
    """
    Get a table of all the passed transfers.

    Transfer is a dict with the following format:
    {
        "token": name of a token to transfer,
        "amount": transferred quantity,
        "from": wallet address to send transfer from,
        "to": deposit to send transfer to,
    }

    :param transfers: list of transfers
    :return: table of transfers
    """
    if transfers:
        transfer_df = pd.DataFrame(transfers)
    else:
        transfer_df = pd.DataFrame(
            columns=["token", "amount", "from", "to"]
        )
    return transfer_df
