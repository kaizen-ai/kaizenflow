"""
Import as:

import defi.dao_cross.supply_demand as ddcrsede
"""

import copy
import heapq
import logging
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

import defi.dao_cross.order as ddacrord
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


def get_curve_orders(
    type_: str,
    quantities: List[int],
    limit_prices: List[int],
    *,
    quantity_scale: float = 1.0,
    quantity_const: float = 0.0,
    limit_price_scale: float = 1.0,
    timestamp: Optional[pd.Timestamp] = None,
    base_token: str = "BTC",
    quote_token: str = "ETH",
    deposit_address: int = 1,
    wallet_address: int = 1,
):
    """
    Get a list of orders that represent supply or demand.
    """
    orders = []
    for quantity, limit_price in zip(quantities, limit_prices):
        # Adjust values by the passed params.
        quantity = quantity * quantity_scale + quantity_const
        limit_price = limit_price * limit_price_scale
        # Get order action based on the curve type.
        if type_ == "supply":
            action = "sell"
        elif type_ == "demand":
            action = "buy"
        else:
            raise ValueError("Invalid type_='%s'" % type_)
        #
        order = ddacrord.Order(
            timestamp,
            action,
            quantity,
            base_token,
            limit_price,
            quote_token,
            deposit_address,
            wallet_address,
        )
        orders.append(order)
    return orders


def get_curve(
    orders: List[ddacrord.Order], type_: str
) -> List[Tuple[int, int]]:
    """
    Build a supply / demand curve using the given orders.
    """
    hdbg.dassert_in(type_, ["demand", "supply"])
    # Extract quantity and price from the passed orders in order to filter them.
    orders_info = [
        (order.quantity, order.limit_price,) for order in orders
    ]
    # Sort orders by limit price with respect to the curve type.
    # Supply curve is monotonically increasing, so sort orders in ascending order.
    # Demand curve is monotonically decreasing, so sort orders in descending order.
    reverse = type_ == "demand"
    orders_info = sorted(orders_info, key=lambda x: x[1], reverse=reverse)
    # Initiate the list with the first coordintate.
    curve_points = []
    # Set amount of quantity that has entered the market before the contemplated order.
    quantity_before = 0
    for order_info in orders_info:
        quantity = order_info[0] + quantity_before
        price = order_info[1]
        #
        curve_point1 = (quantity_before, price)
        curve_points.append(curve_point1)
        curve_point2 = (quantity, price)
        curve_points.append(curve_point2)
        #
        quantity_before = quantity_before + order_info[0]
    return curve_points
