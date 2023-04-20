"""
Import as:

import defi.dao_cross.supply_demand as ddcrsede
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


def _get_curve_orders(
    action: str,
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
    Get orders corresponding to a specified supply / demand curve.
    """
    orders = []
    for quantity, limit_price in zip(quantities, limit_prices):
        #
        quantity = quantity * quantity_scale + quantity_const
        limit_price = limit_price * limit_price_scale
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


def get_supply_orders1(
    *,
    quantity_scale: float = 1.0,
    quantity_const: float = 0.0,
    limit_price_scale: float = 1.0,
) -> List[ddacrord.Order]:
    """
    Get orders corresponding to a monotonically increasing supply curve.
    """
    action = "sell"
    quantities = [40, 40, 30, 30, 20, 20]
    limit_prices = [100, 60, 40, 30, 20, 10]
    orders = _get_curve_orders(
        action,
        quantities,
        limit_prices,
    )
    return orders


def get_demand_orders1(
    *,
    quantity_scale: float = 1.0,
    quantity_const: float = 0.0,
    limit_price_scale: float = 1.0,
) -> List[ddacrord.Order]:
    """
    Get orders corresponding to a monotonically decreasing demand curve.
    """
    action = "buy"
    quantities = [10, 30, 20, 40, 50, 30]
    limit_prices = [110, 100, 80, 60, 40, 30]
    orders = _get_curve_orders(
        action,
        quantities,
        limit_prices,
    )
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
    ascending = type_ == "supply"
    orders_info = sorted(orders_info, key=lambda x: x[1], ascending=ascending)
    # Get the coordinates where curves crosses prices axis.
    first_curve_point = (0, orders_info[0][1])
    # Initiate the list with the first coordintate.
    curve_points = [first_curve_point]
    # Set amount of quantity that has entered the market before the contemplated order.
    quantity_before = 0
    for order_info in orders_info:
        quantity = order_info[0] + quantity_before
        quantity_before = quantity_before + quantity
        price = order_info[1]
        curve_point = (quantity, price,)
        curve_points.append(curve_point)
    return curve_points
