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


def get_supply_orders1() -> List[ddacrord.Order]:
    ts = None
    action = "sell"
    base = "BTC"
    quote = "ETH"
    src = -1
    dst = -1
    orders = [
        ddacrord.Order(
            ts, action, quantity=40, base, limit_price=100, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=40, base, limit_price=60, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=30, base, limit_price=40, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=30, base, limit_price=30, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=20, base, limit_price=20, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=20, base, limit_price=10, quote, src, dst
        ),
    ]
    return orders


def get_supply_orders2() -> List[ddacrord.Order]:
    ts = None
    action = "sell"
    base = "BTC"
    quote = "ETH"
    src = -1
    dst = -1
    orders = [
        ddacrord.Order(
            ts, action, quantity=4, base, limit_price=10, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=4, base, limit_price=6, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=3, base, limit_price=4, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=3, base, limit_price=3, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=2, base, limit_price=2, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=2, base, limit_price=1, quote, src, dst
        ),
    ]
    return orders


def get_demand_orders1() -> List[ddacrord.Order]:
    ts = None
    action = "buy"
    base = "BTC"
    quote = "ETH"
    src = 1
    dst = 1
    orders = [
        ddacrord.Order(
            ts, action, quantity=10, base, limit_price=110, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=30, base, limit_price=100, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=20, base, limit_price=80, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=40, base, limit_price=60, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=50, base, limit_price=40, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=30, base, limit_price=30, quote, src, dst
        ),
    ]
    return orders


def get_demand_orders2() -> List[ddacrord.Order]:
    ts = None
    action = "buy"
    base = "BTC"
    quote = "ETH"
    src = 1
    dst = 1
    orders = [
        ddacrord.Order(
            ts, action, quantity=1, base, limit_price=9, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=3, base, limit_price=8, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=2, base, limit_price=6, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=4, base, limit_price=4, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=5, base, limit_price=2, quote, src, dst
        ),
        ddacrord.Order(
            ts, action, quantity=3, base, limit_price=1, quote, src, dst
        ),
    ]
    return orders


def get_curve(
    orders: List[ddacrord.Order], type_: str
) -> List[Tuple[int, int]]:
    # Extract quantity and proce from the passed orders in order to filter them.
    orders_info = [
        (order.quantity, order.limit_price,) for order in orders
    ]
    # Sort orders by limit price so order of asks is preserved.
    if type_ == "supply":
        # Supply curve starts from the order with the lowest limit price.
        orders_info = sorted(orders_info, key=lambda x: x[1])
    elif type_ == "demand":
        # Supply curve starts from the order with the highest limit price.
        orders_info = sorted(orders_info, key=lambda x: x[1], ascending=False)
    else:
        ValueError("Invalid type_='%s'" % self.type_)
    # Get the coordinates where curves crosses prices axis.
    first_curve_point = (0, orders_info[0][1],)
    # Initiate the list with the first coordintate.
    curve_points = [first_curve_point,]
    # Set amount of quantity that has entered the market before the contemplated order.
    quantity_before = 0
    for order_info in orders_info:
        quantity = order_info[0] + quantity_before
        quantity_before = quantity_before + quantity
        price = order_info[1]
        curve_point = (quantity, price,)
        curve_points.append(curve_point)
    return curve_points
