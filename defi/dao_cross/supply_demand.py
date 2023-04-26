"""
Import as:

import defi.dao_cross.supply_demand as ddcrsede
"""

import logging
import random
from typing import List, Tuple

import helpers.hdbg as hdbg
import defi.dao_cross.order as ddacrord

_LOG = logging.getLogger(__name__)


def get_curve_orders(
    type_: str,
    quantities: List[int],
    limit_prices: List[int],
    *,
    quantity_scale: float = 1.0,
    quantity_const: float = 0.0,
    limit_price_scale: float = 1.0,
    limit_price_const: float = 0.0,
) -> List[ddacrord.Order]:
    """
    Get a list of orders that represent supply or demand.
    """
    orders = []
    for quantity, limit_price in zip(quantities, limit_prices):
        # Set default values.
        timestamp = None
        base_token = "BTC"
        quote_token = "ETH"
        # Get order action based on the curve type.
        if type_ == "supply":
            action = "sell"
        elif type_ == "demand":
            action = "buy"
        else:
            raise ValueError("Invalid type_='%s'" % type_)
        # Adjust quantities and prices by the passed params.
        quantity = quantity * quantity_scale + quantity_const
        limit_price = limit_price * limit_price_scale + limit_price_const
        # Generate random addresses.
        deposit_address = random.randint(1, 10)
        wallet_address = deposit_address
        # Build orders.
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


def get_curve_dots(
    orders: List[ddacrord.Order], type_: str
) -> List[Tuple[int, int]]:
    """
    Get coordinates of dots that represent a curve of the specified type.
    """
    hdbg.dassert_in(type_, ["demand", "supply"])
    # Extract quantity and price from the passed orders for sorting.
    orders_info = [
        (order.quantity, order.limit_price,) for order in orders
    ]
    # Sort orders by limit price with respect to the curve type.
    # Supply curve is monotonically increasing, orders are in ascending order.
    # Demand curve is monotonically decreasing, orders are in descending order.
    reverse = type_ == "demand"
    orders_info = sorted(orders_info, key=lambda x: x[1], reverse=reverse)
    # Set amount of quantity that has already entered the market.
    quantity_on_market = 0
    dots = []
    for order_info in orders_info:
        # Order quintity is a distance between the dot with this order
        # and quantity on market before it.
        quantity = order_info[0] + quantity_on_market
        price = order_info[1]
        # Add a dot that connects order dots on a broken curve.
        dot1 = (quantity_on_market, price)
        dots.append(dot1)
        # Add a dot with order data.
        dot2 = (quantity, price)
        dots.append(dot2)
        # Update quantity on market.
        quantity_on_market = quantity_on_market + order_info[0]
    # Add last line of the curve:
    if type_ == "supply":
        # Extend supply curve with a straight line up at the max quantity.
        last_dot = (dots[-1][0], dots[-1][1] * 1.25)
    elif type_ == "demand":
        # Extend demand curve with a straight line down until zero quantity.
        last_dot = (dots[-1][0], 0)
    else:
        raise ValueError("Invalid type_='%s'" % type_)
    dots.append(last_dot)
    return dots
