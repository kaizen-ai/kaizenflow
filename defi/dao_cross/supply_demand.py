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



# #############################################################################
# Broken supply/demand curves.
# #############################################################################


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

    :param type_: curve type
    :param quantities: coordinates on quantity axis
    :param limit_prices: coordinates on limit prices axis
    :param quantity_scale: coef to multiply quantities
    :param quantity_const: const to change quantities
    :param limit_price_scale: coef to multiply limit prices
    :param limit_price_const: const to change limit prices
    :return: orders that represent demand or supply curve
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

    :param orders: orders that represent the curve
    :param type_: curve type
    :return: coordinates of dots that belong to curve
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


# #############################################################################
# Linear supply/demand curves.
# #############################################################################


def get_linear_supply_orders(
    alpha: float,
    beta: float,
    n_orders: int,
    *,
    max_quantity: float = 100.0,
    seed: int = 42,
) -> List[ddacrord.Order]:
    """
    Get orders that represent a linear supply curve.

    Formula: limit_price = alpha * quantity + beta

    :param alpha: slope
    :param beta: intercept
    :param n_orders: number of orders to generate
    :param max_quantity: max quantity that a seller can provide
    :param seed: seed for random sampling
    :return: orders that represent linear supply curve
    """
    random.seed(seed)
    hdbg.dassert_lt(0.0, alpha)
    # Get supply min quantity to set it as a limit.
    min_quantity = - beta / alpha
    if min_quantity < 0.0:
        # Supply quantity cannot be negative.
        min_quantity = 0.0
    # Generate supply curve dots.
    quantities = []
    limit_prices = []
    for _ in range(n_orders):
        # Generate random quantity in specified interval.
        q = random.uniform(min_quantity, max_quantity)
        quantities.append(q)
        # Get the corresponging limit price using linear function formula.
        p = alpha * q + beta
        limit_prices.append(p)
    # Sort values in both dot lists in ascending order to represent supply.
    quantities = sorted(quantities)
    limit_prices = sorted(limit_prices)
    # Get orders.
    type_ = "supply"
    orders = _get_orders_from_dots(type_, quantities, limit_prices)
    return orders


def get_linear_demand_orders(
    alpha: float,
    beta: float,
    n_orders: int,
    *,
    seed: int = 42,
) -> List[ddacrord.Order]:
    """
    Get orders that represent a linear demand curve.

    Formula: limit_price = alpha * quantity + beta

    :param alpha: slope
    :param beta: intercept
    :param n_orders: number of orders to generate
    :param seed: seed for random sampling
    :return: orders that represent linear demand curve
    """
    random.seed(seed)
    hdbg.dassert_lt(alpha, 0.0)
    hdbg.dassert_lt(0.0, beta)
    # Get demand max quantity to set it as a limit.
    max_quantity = - beta / alpha
    # Generate supply curve dots.
    quantities = []
    limit_prices = []
    for _ in range(n_orders):
        # Generate random quantity in specified interval.
        q = random.uniform(0.0, max_quantity)
        quantities.append(q)
        # Get the corresponging limit price using linear function formula.
        p = alpha * q + beta
        limit_prices.append(p)
    # Sort quantity in ascending order and limit prices in descending
    # to represent demand.
    quantities = sorted(quantities)
    limit_prices = sorted(limit_prices, reverse=True)
    # Get orders.
    type_ = "demand"
    orders = _get_orders_from_dots(type_, quantities, limit_prices)
    return orders



def _get_orders_from_dots(
    type_: str,
    quantities: List[float],
    limit_prices: List[float],
) -> List[ddacrord.Order]:
    """
    Get orders from linear curve dots coordinates.

    :param type_: curve type
    :param quantities: coordinates on quantity axis
    :param limit_prices: coordinates on limit prices axis
    :return: orders that represent demand or supply curve
    """
    # Set amount of quantity that has already entered the market.
    quantity_on_market = 0.0
    orders = []
    for q, p in zip(quantities, limit_prices):
        # TODO(Dan): Pass as params?
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
        # Order quintity is a distance between the dot with this order
        # and quantity on market before it.
        order_q = q - quantity_on_market
        # Generate random addresses.
        deposit_address = random.randint(1, 10)
        wallet_address = deposit_address
        # Generate orders.
        order = ddacrord.Order(
            timestamp=timestamp,
            action=action,
            quantity=order_q,
            base_token=base_token,
            limit_price=p,
            quote_token=quote_token,
            deposit_address=deposit_address,
            wallet_address=wallet_address,
        )
        orders.append(order)
        # Update quantity on market.
        quantity_on_market = q
    return orders 
