"""
Import as:

import defi.tulip.implementation.supply_demand as dtuimsd
"""

import logging
import random
from typing import List, Tuple

import pandas as pd

import helpers.hdbg as hdbg
import defi.dao_cross.order as ddacrord

_LOG = logging.getLogger(__name__)



# #############################################################################
# Discrete supply/demand curves.
# #############################################################################


def convert_discrete_curve_to_limit_orders(
    discrete_curve: pd.Series,
    *,
    quantity_scale: float = 1.0,
    quantity_const: float = 0.0,
    limit_price_scale: float = 1.0,
    limit_price_const: float = 0.0,
    seed: int = 42,
) -> List[ddacrord.Order]:
    """
    Get a list of orders that represent supply or demand.

    :param discrete_curve: discrete curve coordinates
    :param quantity_scale: coef to multiply quantities
    :param quantity_const: const to change quantities
    :param limit_price_scale: coef to multiply limit prices
    :param limit_price_const: const to change limit prices
    :param seed: seed for random sampling
    :return: orders that represent demand or supply curve
    """
    random.seed(seed)
    # TODO(Dan): Pass as params?
    # Set default values.
    timestamp = None
    base_token = "BTC"
    quote_token = "ETH"
    # Get order action based on the curve type.
    # TODO(Dan): Add asserts for the curve.
    type_ = discrete_curve.name
    hdbg.dassert_in(type_, ["supply", "demand"])
    if type_ == "supply":
        action = "sell"
        ascending = True
    else:
        action = "buy"
        ascending = False
    # Invert coordinates, so prices are indices and quantities are values.
    # Sort by prices with respect to the curve type.
    inverted_discrete_curve = pd.Series(
        index=discrete_curve.values,
        data=discrete_curve.index,
        name=discrete_curve.name,
    ).sort_index(ascending=ascending)
    # Order quantities are distances between the curve quantities.
    # Order limit prices correspond to the curve limit prices.
    order_data = inverted_discrete_curve.drop_duplicates().diff().dropna()
    # Generate orders.
    orders = []
    for p, q in order_data.items():
        # Adjust quantities and prices by the passed params.
        q = q * quantity_scale + quantity_const
        p = p * limit_price_scale + limit_price_const
        # Generate random addresses.
        deposit_address = random.randint(1, 10)
        wallet_address = deposit_address
        # Build orders.
        order = ddacrord.Order(
            timestamp,
            action,
            q,
            base_token,
            p,
            quote_token,
            deposit_address,
            wallet_address,
        )
        orders.append(order)
    return orders


def get_supply_demand_discrete_curve(
    type_: str,
    orders_df: pd.DataFrame,
) -> pd.Series:
    """
    Get a discrete curve of the specified type from the orders data.

    :param type_: curve type
    :param orders_df: orders data
    :return: coordinates of dots that belong to the curve
    """
    # TODO(Dan): Add asserts for orders compatibility.
    if type_ == "supply":
        orders_df = orders_df[orders_df["action"]=="sell"]
        ascending = True
        # Extend supply curve with a straight line up at the max quantity.
        last_limit_price_mpl = 1.25
    elif type_ == "demand":
        orders_df = orders_df[orders_df["action"]=="buy"]
        ascending = False
        # Extend demand curve with a straight line down until zero quantity.
        last_limit_price_mpl = 0.0
    else:
        raise ValueError("Invalid type_='%s'" % type_)
    orders_df = orders_df[["limit_price", "quantity"]]
    # Sort orders by limit price with respect to the curve type.
    # Supply curve is increasing, orders are in ascending order.
    # Demand curve is decreasing, orders are in descending order.
    orders_df = orders_df.sort_values(by="limit_price", ascending=ascending)
    # Set amount of quantity that has already entered the market.
    quantity_on_market = 0
    dots_x = []
    dots_y = []
    for _, row in orders_df.iterrows():
        price = row["limit_price"]
        order_quantity = row["quantity"]
        # Add a dot that connects order dots on a discrete curve.
        dots_x.append(quantity_on_market)
        dots_y.append(price)
        # Add a dot with order data.
        # Order quantity is a distance between the dot with this order
        # and quantity on market before it.
        dot_x = order_quantity + quantity_on_market
        dots_x.append(dot_x)
        dots_y.append(price)
        # Update quantity on market.
        quantity_on_market = quantity_on_market + order_quantity
    # Add last line of the curve:
    last_quantity = dots_x[-1]
    last_limit_price = dots_y[-1] * last_limit_price_mpl
    dots_x.append(last_quantity)
    dots_y.append(last_limit_price)
    # Build a series from curve coordinates.
    dots = pd.Series(data=dots_y, index=dots_x, name=type_)
    return dots


# #############################################################################
# Aggregated supply/demand curves.
# #############################################################################


def get_supply_demand_aggregated_curve(
    type_: str,
    alpha: float,
    beta: float,
    n_orders: int,
    *,
    max_quantity: float = 100.0,
    seed: int = 42,
) -> pd.Series:
    """
    Get aggregated curve from the passed linear function formula.

    Formula: limit_price = alpha * quantity + beta

    :param type_: curve type
    :param alpha: slope
    :param beta: intercept
    :param n_orders: number of orders to generate
    :param max_quantity: max quantity that a seller can provide
    :param seed: seed for random sampling
    :return: aggregated curve coordinates
    """
    random.seed(seed)
    if type_ == "supply":
        hdbg.dassert_lt(0.0, alpha)
        # Get supply min quantity to set it as a limit.
        min_quantity = - beta / alpha
        if min_quantity < 0.0:
            # Supply quantity cannot be negative.
            min_quantity = 0.0
    elif type_ == "demand":
        hdbg.dassert_lt(alpha, 0.0)
        hdbg.dassert_lt(0.0, beta)
        # A buyer can order any non-negative quantity.
        min_quantity = 0.0
        # Get demand max quantity to set it as a limit.
        max_quantity = - beta / alpha
    else:
        raise ValueError("Invalid type_='%s'" % type_)
    # Generate curve dots.
    quantities = []
    limit_prices = []
    for _ in range(n_orders):
        # Generate random quantity in specified interval.
        q = random.uniform(min_quantity, max_quantity)
        quantities.append(q)
        # Get the corresponging limit price using linear function formula.
        p = alpha * q + beta
        limit_prices.append(p)
    # Sort quantities in ascending order to represent a curve.
    quantities = sorted(quantities)
    # Sort limit prices with respect to the curve type.
    # Supply curve is increasing, limit prices are in ascending order.
    # Demand curve is decreasing, limit prices are in descending order.
    reverse = type_ == "demand"
    limit_prices = sorted(limit_prices, reverse=reverse)
    # Put aggregated curve coodinates in a series.
    agg_curve = pd.Series(data=limit_prices, index=quantities, name=type_)
    return agg_curve


def convert_aggregated_curve_to_limit_orders(
    agg_curve: pd.Series,
    *,
    seed: int = 42,
) -> List[ddacrord.Order]:
    """
    Get orders from aggregated curve dots coordinates.

    :param agg_curve: aggregated curve coordinates
    :param seed: seed for random sampling
    :return: orders that represent demand or supply curve
    """
    random.seed(seed)
    # TODO(Dan): Pass as params?
    # Set default values.
    timestamp = None
    base_token = "BTC"
    quote_token = "ETH"
    # Get order action based on the curve type.
    # TODO(Dan): Add asserts for curve.
    type_ = agg_curve.name
    hdbg.dassert_in(type_, ["supply", "demand"])
    if type_ == "supply":
        action = "sell"
    else:
        action = "buy"
    # Set amount of quantity that has already entered the market.
    quantity_on_market = 0.0
    orders = []
    for q, p in agg_curve.items():
        # Order quantity is a distance between the dot with this order
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
