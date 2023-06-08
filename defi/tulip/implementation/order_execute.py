"""
Import as:

import defi.tulip.implementation.order_execute as dtimorex
"""

from typing import List, Tuple

import defi.tulip.implementation.order as dtuimord


def execute_order(order: dtuimord, price: float) -> List[Tuple[float, str]]:
    """
    Execute order at specified price.

    :param order: Order object
    :param price: Price that user pays in quote_token in exchange to get base_token
    :return: A list that contains deductions in quote_token and acquired base_token
    """
    if price < order.limit_price:
        return [
            (-order.quantity * price, order.quote_token),
            (order.quantity, order.base_token),
        ]
    return None
