"""
Import as:

import defi.dao_cross.order as ddacrord
"""

import logging
from typing import Optional, Union

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hdatetime as hdateti

_LOG = logging.getLogger(__name__)


class Order:
    """
    Create order for DaoCross or DaoSwap.
    """

    def __init__(
        self,
        timestamp: Union[float, pd.Timestamp],
        action: str,
        quantity: float,
        base_token: str,
        limit_price: float,
        quote_token: str,
        deposit_address: Union[int, str],
        wallet_address: Union[int, str],
    ) -> None:
        """
        Constructor.

        :param timestamp: time of order execution
        :param action: order action type
            - "buy": purchase the base token and pay with the quote token
            - "sell": sell the base token and receive the quote token
        :param quantity: quantity in terms of the base token
        :param base_token: token to express order quantity
        :param limit_price: limit price in terms of the quote token per base token
        :param quote_token: token to express order price
        :param deposit_address: deposit address to implement the order for
        :param wallet_address: wallet address to implement the order for
        """
        hdbg.dassert_isinstance(base_token, str)
        hdbg.dassert_isinstance(quote_token, str)
        hdbg.dassert_lte(0, quantity)
        hdbg.dassert_in(action, ["buy", "sell"])
        if pd.isna(timestamp):
            # Use current time of execution if timestamp is not specified.
            self.timestamp = hdateti.get_current_time(tz="UTC")
        else:
            hdbg.dassert_isinstance(timestamp, pd.Timestamp)
            self.timestamp = timestamp
        self.action = action
        self.quantity = quantity
        self.base_token = base_token
        # Replace NaN with signed `np.inf` depending upon `action`.
        # This helps with `Order` comparisons (`lt` and `gt`).
        if pd.isna(limit_price):
            if self.action == "buy":
                self.limit_price = np.inf
            elif self.action == "sell":
                self.limit_price = -np.inf
            else:
                raise ValueError("Invalid action='%s'" % self.action)
        else:
            self.limit_price = limit_price
        self.quote_token = quote_token
        self.deposit_address = deposit_address
        self.wallet_address = wallet_address

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        ret = (
            "timestamp=%s action=%s quantity=%s base_token=%s limit_price=%s quote_token=%s deposit_address=%s wallet_address=%s"
            % (
                self.timestamp,
                self.action,
                self.quantity,
                self.base_token,
                self.limit_price,
                self.quote_token,
                self.deposit_address,
                self.wallet_address,
            )
        )
        return ret

    def __lt__(self, other: "Order") -> bool:
        """
        Compare if order is lower in priority than the passed one.

        Because we use a min-heap, taking precedence in the order queue
        is given "lower priority" in the internal heap.
        """
        return self._takes_precedence(other)

    def __gt__(self, other: "Order") -> bool:
        """
        Compare if order is greater in priority than the passed one.
        """
        return not self._takes_precedence(other)

    def _takes_precedence(self, other: "Order") -> bool:
        """
        Compare order to another one according to quantity, price and
        timestamp. Prioritize orders according to:

            1. Quantity - higher quantity comes first in priority
            2. Price - higher limit price breaks quantity ties
            3. Timestamp - earlier timestamp breaks ties in quantity and price
        :param other: order to compare the actual order with
        :return: "True" if order preceeds the other one, "False" otherwise
        """
        takes_precedence = False
        if self.quantity > other.quantity:
            takes_precedence = True
        elif self.limit_price > other.limit_price:
            takes_precedence = True
        elif self.timestamp > other.timestamp:
            takes_precedence = True
        return takes_precedence


def get_random_order(seed: Optional[int] = None) -> Order:
    """
    Get an order for ETH/BTC with randomized valid parameters.
    """
    if seed is not None:
        np.random.seed(seed)
    # Do not impose a timestamp.
    timestamp = np.nan
    # Generate random buy/sells.
    action = "buy" if np.random.random() < 0.5 else "sell"
    # Generate random quantities.
    quantity = np.random.randint(1, 10)
    # Set token names.
    base_token = "ETH"
    quote_token = "BTC"
    # Do not impose a limit price.
    limit_price = np.nan
    # Create a random wallet address.
    deposit_address = np.random.randint(-3, 3)
    # Prevent self-crossing (in a crude way).
    if action == "buy":
        deposit_address = abs(deposit_address)
    elif action == "sell":
        deposit_address = -abs(deposit_address)
    # Make wallet address and deposit address the same.
    wallet_address = deposit_address
    # Build the order.
    order = Order(
        timestamp,
        action,
        quantity,
        base_token,
        limit_price,
        quote_token,
        deposit_address,
        wallet_address,
    )
    return order


def action_to_int(action: str) -> int:
    """
    Translate an action to an int.

    :param action: direction: `buy` or `sell`
    :return: int representation of a direction
    """
    ret = None
    if action == "buy":
        ret = 1
    elif action == "sell":
        ret = -1
    else:
        raise ValueError(f"Unsupported action={action}")
    return ret
