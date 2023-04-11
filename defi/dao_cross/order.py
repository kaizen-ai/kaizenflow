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


# TODO(gp): Maybe LimitOrder or DaoLimitOrder?
class Order:
    """
    Limit order to be used in DaoCross or DaoSwap.
    """

    # TODO(gp): @all Reorg the params to match the white paper order.
    # (timestamp, action, quantity, base_token, limit_price, quote_token,
    # deposit_address)
    def __init__(
        self,
        base_token: str,
        quote_token: str,
        action: str,
        quantity: float,
        limit_price: float,
        # TODO(gp): -> dst_address?
        deposit_address: Union[int, str],
        # TODO(gp): -> src_address?
        wallet_address: Union[int, str],
        timestamp: Optional[pd.Timestamp],
    ) -> None:
        """
        Constructor.

        According to the white paper, an order like:

        ```(0xabcd0000, 1678660406, buy, 3.2, ETH, 4.0, BTC, 0xdeadc0de)```

        corresponds to the natural language description:
        "At timestamp Mon Mar 13 2023 02:33:25 GMT+0000, the user with wallet
        0xabcd0000 commits to buy up to 3.2 units of ETH in exchange for BTC up
        to a limit price of 4.0 BTC per ETH with proceeds deposited at 0xdeadc0de"

        :param base_token: token to express order quantity with (e.g., ETH)
        :param quote_token: token to express order price with (e.g., BTC)
        :param action: order action type
            - "buy": purchase the base token and pay with the quote token
            - "sell": sell the base token and receive the quote token
        :param quantity: maximum quantity in terms of the base token (e.g., 3.2)
        :param limit_price: limit price in terms of the quote token (e.g.,
            4.0 BTC per ETH). The limit price is interpreted as non-strict
            inequality, e.g., if `limit_price=4`, the order can be executed
            with a price of 4.0 quote / base token
        :param deposit_address: deposit address to transfer the result of the swap
            (e.g., 0xdeadc0de)
        :param wallet_address: wallet address with the token to provide to the
            swap (e.g., 0xabcd0000)
        :param timestamp: time of order execution (e.g., "Mon Mar 13 2023
            02:33:25 GMT+0000")
            - `None` means the current wall clock time
        """
        hdbg.dassert_isinstance(base_token, str)
        hdbg.dassert_isinstance(quote_token, str)
        hdbg.dassert_lte(0, quantity)
        hdbg.dassert_in(action, ["buy", "sell"])
        self.base_token = base_token
        self.quote_token = quote_token
        self.action = action
        self.quantity = quantity
        # Replace NaN with signed `np.inf` depending upon `action`.
        # This helps with `Order` comparisons (`lt` and `gt`).
        if np.isnan(limit_price):
            if self.action == "buy":
                self.limit_price = np.inf
            elif self.action == "sell":
                self.limit_price = -np.inf
            else:
                raise ValueError("Invalid action='%s'" % self.action)
        else:
            self.limit_price = limit_price
        # Use current time of execution if timestamp is not specified.
        if timestamp:
            hdbg.dassert_type_is(timestamp, pd.Timestamp)
            self.timestamp = timestamp
        else:
            self.timestamp = hdateti.get_current_time(tz="UTC")
        self.deposit_address = deposit_address
        self.wallet_address = wallet_address

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        # TODO(gp): @all add wallet_address
        # TODO(gp): @all reorder to match the constructor
        ret = (
            "base_token=%s quote_token=%s action=%s quantity=%s limit_price=%s timestamp=%s deposit_address=%s wallet_address=%s"
            % (
                self.base_token,
                self.quote_token,
                self.action,
                self.quantity,
                self.limit_price,
                self.timestamp,
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
        Compare order to another one according to quantity, price and timestamp.

        Prioritize orders according to:
            1. Quantity - higher quantity comes first in priority
            2. Price - higher limit price breaks quantity ties
            3. Timestamp - earlier timestamp breaks ties in quantity and price
        :param other: order to compare the actual order with
        :return: "True" if this order preceeds the other one, "False" otherwise
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
    base_token = "ETH"
    quote_token = "BTC"
    # Generate random buy/sells.
    action = "buy" if np.random.random() < 0.5 else "sell"
    # Generate random quantities.
    quantity = np.random.randint(1, 10)
    # Do not impose a limit price.
    limit_price = np.nan
    # Do not impose a timestamp.
    timestamp = np.nan
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
        base_token,
        quote_token,
        action,
        quantity,
        limit_price,
        timestamp,
        deposit_address,
        wallet_address,
    )
    return order


# TODO(gp): I'd make it a static method of Order.
def is_active_order(order: Optional[Order]) -> bool:
    """
    Return whether `order` is active or not.

    Order is active if it is not empty and its quantity is above 0.
    """
    if order is None:
        return False
    if not order.quantity > 0:
        return False
    return True


# TODO(gp): I'd make it a static method of Order.
def action_to_int(action: str) -> int:
    """
    Translate an action to an int.

    :param action: direction: `buy` or `sell`
    :return: int representation of a direction with the usual conventions of
        buy / sell
    """
    ret = None
    if action == "buy":
        ret = 1
    elif action == "sell":
        ret = -1
    else:
        raise ValueError(f"Unsupported action={action}")
    return ret
