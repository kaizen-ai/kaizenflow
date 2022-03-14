"""
Import as:

import oms.order as omorder
"""
import collections
import copy
import logging
import re
from typing import Any, Dict, List, Match, Optional, cast

import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


class Order:
    """
    Represent an order to be executed in (start_timestamp, end_timestamp].

    An order is characterized by:
    1) what price the order is executed at
       - E.g.,
           - "price": the (historical) realized price
           - "midpoint": the midpoint
           - "full_spread": always cross the spread to hit ask or lift bid
           - "partial_spread": pay a percentage of spread
    2) when the order is executed
       - E.g.,
           - "start": at beginning of interval
           - "end": at end of interval
           - "twap": using TWAP prices
           - "vwap": using VWAP prices
    3) number of shares to buy (if positive) or sell (if negative)
    """

    _order_id = 0

    def __init__(
        self,
        creation_timestamp: pd.Timestamp,
        asset_id: int,
        type_: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        curr_num_shares: float,
        diff_num_shares: float,
        *,
        order_id: Optional[int] = None,
    ) -> None:
        """
        Constructor.

        :param creation_timestamp: when the order was placed
        :param asset_id: ID of the asset
        :param type_: e.g.,
            - `price@twap`: pay the TWAP price in the interval
            - `partial_spread_0.2@twap`: pay the TWAP midpoint weighted by 0.2
        :param curr_num_shares: the number of currently owned shares
            - This is needed to track that we are aware of the current position
        :param diff_num_shares: the number of shares to buy / sell to reach the
            desired target position
        """
        if order_id is None:
            order_id = self._get_next_order_id()
        self.order_id = order_id
        self.creation_timestamp = creation_timestamp
        # By convention we use `asset_id = -1` for cash.
        hdbg.dassert_lte(0, asset_id)
        self.asset_id = asset_id
        self.type_ = type_
        hdbg.dassert_lt(start_timestamp, end_timestamp)
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.curr_num_shares = float(curr_num_shares)
        hdbg.dassert_ne(diff_num_shares, 0)
        self.diff_num_shares = float(diff_num_shares)
        #
        hdbg.dassert_eq(creation_timestamp.tz, start_timestamp.tz)
        hdbg.dassert_eq(creation_timestamp.tz, end_timestamp.tz)
        self.tz = creation_timestamp.tz

    def __str__(self) -> str:
        txt: List[str] = []
        txt.append("Order:")
        dict_ = self.to_dict()
        for k, v in dict_.items():
            txt.append(f"{k}={v}")
        return " ".join(txt)

    @classmethod
    def from_string(cls, txt: str) -> "Order":
        """
        Create an order from a string coming from `__str__()`.
        """
        # Parse the string.
        m = re.match(
            "^Order: order_id=(.*) creation_timestamp=(.*) asset_id=(.*) "
            "type_=(.*) start_timestamp=(.*) end_timestamp=(.*) "
            "curr_num_shares=(.*) diff_num_shares=(.*) tz=(.*)",
            txt,
        )
        hdbg.dassert(m, "Can't match '%s'", txt)
        m = cast(Match[str], m)
        # Build the object.
        tz = m.group(9)
        order_id = int(m.group(1))
        creation_timestamp = pd.Timestamp(m.group(2), tz=tz)
        asset_id = int(m.group(3))
        type_ = m.group(4)
        start_timestamp = pd.Timestamp(m.group(5), tz=tz)
        end_timestamp = pd.Timestamp(m.group(6), tz=tz)
        curr_num_shares = float(m.group(7))
        diff_num_shares = float(m.group(8))
        return cls(
            creation_timestamp,
            asset_id,
            type_,
            start_timestamp,
            end_timestamp,
            curr_num_shares,
            diff_num_shares,
            order_id=order_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        dict_: Dict[str, Any] = collections.OrderedDict()
        dict_["order_id"] = self.order_id
        dict_["creation_timestamp"] = self.creation_timestamp
        dict_["asset_id"] = self.asset_id
        dict_["type_"] = self.type_
        dict_["start_timestamp"] = self.start_timestamp
        dict_["end_timestamp"] = self.end_timestamp
        dict_["curr_num_shares"] = self.curr_num_shares
        dict_["diff_num_shares"] = self.diff_num_shares
        dict_["tz"] = self.tz
        return dict_

    def is_mergeable(self, rhs: "Order") -> bool:
        """
        Return whether this order can be merged (i.e., internal crossed) with
        `rhs`.

        Two orders can be merged if they are of the same type and on the
        same interval. The merged order combines the `diff_num_shares`
        of the two orders.
        """
        return (
            (self.type_ == rhs.type_)
            and (self.start_timestamp == rhs.start_timestamp)
            and (self.end_timestamp == rhs.end_timestamp)
            and (self.curr_num_shares == rhs.curr_num_shares)
        )

    def merge(self, rhs: "Order") -> "Order":
        """
        Merge the current order with `rhs` and return the merged order.
        """
        # Only orders for the same type / interval can be merged.
        hdbg.dassert(self.is_mergeable(rhs))
        diff_num_shares = self.diff_num_shares + rhs.diff_num_shares
        order = Order(
            self.type_,
            self.start_timestamp,
            self.end_timestamp,
            self.curr_num_shares,
            diff_num_shares,
        )
        return order

    def copy(self) -> "Order":
        # TODO(gp): This is dangerous since we might copy the MarketData too.
        return copy.copy(self)

    def _get_next_order_id(self) -> int:
        order_id = Order._order_id
        Order._order_id += 1
        return order_id


# #############################################################################


def orders_to_string(orders: List[Order]) -> str:
    """
    Get the string representations of a list of Orders.
    """
    return "\n".join(map(str, orders))


def orders_from_string(txt: str) -> List[Order]:
    """
    Deserialize a list of Orders from a multi-line string.

    E.g.,
    ```
    Order: order_id=0 creation_timestamp=2021-01-04 09:29:00-05:00 asset_id=1 ...
    Order: order_id=1 creation_timestamp=2021-01-04 09:29:00-05:00 asset_id=3 ...
    ```
    """
    orders: List[Order] = []
    for line in txt.split("\n"):
        order = Order.from_string(line)
        _LOG.debug("line='%s'\n-> order=%s", line, order)
        orders.append(order)
    return orders


def _get_orders_to_execute(
    timestamp: pd.Timestamp,
    orders: List[Order],
) -> List[Order]:
    """
    Return the orders from `orders` that can be executed at `timestamp`.
    """
    orders.sort(key=lambda x: x.start_timestamp, reverse=False)
    hdbg.dassert_lte(orders[0].start_timestamp, timestamp)
    # TODO(gp): This is inefficient. Use binary search.
    curr_orders = []
    for order in orders:
        if order.start_timestamp == timestamp:
            curr_orders.append(order)
    return curr_orders


def get_orders_to_execute(
    timestamp: pd.Timestamp, orders: List[Order]
) -> List[Order]:
    if True:
        if orders[0].start_timestamp == timestamp:
            return [orders.pop()]
        # hdbg.dassert_eq(len(orders), 1, "%s", orders_to_string(orders))
        assert 0
    orders_to_execute = _get_orders_to_execute(orders, timestamp)
    _LOG.debug("orders_to_execute=%s", orders_to_string(orders_to_execute))
    # Merge the orders.
    merged_orders = []
    while orders_to_execute:
        order = orders_to_execute.pop()
        orders_to_execute_tmp = orders_to_execute[:]
        for next_order in orders_to_execute_tmp:
            if order.is_mergeable(next_order):
                order = order.merge(next_order)
                orders_to_execute_tmp.remove(next_order)
        merged_orders.append(order)
        orders_to_execute = orders_to_execute_tmp
    _LOG.debug(
        "After merging:\n  merged_orders=%s\n  orders_to_execute=%s",
        orders_to_string(merged_orders),
        orders_to_string(orders_to_execute),
    )
    return merged_orders