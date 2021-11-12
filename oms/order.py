"""
Import as:

import oms.order as oord
"""

import logging
from typing import List

import pandas as pd

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


# TODO(gp): Replace MarketInterface -> PriceInterface
# TODO(gp): mi -> price_interface
class Order:
    def __init__(
        self,
        order_id: int,
        mi: MarketInterface,
        creation_ts: pd.Timestamp,
        asset_id: int,
        type_: str,
        ts_start: pd.Timestamp,
        ts_end: pd.Timestamp,
        num_shares: float,
    ):
        """
        Represent an order executed in the interval of time (ts_start, ts_end].

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

        :param order_id: unique ID for cross-referencing
        :param creation_ts: when the order was placed
        :param asset_id: ID of the asset
        :param type_: e.g.,
            - `price@twap`: pay the TWAP price in the interval
            - `partial_spread_0.2@twap`: pay the TWAP midpoint weighted by 0.2
        """
        self._order_id = order_id
        self._mi = mi
        self._creation_ts = creation_ts
        hdbg.dassert_lte(0, asset_id)
        self._asset_id = asset_id
        self.type_ = type_
        hdbg.dassert_lt(ts_start, ts_end)
        self.ts_start = ts_start
        self.ts_end = ts_end
        hdbg.dassert_ne(num_shares, 0)
        self.num_shares = num_shares

    def __str__(self) -> str:
        return (
            f"Order: type={self.type_} "
            + f"ts=[{self.ts_start}, {self.ts_end}] "
            + f"num_shares={self.num_shares}"
        )

    @staticmethod
    def get_price(
        mi: MarketInterface,
        type_: str,
        ts_start: pd.Timestamp,
        ts_end: pd.Timestamp,
        num_shares: float,
    ) -> float:
        """
        Get the price that a generic order with the given parameters would
        achieve.

        :param type_: like in the constructor
        """
        # Parse the order type.
        config = type_.split("@")
        hdbg.dassert_eq(len(config), 2, "Invalid type_='%s'", type_)
        price_type, timing = config
        # Get the price depending on the price_type.
        if price_type in ("price", "midpoint"):
            column = price_type
            price = Order._get_price(mi, ts_start, ts_end, column, timing)
        elif price_type == "full_spread":
            # Cross the spread depending on buy / sell.
            if num_shares >= 0:
                column = "ask"
            else:
                column = "bid"
            price = Order._get_price(mi, ts_start, ts_end, column, timing)
        elif price_type.startswith("partial_spread"):
            # Pay part of the spread depending on the parameter encoded in the
            # `price_type` (e.g., twap
            perc = float(price_type.split("_")[2])
            hdbg.dassert_lte(0, perc)
            hdbg.dassert_lte(perc, 1.0)
            bid_price = Order._get_price(mi, ts_start, ts_end, "bid", timing)
            ask_price = Order._get_price(mi, ts_start, ts_end, "ask", timing)
            if num_shares >= 0:
                # We need to buy:
                # - if perc == 1.0 pay ask (i.e., pay full-spread)
                # - if perc == 0.5 pay midpoint
                # - if perc == 0.0 pay bid
                price = perc * ask_price + (1.0 - perc) * bid_price
            else:
                # We need to sell:
                # - if perc == 1.0 pay bid (i.e., pay full-spread)
                # - if perc == 0.5 pay midpoint
                # - if perc == 0.0 pay ask
                price = (1.0 - perc) * ask_price + perc * bid_price
        else:
            raise ValueError("Invalid type='%s'", type_)
        _LOG.debug(
            "type=%s, ts_start=%s, ts_end=%s -> execution_price=%s",
            type_,
            ts_start,
            ts_end,
            price,
        )
        return price

    def get_execution_price(self) -> float:
        """
        Get the price that this order executes at.
        """
        price = self.get_price(
            self._mi, self.type_, self.ts_start, self.ts_end, self.num_shares
        )
        return price

    def is_mergeable(self, rhs: "Order") -> bool:
        """
        Return whether this order can be merged (i.e., internal crossed) with
        `rhs`.

        Two orders can be merged if they are of the same type and on the
        same interval. The merged order combines the `num_shares` of the
        two orders.
        """
        return (
            (self.type_ == rhs.type_)
            and (self.ts_start == rhs.ts_start)
            and (self.ts_end == rhs.ts_end)
        )

    def merge(self, rhs: "Order") -> "Order":
        """
        Merge the current order with `rhs` and return the merged order.
        """
        # Only orders for the same type / interval can be merged.
        hdbg.dassert(self.is_mergeable(rhs))
        num_shares = self.num_shares + rhs.num_shares
        order = Order(
            self._mi, self.type_, self.ts_start, self.ts_end, num_shares
        )
        return order

    def copy(self) -> "Order":
        return copy.copy(self)

    @staticmethod
    def _get_price(
        mi: MarketInterface,
        ts_start: pd.Timestamp,
        ts_end: pd.Timestamp,
        column: str,
        timing: str,
    ) -> float:
        """
        Get the price corresponding to a certain column and timing (e.g.,
        `start`, `end`, `twap`).
        """
        if timing == "start":
            price = mi.get_instantaneous_price(ts_start, column)
        elif timing == "end":
            price = mi.get_instantaneous_price(ts_end, column)
        elif timing == "twap":
            price = mi.get_twap_price(ts_start, ts_end, column)
        else:
            raise ValueError("Invalid timing='%s'", timing)
        return price


# #############################################################################


def _get_orders_to_execute(orders: List[Order], ts: pd.Timestamp) -> List[Order]:
    """
    Return the orders from `orders` that can be executed at timestamp `ts`.
    """
    orders.sort(key=lambda x: x.ts_start, reverse=False)
    hdbg.dassert_lte(orders[0].ts_start, ts)
    # TODO(gp): This is inefficient. Use binary search.
    curr_orders = []
    for order in orders:
        if order.ts_start == ts:
            curr_orders.append(order)
    return curr_orders


def get_orders_to_execute(ts: pd.Timestamp, orders: List[Order]) -> List[Order]:
    if True:
        if orders[0].ts_start == ts:
            return [orders.pop()]
        # hdbg.dassert_eq(len(orders), 1, "%s", orders_to_string(orders))
        assert 0
    orders_to_execute = get_orders_to_execute(orders, ts)
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


def orders_to_string(orders: List[Order]) -> str:
    return str(list(map(str, orders)))
