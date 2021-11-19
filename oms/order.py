"""
Import as:

import oms.order as omorder
"""
import collections
import copy
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import core.dataflow.price_interface as cdtfprint
import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


class Order:
    def __init__(
        self,
        order_id: int,
        price_interface: cdtfprint.AbstractPriceInterface,
        creation_timestamp: pd.Timestamp,
        asset_id: int,
        type_: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        num_shares: float,
        *,
        column_remap: Optional[Dict[str, str]] = None,
    ):
        """
        Represent an order executed in (start_timestamp, end_timestamp].

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
        :param creation_timestamp: when the order was placed
        :param asset_id: ID of the asset
        :param type_: e.g.,
            - `price@twap`: pay the TWAP price in the interval
            - `partial_spread_0.2@twap`: pay the TWAP midpoint weighted by 0.2
        """
        self.order_id = order_id
        self.price_interface = price_interface
        self.creation_timestamp = creation_timestamp
        hdbg.dassert_lte(0, asset_id)
        self.asset_id = asset_id
        self.type_ = type_
        hdbg.dassert_lt(start_timestamp, end_timestamp)
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        hdbg.dassert_ne(num_shares, 0)
        self.num_shares = num_shares
        #
        needed_columns = ["bid", "ask", "price", "midpoint"]
        if column_remap is None:
            column_remap = {col_name: col_name for col_name in needed_columns}
        hdbg.dassert_set_eq(column_remap.keys(), needed_columns)
        self.column_remap: Dict[str, str] = column_remap

    def __str__(self) -> str:
        txt: List[str] = []
        txt.append(f"Order:")
        txt.append(f"order_id={self.order_id}")
        txt.append(f"creation_timestamp='{self.creation_timestamp}'")
        txt.append(f"asset_id={self.asset_id}")
        txt.append(f"type='{self.type_}'")
        txt.append(f"timestamp=[{self.start_timestamp}, {self.end_timestamp}]")
        txt.append(f"num_shares={self.num_shares}")
        return " ".join(txt)

    def to_dict(self) -> Dict[str, Any]:
        dict_: Dict[str, Any] = collections.OrderedDict()
        dict_["order_id"] = self.order_id
        dict_["creation_timestamp"] = self.creation_timestamp
        dict_["asset_id"] = self.asset_id
        dict_["type_"] = self.type_
        dict_["start_timestamp"] = self.start_timestamp
        dict_["end_timestamp"] = self.end_timestamp
        dict_["num_shares"] = self.num_shares
        return dict_

    @staticmethod
    def get_price(
        price_interface: cdtfprint.AbstractPriceInterface,
        # TODO(gp): Move it after end_timestamp.
        asset_id: int,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        timestamp_col_name: str,
        type_: str,
        num_shares: float,
        column_remap: Dict[str, str],
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
            column = column_remap[price_type]
            price = Order._get_price_per_share(
                price_interface,
                start_timestamp,
                end_timestamp,
                timestamp_col_name,
                asset_id,
                column,
                timing,
            )
        elif price_type == "full_spread":
            # Cross the spread depending on buy / sell.
            if num_shares >= 0:
                column = "ask"
            else:
                column = "bid"
            column = column_remap[column]
            price = Order._get_price_per_share(
                price_interface,
                start_timestamp,
                end_timestamp,
                timestamp_col_name,
                asset_id,
                column,
                timing,
            )
        elif price_type.startswith("partial_spread"):
            # Pay part of the spread depending on the parameter encoded in the
            # `price_type` (e.g., twap).
            perc = float(price_type.split("_")[2])
            hdbg.dassert_lte(0, perc)
            hdbg.dassert_lte(perc, 1.0)
            # TODO(gp): This should not be hardwired.
            timestamp_col_name = "end_datetime"
            column = column_remap["bid"]
            bid_price = Order._get_price_per_share(
                price_interface,
                start_timestamp,
                end_timestamp,
                timestamp_col_name,
                asset_id,
                column,
                timing,
            )
            column = column_remap["ask"]
            ask_price = Order._get_price_per_share(
                price_interface,
                start_timestamp,
                end_timestamp,
                timestamp_col_name,
                asset_id,
                column,
                timing,
            )
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
            "type=%s, start_timestamp=%s, end_timestamp=%s -> execution_price=%s",
            type_,
            start_timestamp,
            end_timestamp,
            price,
        )
        return price

    def get_execution_price(self) -> float:
        """
        Get the price that this order executes at.
        """
        try:
            # This order price probably depend on future prices, so we need to allow
            # future peeking (unfortunately).
            old_value = self.price_interface.set_allow_future_peeking(True)
            # TODO(gp): It should not be hardwired.
            timestamp_col_name = "end_datetime"
            price = self.get_price(
                self.price_interface,
                self.asset_id,
                self.start_timestamp,
                self.end_timestamp,
                timestamp_col_name,
                self.type_,
                self.num_shares,
                self.column_remap,
            )
        finally:
            self.price_interface.set_allow_future_peeking(old_value)
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
            and (self.start_timestamp == rhs.start_timestamp)
            and (self.end_timestamp == rhs.end_timestamp)
        )

    def merge(self, rhs: "Order") -> "Order":
        """
        Merge the current order with `rhs` and return the merged order.
        """
        # Only orders for the same type / interval can be merged.
        hdbg.dassert(self.is_mergeable(rhs))
        num_shares = self.num_shares + rhs.num_shares
        order = Order(
            self.price_interface,
            self.type_,
            self.start_timestamp,
            self.end_timestamp,
            num_shares,
        )
        return order

    def copy(self) -> "Order":
        return copy.copy(self)

    @staticmethod
    def _get_price_per_share(
        mi: cdtfprint.AbstractPriceInterface,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        timestamp_col_name: str,
        asset_id: int,
        column: str,
        timing: str,
    ) -> float:
        """
        Get the price corresponding to a certain column and timing (e.g.,
        `start`, `end`, `twap`).

        :param timestamp_col_name: column to use to filter based on start_timestamp and end_timestamp
        :param column: column to use to compute the price
        """
        if timing == "start":
            asset_ids = [asset_id]
            price = mi.get_data_at_timestamp(
                start_timestamp, timestamp_col_name, asset_ids
            )[column]
        elif timing == "end":
            asset_ids = [asset_id]
            price = mi.get_data_at_timestamp(
                end_timestamp, timestamp_col_name, asset_ids
            )[column]
        elif timing == "twap":
            price = mi.get_twap_price(
                start_timestamp,
                end_timestamp,
                timestamp_col_name,
                asset_id,
                column,
            )
        else:
            raise ValueError("Invalid timing='%s'", timing)
        return price


# #############################################################################


def _get_orders_to_execute(
    orders: List[Order], timestamp: pd.Timestamp
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
    orders_to_execute = get_orders_to_execute(orders, timestamp)
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
