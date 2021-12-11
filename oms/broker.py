"""
Import as:

import oms.broker as ombroker
"""

import abc
import collections
import logging
from typing import Any, Dict, List

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.sql as hsql
import market_data.market_data_interface as mdmadain
import oms.oms_db as oomsdb
import oms.order as omorder

_LOG = logging.getLogger(__name__)


# #############################################################################


class Fill:
    """
    Represent an order fill.

    An order can be filled partially or completely. Each fill can happen at
    different prices.

    The simplest case is for an order to be completely filled (e.g., at the end of
    its VWAP execution window) at a single price. In this case a single `Fill`
    object can represent the execution.
    """

    _fill_id = 0

    def __init__(
        self,
        order: omorder.Order,
        timestamp: pd.Timestamp,
        num_shares: float,
        price: float,
    ):
        # TODO(Paul): decide how to id these.
        self._fill_id = Fill._fill_id
        # Pointer to the order.
        self.order = order
        # TODO(gp): An Order should contain a list of pointers to its fills for
        #  accounting purposes.
        #  We can verify the invariant that no more than the desired quantity
        #  was filled.
        # Timestamp of when it was completed.
        self.timestamp = timestamp
        # Number of shares executed. This has the same meaning as in Order, i.e., it
        # can be positive and negative depending on long / short.
        hdbg.dassert_ne(num_shares, 0)
        self.num_shares = num_shares
        # Price executed for the given shares.
        hdbg.dassert_lt(0, price)
        self.price = price

    def __str__(self) -> str:
        txt: List[str] = []
        txt.append("Fill:")
        dict_ = self.to_dict()
        for k, v in dict_.items():
            txt.append(f"{k}={v}")
        return " ".join(txt)

    def to_dict(self) -> Dict[str, Any]:
        dict_: Dict[str, Any] = collections.OrderedDict()
        dict_["asset_id"] = self.order.asset_id
        dict_["fill_id"] = self.order.order_id
        dict_["timestamp"] = self.timestamp
        dict_["num_shares"] = self.num_shares
        dict_["price"] = self.price
        return dict_


# #############################################################################


class AbstractBroker(abc.ABC):
    """
    Represent a broker to which we can place orders and receive fills back.
    """

    def __init__(
        self,
        market_data_interface: mdmadain.AbstractMarketDataInterface,
        # TODO(gp): -> market_data_interface.get_wall_clock_time
        get_wall_clock_time: hdateti.GetWallClockTime,
    ) -> None:
        hdbg.dassert_issubclass(
            market_data_interface, mdmadain.AbstractMarketDataInterface
        )
        self._market_data_interface = market_data_interface
        self._get_wall_clock_time = get_wall_clock_time
        # Track the orders for internal accounting.
        self._orders: List[omorder.Order] = []
        # Last seen timestamp to enforce that time is only moving ahead.
        self._last_timestamp = None

    def submit_orders(
        self,
        orders: List[omorder.Order],
    ) -> None:
        """
        Submit a list of orders to the broker at the current wall clock time.
        """
        _ = self._update_last_timestamp()
        # Submit the orders.
        _LOG.debug("Submitting orders=%s", omorder.orders_to_string(orders))
        self._orders.extend(orders)
        self._submit_orders(orders)

    def get_fills(self, as_of_timestamp: pd.Timestamp) -> List[Fill]:
        """
        Get fills for the orders that should have been executed by
        `as_of_timestamp`.

        Note that this function can be called only once for a given
        `as_of_timestamp`. In fact it assumes that the fills are
        consumed and processed from the caller and fills are deleted.
        """
        wall_clock_timestamp = self._update_last_timestamp()
        # Check future peeking.
        if as_of_timestamp > wall_clock_timestamp:
            raise ValueError(
                "You are asking about the future: " +
                f"as_of_timestamp={as_of_timestamp} > " +
                f"wall_clock_timestamp={wall_clock_timestamp}"
            )
        # Get the fills.
        fills = self._get_fills(as_of_timestamp)
        return fills

    @abc.abstractmethod
    def _submit_orders(
        self,
        orders: List[omorder.Order],
    ) -> None:
        ...

    @abc.abstractmethod
    def _get_fills(self, as_of_timestamp: pd.Timestamp) -> List[Fill]:
        ...

    def _update_last_timestamp(self) -> pd.Timestamp:
        """
        Make sure that the current wall clock time is after the previous
        interaction.

        :return: current wall clock time
        """
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        #
        if self._last_timestamp is not None:
            hdbg.dassert_lte(self._last_timestamp, wall_clock_timestamp)
        # Update.
        self._last_timestamp = wall_clock_timestamp
        return wall_clock_timestamp


# #############################################################################


# TODO(Paul): -> SimulatedBroker
# TODO(Paul): Add unit tests
class Broker(AbstractBroker):
    """
    Represent a broker to which we can place orders and receive fills back.
    """

    def __init__(
        self,
        *args: Any,
    ) -> None:
        super().__init__(*args)
        # Map a timestamp to the orders with that execution time deadline.
        self._deadline_timestamp_to_orders: Dict[
            pd.Timestamp, List[omorder.Order]
        ] = collections.defaultdict(list)
        # Track the fills for internal accounting.
        self._fills: List[Fill] = []

    def _submit_orders(
        self,
        orders: List[omorder.Order],
    ) -> None:
        # Enqueue the orders based on their completion deadline time.
        _LOG.debug("Submitting %d orders", len(orders))
        for order in orders:
            _LOG.debug("Submitting order %s", order.order_id)
            # TODO(gp): curr_timestamp <= order.start_timestamp
            self._deadline_timestamp_to_orders[order.end_timestamp].append(order)

    def _get_fills(self, as_of_timestamp: pd.Timestamp) -> List[Fill]:
        # We should always get the "next" orders, for this reason one should use
        # a priority queue.
        timestamps = self._deadline_timestamp_to_orders.keys()
        _LOG.debug("Timestamps of orders in queue: %s", timestamps)
        if not timestamps:
            return []
        # In our current execution model, we should ask about the orders that are
        # terminating.
        hdbg.dassert_eq(min(timestamps), as_of_timestamp)
        orders_to_execute = self._deadline_timestamp_to_orders[as_of_timestamp]
        _LOG.debug("Executing %d orders", len(orders_to_execute))
        # `as_of_timestamp` should match the end time of the orders.
        for order in orders_to_execute:
            hdbg.dassert_eq(as_of_timestamp, order.end_timestamp)
        # "Execute" the orders.
        fills = []
        for order in orders_to_execute:
            # TODO(gp): Here there should be a programmable logic that decides
            #  how many shares are filled.
            fills.extend(self._fully_fill(as_of_timestamp, order))
        self._fills.extend(fills)
        # Remove the orders that have been executed.
        _LOG.debug(
            "Removing orders from queue with deadline=`%s`", as_of_timestamp
        )
        del self._deadline_timestamp_to_orders[as_of_timestamp]
        _LOG.debug("-> Returning fills:\n%s", str(fills))
        return fills

    def _fully_fill(
        self, wall_clock_timestamp: pd.Timestamp, order: omorder.Order
    ) -> List[Fill]:
        num_shares = order.num_shares
        # TODO(Paul): We should move the logic here.
        price = order.get_execution_price()
        fill = Fill(order, wall_clock_timestamp, num_shares, price)
        return [fill]


# #############################################################################


class MockedBroker(AbstractBroker):
    """
    Implement an object that mocks a real OMS / broker backed by a DB where
    updates to the state representing the placed orders are asynchronous.

    The DB contains the following tables:
    - `processed`
        - tradedate
        - strategyid
        - timestamp_db
            - when the order list was received from the OMS

    - A more complex implementation can also have:
        - target_count
        - changed_count
        - unchanged_count
    """

    _submitted_order_id: int = 0

    def __init__(
        self,
        *args: Any,
        db_connection: hsql.DbConnection,
        submitted_orders_table_name: str,
        accepted_orders_table_name: str,
    ):
        super().__init__(*args)
        self._db_connection = db_connection
        self._submitted_orders_table_name = submitted_orders_table_name
        self._accepted_orders_table_name = accepted_orders_table_name

    def submit_orders(
        self,
        orders: List[omorder.Order],
    ) -> None:
        # Add an order in the submitted orders table.
        submitted_order_id = self._get_next_submitted_order_id()
        #
        file_name = f"filename_{submitted_order_id}.txt"
        timestamp_db = self._get_wall_clock_time()
        orders_as_txt = omorder.orders_to_string(orders)
        row = [
            ("filename", file_name),
            ("timestamp_db", timestamp_db),
            ("orders_as_txt", orders_as_txt),
        ]
        row = pd.Series(row)
        hsql.execute_insert_query(
            self._db_connection, row, self._submitted_orders_table_name
        )
        # TODO(gp): Wait on `OmsDb.processed_orders`.

    def get_fills(self, curr_timestamp: pd.Timestamp) -> List[Fill]:
        """
        The reference system doesn't return fills but directly updates the
        state of a table representing the current holdings.
        """
        raise NotImplementedError

    def _get_next_submitted_order_id(self) -> int:
        submitted_order_id = self._submitted_order_id
        self._submitted_order_id += 1
        return submitted_order_id
