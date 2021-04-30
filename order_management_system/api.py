import logging
import pandas as pd
from typing import List, Dict, Optional, Set

import helpers.dbg as dbg
import helpers.printing as prn

_LOG = logging.getLogger(__name__)

class Contract:
    """
    Represent a financial instrument.

    Modelled after:
    https://ib-insync.readthedocs.io/api.html#module-ib_insync.contract
    """

    def __init__(self, symbol: str, sec_type: str, currency: Optional[str]=None,
            exchange: Optional[str]=None):
        self.symbol = symbol
        dbg.dassert_in(sec_type, ("STK", "FUT"))
        self.sec_type = sec_type
        dbg.dassert_in(currency, ("USD", None))
        self.currency = currency
        self.exchange = exchange

    def __repr__(self):
        return "Contract: symbol=%s, sec_type=%s, currency=%s, exchange=%s" % (
            self.symbol, self.sec_type, self.currency, self.exchange
        )

    def __key(self):
        return (self.symbol, self.sec_type, self.currency, self.exchange)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Contract):
            return self.__key() == other.__key()
        return NotImplemented


class ContinuousFutures(Contract):
    pass

class Futures(Contract):
    pass

class Stock(Contract):
    pass

# #############################################################################

class Order:
    """
    Order for trading contracts.

    Modelled after:
    https://ib-insync.readthedocs.io/api.html#ib_insync.order.Order
    """

    def __init__(self, order_id: int, action: str, total_quantity: float,
            order_type: str, timestamp: Optional[pd.Timestamp] = None):
        self.order_id = order_id
        dbg.dassert_in(action, ("BUY", "SELL"))
        self.action = action
        dbg.dassert_lt(0.0, total_quantity)
        self.total_quantity = total_quantity
        dbg.dassert_in(order_type, ("MKT", "LIM"))
        self.order_type = order_type
        #
        self.timestamp = timestamp

    def __repr__(self):
        return "Order: order_id=%s, action=%s, total_quantity=%s, order_type=%s timestamp=%s" % (
            self.order_id, self.action, self.total_quantity, self.order_type, self.timestamp
        )

class MarketOrder(Order):
    pass


class LimitOrder(Order):
    def __init__(self, limit_price: float):
        self.limit_price = limit_price


# #############################################################################

class Position:
    """
    Modelled after:
    https://ib-insync.readthedocs.io/api.html#ib_insync.objects.Position
    """

    def __init__(self, contract: Contract, position: float):
        self.contract = contract
        # We don't allow a position with no shares.
        dbg.dassert_ne(0, position)
        self.position = position

    @staticmethod
    def update(lhs:"Position", rhs: "Position") -> Optional["Position"]:
        """
        Update the position `lhs` using another position `rhs`.
        """
        dbg.dassert_eq(lhs.contract, rhs.contract)
        position = lhs.position + rhs.position
        if position == 0:
            return None
        return Position(lhs.contract, position)

    def __repr__(self):
        ret = []
        ret.append("contract=%s" % self.contract)
        ret.append("position=%s" % self.position)
        ret = "\n".join(ret)
        ret = "Position:\n" + prn.indent(ret, 2)
        return ret

    def __key(self):
        return (self.contract, self.position)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Position):
            return self.__key() == other.__key()
        return NotImplemented


# #############################################################################

class OrderStatus:

    def __init__(self, order_id:int , status: str, filled: float, remaining: float, avg_fill_price: float):
        # Pointer to the corresponding Order.
        dbg.dassert_lte(0, order_id)
        self.order_id = order_id
        self.status = status
        # How many shares are filled.
        dbg.dassert_lte(0, filled)
        self.filled = filled
        # How many shares were not filled.
        dbg.dassert_lte(0, remaining)
        self.remaining = remaining
        dbg.dassert_lte(0, avg_fill_price)
        self.avg_fill_price = avg_fill_price

    def __repr__(self):
        return "OrderStatus: order_id=%s, status=%s, filled=%s, remaining=%s avg_fill_price=%s" % (
            self.order_id, self.status, self.filled, self.remaining, self.avg_fill_price
        )


class Trade:
    """
    Keep track of an order, its status, and its fills.

    Modelled after:
    https://ib-insync.readthedocs.io/api.html#ib_insync.order.Trade
    """

    def __init__(self, contract: Contract, order: Order, order_status: OrderStatus, timestamp: Optional[pd.Timestamp]=None):
        self.contract = contract
        self.order = order
        dbg.dassert_lte(order_status.filled, order.total_quantity,
                        msg="Can't fill more than what was requested")
        dbg.dassert_eq(order.total_quantity, order_status.filled + order_status.remaining,
            msg="The filled and remaining shares must be the same as the total quantity")
        self.order_status = order_status
        self.timestamp = timestamp # TODO(gp): Implement fills.

    def to_position(self) -> Position:
        return Position(self.contract, self.order_status.filled)

    def __repr__(self):
        ret = []
        ret.append("contract=%s" % self.contract)
        ret.append("order=%s" % self.order)
        ret.append("order_status=%s" % self.order_status)
        ret.append("timestamp=%s" % self.timestamp)
        ret = "\n".join(ret)
        ret = "Trade:\n" + prn.indent(ret, 2)
        return ret


# #############################################################################
    
# TODO(gp): Consider extending to support more accounts.
class OMS:
    """
    Order management system.

    It is a singleton.

    Modelled after:
    https://ib-insync.readthedocs.io/api.html#module-ib_insync.ib
    """
    def __init__(self):
        self._trades = []
        self._orders = []
        #
        self._current_positions: Dict[Contract, Position] = {}

    def get_current_positions(self) -> Dict[Contract, Position]:
        return self._current_positions.copy()
    
    def __repr__(self):
        def _to_string(prefix, objs) -> str:
            ret = "%s=%d" % (prefix, len(objs))
            if objs:
                ret += "\n" + prn.indent("\n".join(map(str, objs)), 2)
            return ret
        ret = []
        ret.append(_to_string("trades", self.trades))
        ret.append(_to_string("orders", self.orders))
        ret.append(_to_string("positions", sorted(self.positions)))
        #
        ret = "\n".join(ret)
        ret = "OMS:\n" + prn.indent(ret, 2)
        return ret

    # TODO(gp): To be implemented.
    def pnl(self):
        pass

    def place_order(self, contract: Contract, order: Order, timestamp: Optional[pd.Timestamp]=None) -> Trade:
        self.orders.append(order)
        # Assume that everything is filled.
        # TODO(gp): Here we can implement market impact and incomplete fills.
        status = "filled"
        filled = order.total_quantity
        remaining = 0.0
        # TODO(gp): Implement this by talking to IM.
        avg_fill_price = 1000.0
        order_status = OrderStatus(order.order_id, status, filled, remaining,
                avg_fill_price)
        trade = Trade(contract, order, order_status, timestamp=timestamp)
        self.trades.append(trade)
        #
        self._update_positions(trade)
        return trade

    def _update_positions(self, trade: Trade):
        """
        Update the current position given the executed trade.
        """
        dbg.dassert_eq(len(set(self.positions)), len(self.positions),
                       msg="All positions should be about different Contracts")
        # Look for the contract corresponding to `trade` among the current positions.
        contract = self.positions.get(trade.contract, None)
        if contract is None:
            _LOG.debug("Adding new contract: %s", contract)
            position = Position(trade.contract, trade.order.total_quantity)
        else:
            position = Position.update(self.positions[contract], trade.to_position())
        _LOG.debug("position=%s", position)
        # Update the contract.
        if position is None:
            if contract in self.positions:
                _LOG.debug("Removing %s from %s", contract, self.positions)
                del self.positions[contract]
        else:
            _LOG.debug("Updating %s to %s", self.positions.get(contract, None), position)
            self.positions[contract] = position
