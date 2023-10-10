"""
Import as:

import oms.broker.ccxt.mock_ccxt_exchange as obcmccex
"""

import asyncio
import copy
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import helpers.hdatetime as hdateti

_LOG = logging.getLogger(__name__)

# Represent a deterministic delay or a delay randomly distributed in an interval.
DelayType = Union[float, Tuple[float, float]]

ParamsDict = Dict[str, Union[str, int]]
# For simplicity, say values can be of Any type.
CcxtOrderStructure = Dict[str, Any]


class MockCcxtExchange:
    """
    Class to mock behavior of a CCXT Exchange.

    - This class allows advanced testing of broker behavior.
    - Instead of interacting with real Binance API, the behavior is simulated.

    Invariants in v0.1:
    - Server response delay is simulated as a constant time interval.
    - Orders are filled immediately in full.
    """

    # TODO(Juraj): Make the delay argument more advanced, specify per order etc.).
    def __init__(
        self,
        delay_in_secs: DelayType,
        event_loop: asyncio.AbstractEventLoop,
        get_wall_clock_time: hdateti.GetWallClockTime,
    ):
        """
        Initialize MockCcxtExchange.

        :param delay_in_secs: default delay in responding
        """
        # Needed for assigning correct timestamp while inside solipsism context manager.
        # This is a callable.
        self._get_wall_clock_time = get_wall_clock_time
        self._delay_in_secs = delay_in_secs
        # The following attributes represent a state of the binance account, they act as stubs
        # for data that would otherwise be fetched via Binance API calls
        # TODO(Juraj): It might be beneficial to wrap these into a separate class
        # to encapsulate a state of a binance account, decide based on observed complexity.
        self._total_balance = {}

        # Represent exchange positions, example:
        # [
        #    {"info": {"positionAmt": 2500}, "symbol": "ETH/USDT"},
        #    {"info": {"positionAmt": 1000}, "symbol": "BTC/USDT"},
        # ]
        self._positions = {}
        # Store all orders as a list of CCXT order structures.
        # CCXT order structure is the cornerstone of most order related methods/operations.
        # https:docs.ccxt.com/#/README?id=order-structure
        # The implementation of methods consists of manipulating this structure +
        #  adding artifical waiting times to simulate interaction with a 3rd party server.
        self._orders = []
        # Set-up trivial order ID assigner.
        self._id_counter = None
        # Set event loop in order to simulate passage of time.
        self._event_loop = event_loop

    def fetch_orders(
        self, symbol: str, *, limit: Optional[int] = None
    ) -> List[CcxtOrderStructure]:
        """
        Fetch orders from exchange account.
        
        :param limit: return of to the last limit orders, if None return all
        """
        # It is safer to deepcopy to avoid weird behavior.
        orders = copy.deepcopy(self._orders)
        orders = [o for o in orders if o["symbol"] == symbol]
        if limit:
            # In the simulated logic orders are appended one after another,
            # meaning the newest order is the last in the list.
            adjusted_limit = min(len(orders), limit)
            orders = orders[-adjusted_limit:]
        return orders

    def fetchPositions(
        self, *, symbols: Optional[List[str]] = None
    ) -> Dict[str, Union[str, float]]:
        """
        Fetch positions from exchange account.

        CCXT returns the position structure
        https://docs.ccxt.com/#/README?id=position-structure
        We return only a subset of fields of interest to us.

        :param symbols: specify list of symbols to return positions of, if None then all symbo
        """
        positions = copy.deepcopy(self._positions)
        if symbols:
            positions = [p for p in positions if p["symbol"] in symbols]
        return positions

    def setLeverage(self, leverage: int, symbol: str) -> None:
        """
        Set leverage for a symbol.

        Currently this behavior does not alter outputs of the mock
        exchange, meaning implementation is empty.
        """

    def cancelAllOrders(self, symbol: str) -> List[CcxtOrderStructure]:
        self._simulate_waiting_for_response()
        cancelled_orders = []
        for order in self._orders:
            if order["status"] == "opened":
                order["status"] = "canceled"
                cancelled_orders.append(order)
        return cancelled_orders

    def createLimitBuyOrder(
        self,
        symbol: str,
        amount: float,
        price: float,
        *,
        params: Optional[ParamsDict] = None
    ) -> CcxtOrderStructure:
        """
        Simulate sending a limit buy order:
        """
        return self._create_ccxt_order(
            symbol, amount, price, "buy", params=params
        )

    def createLimitSellOrder(
        self, symbol: str, amount: float, price: float, params: ParamsDict
    ) -> CcxtOrderStructure:
        """
        Simulate sending a limit sell order:
        """
        return self._create_ccxt_order(
            symbol, amount, price, "sell", params=params
        )

    def _create_ccxt_order(
        self,
        symbol: str,
        amount: float,
        price: float,
        side: str,
        *,
        params: Optional[ParamsDict]
    ) -> CcxtOrderStructure:
        """
        Helper function for creating an order.
        """
        self._simulate_waiting_for_response()
        # TODO(Juraj): implement usage of params.
        amount = amount if side == "buy" else -amount
        order = {
            "id": self._generate_ccxt_order_id(),  # string
            # Currently we simulate order being filled immediately meaning its closed.
            "status": "closed",
            "symbol": symbol,
            # TODO(Juraj): Only "limit" is supported for now.
            "type": "limit",
            "side": side,
            "price": price,
            "amount": amount,
            # Assume order is filled immediately in full.
            "filled": amount,
            "remaining": 0.0,
            # Needed when broker calls get_fills().
            "cost": price * amount,
            "info": {
                "updateTime": hdateti.convert_timestamp_to_unix_epoch(
                    self._get_wall_clock_time()
                )
            },
        }
        self._update_position(symbol, amount)
        self._orders.append(order)
        return order

    def _update_position(self, symbol: str, amount: float) -> None:
        for position in self._positions:
            if position["symbol"] == symbol:
                position["info"]["positionAmt"] += amount

    # TODO(Juraj): we might even create a decorator.
    def _simulate_waiting_for_response(self) -> None:
        """
        Sleep for a defined time interval to simulate waiting for server
        response.
        """
        asyncio.run_coroutine_threadsafe(
            asyncio.sleep(self._delay_in_secs), self._event_loop
        )

    def _generate_ccxt_order_id(self) -> str:
        """
        Dummy order ID generator.

        The function simply increases internally stored counter by one
        and returns the value.
        """
        if self._id_counter is not None:
            self._id_counter += 1
        else:
            self._id_counter = 0
        # CCXT returns string id, stick to the interface.
        return str(self._id_counter)


class MockCcxtExchange_withErrors(MockCcxtExchange):
    """
    Class to mock behavior of a CCXT Exchange.

    - This class allows advanced testing of broker behavior.
    - Instead of interacting with real Binance API, the behavior is simulated.

    Invariants in this child class:
    - Calling method which creates order results in raising an exception.
    - The number of consecutive exceptions raised when repeating the method call is
    determined by the parameter set in constructor.
    """

    def __init__(self, num_exceptions: int, *args):
        """
        Initialize MockCcxtExchange_withErrors.

        :param num_exceptions: number of consecutive exceptions to raise when
        repeating a call to any of the order creating methods
        """
        super.__init__(*args)
        self._num_exceptions = num_exceptions
