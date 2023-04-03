import pytest
import logging
from typing import Tuple

import helpers.hunit_test as hunitest
import defi.dao_cross.order as ddacrord
import defi.dao_cross.optimize as opt

_LOG = logging.getLogger(__name__)

class TestRunSolver1(hunitest.TestCase):
    """
    Run notebooks without failures.
    """
    @staticmethod
    def get_test_orders(
        limit_price_1: float, limit_price_2: float
    ) -> Tuple[ddacrord.Order, ddacrord.Order]:
        """
        Get toy orders to demonstrate how the solver works.
        
        :param limit_price_1: limit price for the buy order
        :param limit_price_2: limit price for the sell order
        :return: buy and sell orders
        """
        # Set dummy variables.
        base_token = "BTC"
        quote_token = "ETH"
        deposit_address = 1
        wallet_address = 1
        # Genereate buy order.
        action = "buy"
        quantity = 5
        base_token = "BTC"
        quote_token = "ETH"
        order_1 = ddacrord.Order(
            base_token,
            quote_token,
            action,
            quantity,
            limit_price_1,
            deposit_address,
            wallet_address,
        )
        _LOG.debug("Buy order: %s", str(order_1))
        # Generate sell order.
        action = "sell"
        quantity = 6
        base_token = "BTC"
        quote_token = "ETH"
        order_2 = ddacrord.Order(
            base_token,
            quote_token,
            action,
            quantity,
            limit_price_2,
            deposit_address,
            wallet_address,
        )
        _LOG.debug("Sell order: %s", str(order_2))
        return order_1, order_2

    def test1(self) -> None:
        exchange_rate = 4
        limit_price_1 = 5
        limit_price_2 = 3
        test_orders_1 = self.get_test_orders(limit_price_1, limit_price_2)
        res = opt.run_solver(test_orders_1[0], test_orders_1[1], exchange_rate)
        print(res)