import logging
import pprint
from typing import List

import defi.dao_cross.optimize as ddacropt
import defi.dao_cross.order as ddacrord
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestRunSolver1(hunitest.TestCase):
    """
    Run the solver using toy orders.
    """

    @staticmethod
    def get_test_orders(
        limit_price_buy: float,
        limit_price_sell: float,
    ) -> List[ddacrord.Order]:
        """
        Get toy orders for the unit tests.

        :param limit_price_buy: limit price for buy orders
        :param limit_price_sell: limit price for sell orders
        :return: buy and sell orders
        """
        # Set dummy variables.
        base_token = "BTC"
        quote_token = "ETH"
        deposit_address = 1
        wallet_address = 1
        # Genereate buy orders.
        buy_action = "buy"
        # 
        quantity = 4
        order_1 = ddacrord.Order(
            base_token,
            quote_token,
            buy_action,
            quantity,
            limit_price_buy,
            deposit_address,
            wallet_address,
        )
        #
        quantity = 5
        order_2 = ddacrord.Order(
            base_token,
            quote_token,
            buy_action,
            quantity,
            limit_price_buy,
            deposit_address,
            wallet_address,
        )
        # Generate sell orders.
        sell_action = "sell"
        #
        quantity = 6
        order_3 = ddacrord.Order(
            base_token,
            quote_token,
            sell_action,
            quantity,
            limit_price_sell,
            deposit_address,
            wallet_address,
        )
        #
        quantity = 1
        order_4 = ddacrord.Order(
            base_token,
            quote_token,
            sell_action,
            quantity,
            limit_price_sell,
            deposit_address,
            wallet_address,
        )
        orders = [order_1, order_2, order_3, order_4]
        return orders

    def test1(self) -> None:
        """
        The limit price condition is True for all orders.
        """
        exchange_rate = 4
        limit_price_buy = 5
        limit_price_sell = 3
        test_orders = self.get_test_orders(limit_price_buy, limit_price_sell)
        result = ddacropt.run_solver(
            test_orders, exchange_rate
        )
        # Check that the solution is found and is different from zero.
        self.assertEqual(result["problem_objective_value"], 14)
        # Check the executed quantity values.
        var_values_str = pprint.pformat(result["q_base_asterisk"])
        exp = r"""
        [2.0, 5.0, 6.0, 1.0]
        """
        self.assert_equal(var_values_str, exp, fuzzy_match=True)

    def test2(self) -> None:
        """
        The limit price condition is False for a buy order.
        """
        exchange_rate = 4
        limit_price_buy = 3
        limit_price_sell = 3
        test_orders = self.get_test_orders(limit_price_buy, limit_price_sell)
        result = ddacropt.run_solver(
            test_orders, exchange_rate
        )
        # Check that the solution is found but it equals zero.
        self.assertEqual(result["problem_objective_value"], 0)
        # Check the executed quantity values.
        var_values_str = pprint.pformat(result["q_base_asterisk"])
        exp = r"""
        [0.0, 0.0, 0.0, 0.0]
        """
        self.assert_equal(var_values_str, exp, fuzzy_match=True)

    def test3(self) -> None:
        """
        The limit price condition is False for a sell order.
        """
        exchange_rate = 4
        limit_price_buy = 5
        limit_price_sell = 5
        test_orders = self.get_test_orders(limit_price_buy, limit_price_sell)
        result = ddacropt.run_solver(
            test_orders, exchange_rate
        )
        # Check that the solution is found but it equals zero.
        self.assertEqual(result["problem_objective_value"], 0)
        # Check the executed quantity values.
        var_values_str = pprint.pformat(result["q_base_asterisk"])
        exp = r"""
        [0.0, 0.0, 0.0, 0.0]
        """
        self.assert_equal(var_values_str, exp, fuzzy_match=True)
