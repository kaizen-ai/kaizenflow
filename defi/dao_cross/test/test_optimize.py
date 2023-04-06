import logging
import pprint
from typing import List

import defi.dao_cross.optimize as ddacropt
import defi.dao_cross.order as ddacrord
import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# TODO(Grisha): split into multiple test classes and
# add more unit tests.
class TestRunSolver1(hunitest.TestCase):
    """
    Run the solver using toy orders.
    """

    @staticmethod
    def get_test_btc_orders(
        limit_prices: List[float],
    ) -> List[ddacrord.Order]:
        """
        Get toy BTC orders for the unit tests.

        :param limit_prices: limit in prices: quote token per base token
        :return: buy and sell orders with the base token "BTC"
        """
        hdbg.dassert_eq(len(limit_prices), 4)
        # Set the common variables.
        base_token = "BTC"
        quote_token = "ETH"
        buy_action = "buy"
        sell_action = "sell"
        deposit_address = 1
        wallet_address = 1
        # Genereate buy orders.
        quantity = 4
        order_0 = ddacrord.Order(
            base_token,
            quote_token,
            buy_action,
            quantity,
            limit_prices[0],
            deposit_address,
            wallet_address,
        )
        #
        quantity = 5
        order_1 = ddacrord.Order(
            base_token,
            quote_token,
            buy_action,
            quantity,
            limit_prices[1],
            deposit_address,
            wallet_address,
        )
        # Genereate sell orders.
        quantity = 6
        order_2 = ddacrord.Order(
            base_token,
            quote_token,
            sell_action,
            quantity,
            limit_prices[2],
            deposit_address,
            wallet_address,
        )
        #
        quantity = 2
        order_3 = ddacrord.Order(
            base_token,
            quote_token,
            sell_action,
            quantity,
            limit_prices[3],
            deposit_address,
            wallet_address,
        )
        orders = [order_0, order_1, order_2, order_3]
        return orders

    @staticmethod
    def get_test_eth_orders(
        limit_prices: List[float],
    ) -> List[ddacrord.Order]:
        """
        Get toy ETH orders for the unit tests.

        :param limit_prices: limit in prices: quote token per base token
        :return: buy and sell orders with the base token "ETH"
        """
        hdbg.dassert_eq(len(limit_prices), 4)
        # Set the common variables.
        base_token = "ETH"
        quote_token = "BTC"
        buy_action = "buy"
        sell_action = "sell"
        deposit_address = 1
        wallet_address = 1
        # Genereate buy orders.
        quantity = 1
        order_0 = ddacrord.Order(
            base_token,
            quote_token,
            buy_action,
            quantity,
            limit_prices[0],
            deposit_address,
            wallet_address,
        )
        #
        quantity = 9
        order_1 = ddacrord.Order(
            base_token,
            quote_token,
            buy_action,
            quantity,
            limit_prices[1],
            deposit_address,
            wallet_address,
        )
        # Genereate sell orders.
        quantity = 8
        order_2 = ddacrord.Order(
            base_token,
            quote_token,
            sell_action,
            quantity,
            limit_prices[2],
            deposit_address,
            wallet_address,
        )
        #
        quantity = 6
        order_3 = ddacrord.Order(
            base_token,
            quote_token,
            sell_action,
            quantity,
            limit_prices[3],
            deposit_address,
            wallet_address,
        )
        orders = [order_0, order_1, order_2, order_3]
        return orders

    def test1(self) -> None:
        """
        The limit price condition is True for all orders.
        """
        # All the limit prices >= clearing price for the buy orders
        # <= clearing price for the sell orders.
        btc_limit_prices = [5, 5.5, 3, 2]
        btc_orders = self.get_test_btc_orders(btc_limit_prices)
        eth_limit_prices = [0.3, 0.43, 0.1, 0.2]
        eth_orders = self.get_test_eth_orders(eth_limit_prices)
        # 
        orders = btc_orders + eth_orders
        prices = {"BTC": 2, "ETH": 8}
        result = ddacropt.run_solver(orders, prices)
        # Check that the solution is found and is different from zero.
        self.assertEqual(result["problem_objective_value"], 192)
        # Check the executed quantity values.
        var_values_str = pprint.pformat(result["q_base_asterisk"])
        exp = r"""
        [3.0, 5.0, 6.0, 2.0, 1.0, 9.0, 8.0, 2.0]
        """
        self.assert_equal(var_values_str, exp, fuzzy_match=True)

    def test2(self) -> None:
        """
        The limit price condition is False for a buy order.
        """
        prices = {"BTC": 2, "ETH": 8}
        btc_limit_prices = [5, 2, 3, 2]
        btc_orders = self.get_test_btc_orders(btc_limit_prices)
        result = ddacropt.run_solver(btc_orders, prices)
        # Check that the solution is found but it equals zero.
        self.assertEqual(result["problem_objective_value"], 16)
        # Check the executed quantity values.
        var_values_str = pprint.pformat(result["q_base_asterisk"])
        exp = r"""
        [4.0, 0.0, 4.0, 0.0]
        """
        self.assert_equal(var_values_str, exp, fuzzy_match=True)

    def test3(self) -> None:
        """
        The limit price condition is False for a sell order.
        """
        prices = {"BTC": 2, "ETH": 8}
        btc_limit_prices = [5, 5.5, 3, 6]
        btc_orders = self.get_test_btc_orders(btc_limit_prices)
        result = ddacropt.run_solver(btc_orders, prices)
        # Check that the solution is found but it equals zero.
        self.assertEqual(result["problem_objective_value"], 24)
        # Check the executed quantity values.
        var_values_str = pprint.pformat(result["q_base_asterisk"])
        exp = r"""
        [1.0, 5.0, 6.0, 0.0]
        """
        self.assert_equal(var_values_str, exp, fuzzy_match=True)
