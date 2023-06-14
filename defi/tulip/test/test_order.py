import numpy as np

import defi.tulip.implementation.order as dtuimord
import helpers.hunit_test as hunitest
from defi.tulip.implementation.order import execute_order


class TestOrderExecute(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the buy order is correctly executed.
        """
        # Create a random order which can be done from implementation in order.py.
        random_order = dtuimord.get_random_order()
        # Change the limit price.
        random_order.limit_price = float(np.random.randint(2, 10))
        # Propose an order price such that it is less than the limit price.
        proposed_price = random_order.limit_price - 1
        # Make sure that the order action is buy type.
        random_order.action = "buy"
        # Execute the order and get results.
        result = execute_order(random_order, proposed_price)
        expected_result = [
            (-random_order.quantity * proposed_price, random_order.quote_token),
            (random_order.quantity, random_order.base_token),
        ]
        self.assertEqual(result, expected_result)

    def test2(self) -> None:
        """
        Check that the buy order is not executed.
        """
        # Create a random order which can be done from implementation in order.py.
        random_order = dtuimord.get_random_order()
        # Change the limit price.
        random_order.limit_price = float(np.random.randint(2, 10))
        # Propose an order price such that it is less than the limit price.
        proposed_price = random_order.limit_price + 1
        # Make sure that the order is buy type.
        random_order.action = "buy"
        # Execute the order and get results.
        result = execute_order(random_order, proposed_price)
        expected_result = None
        self.assertEqual(result, expected_result)

    def test3(self) -> None:
        """
        Check that the sell order is not executed.
        """
        # Create a random order which can be done from implementation in order.py.
        random_order = dtuimord.get_random_order()
        # Change the limit price.
        random_order.limit_price = float(np.random.randint(2, 10))
        # Propose an order price such that it is less than the limit price.
        proposed_price = random_order.limit_price - 1
        # Make sure that the order action is buy type.
        random_order.action = "sell"
        # Execute the order and get results.
        result = execute_order(random_order, proposed_price)
        expected_result = None
        self.assertEqual(result, expected_result)

    def test4(self) -> None:
        """
        Check that the sell order is correctly executed.
        """
        # Create a random order which can be done from implementation in order.py.
        random_order = dtuimord.get_random_order()
        # Change the limit price.
        random_order.limit_price = float(np.random.randint(2, 10))
        # Propose an order price such that it is less than the limit price.
        proposed_price = random_order.limit_price + 1
        # Make sure that the order is buy type.
        random_order.action = "sell"
        # Execute the order and get results.
        result = execute_order(random_order, proposed_price)
        expected_result = [
            (random_order.quantity * proposed_price, random_order.quote_token),
            (random_order.quantity, random_order.base_token),
        ]
        self.assertEqual(result, expected_result)
