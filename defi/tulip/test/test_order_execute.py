import numpy as np

import defi.tulip.implementation.order as dtuimord
import defi.tulip.implementation.order_execute as dtimorex
import helpers.hunit_test as hunitest


class TestOrderExecute1(hunitest.TestCase):
    """
    Run the order execution test case by generating a random order.
    """

    def test1(self) -> None:
        """
        Create a test case with ETH base and BTC Quote with limit price.
        """
        # Create a random order which can be done from implementation in order.py
        random_order = dtuimord.get_random_order()
        #
        # Check the attributes of the generated order for testing - uncomment below statements
        # logging.info(random_order.base_token)
        # logging.info(random_order.quote_token)
        # logging.info(str(random_order.limit_price))
        # logging.info(str(random_order.quantity))
        #
        # Change the limit price
        random_order.limit_price = float(np.random.randint(2, 10))
        # logging.info(str(random_order.limit_price))
        #
        # Propose an order price such that it is less than the limit price
        proposed_price = random_order.limit_price - 1
        # logging.info(str(proposed_price))
        #
        # Execute the order and get results
        result = dtimorex.execute_order(random_order, proposed_price)
        expected_result = [
            (-random_order.quantity * proposed_price, random_order.quote_token),
            (random_order.quantity, random_order.base_token),
        ]
        self.assertEqual(result, expected_result)

    def test2(self) -> None:
        """
        Create a test case with ETH base and BTC Quote with lesser limit price.
        """
        # Create a random order which can be done from implementation in order.py
        random_order = dtuimord.get_random_order()
        #
        # Check the attributes of the generated order for testing
        # logging.info(random_order.base_token)
        # logging.info(random_order.quote_token)
        # logging.info(str(random_order.limit_price))
        # logging.info(str(random_order.quantity))
        #
        # Change the limit price
        random_order.limit_price = float(np.random.randint(2, 10))
        # logging.info(str(random_order.limit_price))
        #
        # Propose an order price such that it is less than the limit price
        proposed_price = random_order.limit_price + 1
        # logging.info(str(proposed_price))
        #
        # Execute the order and get results
        result = dtimorex.execute_order(random_order, proposed_price)
        expected_result = None
        self.assertEqual(result, expected_result)
