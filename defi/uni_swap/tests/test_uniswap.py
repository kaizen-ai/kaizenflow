import defi.uni_swap.uni_swap as duswunsw

import helpers.hunit_test as hunitest

class TestUniswapV1(hunitest.TestCase):
    def test_add_liquidity1(self) -> None:
        """ 
        Adding liquidity and verifying it's correct
        """
        uniswap_pool = Uniswap(90, 10)
        uniswap_pool.add_liquidity(10, 5)
        expected_token_amount = 100
        expected_eth_amount = 15        
        self.assertEqual(expected_token_amount, uniswap_pool.token_reserve)
        self.assertEqual(expected_eth_amount, uniswap_pool.eth_reserve)

    def test_add_liquidity2(self) -> None:
        """
        Adding liquidity with decimals
        """
        uniswap_pool = Uniswap(5.3, 9.1)
        uniswap_pool.add_liquidity(50, 0)
        uniswap_pool.add_liquidity(5, 30)
        expected_token_amount = 60.3
        expected_eth_amount = 39.1 
        self.assertEqual(expected_token_amount, uniswap_pool.token_reserve)
        self.assertEqual(expected_eth_amount, uniswap_pool.eth_reserve)

    def test_get_amount1(self) -> None:
        """
        Getting amounts from a balanced pool
        """
        uniswap_pool = Uniswap(100, 100)
        actual_token_amount = uniswap_pool.get_token_amount(150)
        actual_token_amount = uniswap_pool.get_eth_amount(25)   
        expected_token_amount = 60 
        expected_eth_amount = 20
        self.assertEqual(expected_token_amount, actual_token_amount)
        self.assertEqual(expected_eth_amount, actual_eth_amount) 

    def test_get_amount2(self) -> None:
        """
        Getting amounts after adding more liquidity to pool
        """
        uniswap_pool = Uniswap(70, 1)
        uniswap_pool.add_liquidity(20, 0) 
        actual_token_amount = uniswap_pool.get_token_amount(7)
        actual_eth_amount = uniswap_pool.get_eth_amount(10)
        expected_token_amount = 78.75
        expected_eth_amount = 0.1
        self.assertEqual(expected_token_amount, actual_token_amount)
        self.assertEqual(expected_eth_amount, actual_eth_amount)