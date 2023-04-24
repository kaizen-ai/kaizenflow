import defi.uniswap.uniswapy as duswunsw

import helpers.hunit_test as hunitest

class TestSwapCheck1(hunitest.TestCase):
	def test1(self) -> None:
		""" 
		The pool has a normal token distribution.
		"""
		
		liquidity_pool = Uniswap(90, 10)
		liquidity_pool.add_liquidity(10, 0)
		# Check the amounts.
		expected_token_amount = 50
		expected_eth_amount = 5
		self.assertEqual(expected_token_amount, liquidity_pool.get_token_amount(10))
		self.assertEqual(expected_eth_amount, liquidity_pool.get_eth_amount(100))
	
	def test2(self) -> None:
		"""
		The token distribution is equal
		"""
		
		liquidity_pool = Uniswap(100, 100)
		# Check the amounts.
		expected_token_amount = 60 
		expected_eth_amount = 20
		self.assertEqual(expected_token_amount, liquidity_pool.get_token_amount(150))
		self.assertEqual(expected_eth_amount, liquidity_pool.get_eth_amount(25))

