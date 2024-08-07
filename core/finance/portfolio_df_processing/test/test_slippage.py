from typing import Tuple

import numpy as np
import pandas as pd

import core.finance.portfolio_df_processing.slippage as cfpdprsl
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest


class Test_compute_share_prices_and_slippage(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test the function with:

        - portfolio_df = mock_data,
        - price_df = mock_data,
        - join_output_with_input = False
        """
        # Prepare inputs.
        portfolio_df, price_df = self._get_test_data()
        join_output_with_input = False
        # Test.
        actual_df = cfpdprsl.compute_share_prices_and_slippage(
            portfolio_df,
            join_output_with_input=join_output_with_input,
            price_df=price_df,
        )
        # Check.
        actual = hpandas.df_to_str(actual_df)
        self.check_string(actual, fuzzy_match=True)

    def test2(self) -> None:
        """
        Test the function with:

        - portfolio_df = mock_data,
        - price_df = mock_data,
        - join_output_with_input = True
        """
        # Prepare inputs.
        portfolio_df, price_df = self._get_test_data()
        join_output_with_input = True
        # Test.
        actual_df = cfpdprsl.compute_share_prices_and_slippage(
            portfolio_df,
            join_output_with_input=join_output_with_input,
            price_df=price_df,
        )
        # Check.
        actual = hpandas.df_to_str(actual_df)
        self.check_string(actual, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test the function with:

        - portfolio_df = mock_data,
        - price_df = None,
        - join_output_with_input = False
        """
        # Prepare inputs.
        portfolio_df, _ = self._get_test_data()
        join_output_with_input = False
        price_df = None
        # Test.
        actual_df = cfpdprsl.compute_share_prices_and_slippage(
            portfolio_df,
            join_output_with_input=join_output_with_input,
            price_df=price_df,
        )
        # Check.
        actual = hpandas.df_to_str(actual_df)
        self.check_string(actual, fuzzy_match=True)

    @staticmethod
    def _get_test_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate test input data.

        :return: portfolio and price data
        """
        timestamps = pd.date_range(
            start="2024-05-30 08:00:00-04:00",
            end="2024-05-30 16:00:00-04:00",
            freq="2H",
        )
        # Generate price data.
        prices = [594.39, 596.99, 596.92, 599.59, 597.39]
        price_df = pd.DataFrame(prices, index=timestamps, columns=[1])
        price_df.index.name = "wall_clock_timestamp"
        # Generate portfolio data.
        portfolio_df_cols = [
            ("holdings_shares", 1),
            ("holdings_notional", 1),
            ("executed_trades_shares", 1),
            ("executed_trades_notional", 1),
        ]
        portfolio_df_data = [
            [0.00, -0.17, 0.00, 0.00, 0.00],
            [0.0000, -101.4883, 0.0000, 0.0000, 0.0000],
            [0.00, -0.17, 0.17, 0.00, 0.00],
            [np.nan, -101.0395, 101.5495, 0.0000, 0.0000],
        ]
        portfolio_data = dict(zip(portfolio_df_cols, portfolio_df_data))
        portfolio_df = pd.DataFrame(portfolio_data, index=timestamps)
        return portfolio_df, price_df
