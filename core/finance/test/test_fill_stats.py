import logging
from typing import List
import numpy as np
import pandas as pd
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import pytz
import core.finance.target_position_df_processing.fill_stats as cftpdpfst

_LOG = logging.getLogger(__name__)

class Test_compute_fill_stats(hunitest.TestCase):
    def create_sample_target_position_df(self) -> pd.DataFrame:
        data = {
            ("holdings_notional", 1): [1000, 1200, 1100, 1300, 1400],
            ("holdings_notional", 2): [800, 850, 870, 900, 950],
            ("holdings_shares", 1): [10, 12, 11, 13, 14],
            ("holdings_shares", 2): [8, 9, 8.5, 9.5, 10],
            ("price", 1): [100, 105, 102, 108, 110],
            ("price", 2): [50, 52, 51, 53, 54],
            ("target_holdings_notional", 1): [950, 1150, 1050, 1250, 1350],
            ("target_holdings_notional", 2): [780, 830, 850, 880, 930],
            ("target_holdings_shares", 1): [9, 11, 10, 12, 13],
            ("target_holdings_shares", 2): [7.5, 8.5, 8.2, 9.2, 9.5],
            ("target_trades_shares", 1): [1, 2, -1, 2, 1],
            ("target_trades_shares", 2): [0.5, 1, -0.3, 0.7, 0.5],
        }

        index = pd.date_range(start="2023-01-01", periods=5, freq="T", tz=pytz.UTC)
        columns = pd.MultiIndex.from_tuples(data.keys())
        return pd.DataFrame(data, index=index, columns=columns)

    def test_compute_fill_stats1(self) -> None:
        """
        Test basic functionality of compute_fill_stats.
        """
        target_position_df = self.create_sample_target_position_df()
        result_df = cftpdpfst.compute_fill_stats(target_position_df)
        # Ensure the result is a DataFrame
        self.assertIsInstance(result_df, pd.DataFrame)
        # Check that the result DataFrame has the expected columns
        expected_columns = [
            "executed_trades_shares",
            "fill_rate",
            "underfill_share_count",
            "underfill_notional",
            "underfill_opportunity_cost_realized_notional",
            "underfill_opportunity_cost_notional",
            "tracking_error_shares",
            "tracking_error_notional",
            "tracking_error_bps",
            "is_buy",
            "is_sell",
            "is_benchmark_profitable",
        ]
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        # Ensure the result DataFrame has the same index as the input
        self.assertTrue(result_df.index.equals(target_position_df.index))

    def test_compute_fill_stats2(self) -> None:
        """
        Test DataFrame index consistency in compute_fill_stats.
        """
        target_position_df = self.create_sample_target_position_df()
        result_df = cftpdpfst.compute_fill_stats(target_position_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertTrue(result_df.index.equals(target_position_df.index))

    def test_compute_fill_stats3(self) -> None:
        """
        Test specific values for fill_rate in compute_fill_stats.
        """
        target_position_df = self.create_sample_target_position_df()
        fills_df = cftpdpfst.compute_fill_stats(target_position_df)
        self.assertFalse(fills_df.empty)
        # Check specific columns for expected values
        expected_fill_rate = [
            [np.nan, np.nan],
            [2.0, 2.0],
            [0.5, 0.5],
            [2.0, 3.333333],
            [0.5, 0.714286],
        ]
        np.testing.assert_allclose(
            fills_df[("fill_rate", 1)].values, [row[0] for row in expected_fill_rate], rtol=1e-5, atol=1e-8, equal_nan=True
        )
        np.testing.assert_allclose(
            fills_df[("fill_rate", 2)].values, [row[1] for row in expected_fill_rate], rtol=1e-5, atol=1e-8, equal_nan=True
        )
        self.assertEqual(fills_df.shape, (5, 24))

    def test_compute_fill_stats4(self) -> None:
        """
        Test 'is_buy' and 'is_sell' columns in compute_fill_stats.
        """
        target_position_df = self.create_sample_target_position_df()
        result_df = cftpdpfst.compute_fill_stats(target_position_df)

        expected_is_buy_1 = [True, True, False, True, True]
        expected_is_buy_2 = [True, True, False, True, True]
        expected_is_sell_1 = [False, False, True, False, False]
        expected_is_sell_2 = [False, False, True, False, False]

        np.testing.assert_array_equal(result_df[("is_buy", 1)].values, expected_is_buy_1)
        np.testing.assert_array_equal(result_df[("is_buy", 2)].values, expected_is_buy_2)
        np.testing.assert_array_equal(result_df[("is_sell", 1)].values, expected_is_sell_1)
        np.testing.assert_array_equal(result_df[("is_sell", 2)].values, expected_is_sell_2)

        self.assertEqual(result_df.shape, (5, 24))
