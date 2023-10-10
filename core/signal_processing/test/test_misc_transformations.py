import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as carsigen
import core.signal_processing.misc_transformations as csprmitr
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_symmetric_equisized_bins(hunitest.TestCase):
    def test_zero_in_bin_interior_false(self) -> None:
        input_ = pd.Series([-1, 3])
        expected = np.array([-3, -2, -1, 0, 1, 2, 3])
        actual = csprmitr.get_symmetric_equisized_bins(input_, 1)
        np.testing.assert_array_equal(actual, expected)

    def test_zero_in_bin_interior_true(self) -> None:
        input_ = pd.Series([-1, 3])
        expected = np.array([-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5])
        actual = csprmitr.get_symmetric_equisized_bins(input_, 1, True)
        np.testing.assert_array_equal(actual, expected)

    def test_infs(self) -> None:
        data = pd.Series([-1, np.inf, -np.inf, 3])
        expected = np.array([-4, -2, 0, 2, 4])
        actual = csprmitr.get_symmetric_equisized_bins(data, 2)
        np.testing.assert_array_equal(actual, expected)


class Test_digitize1(hunitest.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        bins = [0, 0.2, 0.4]
        right = False
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = csprmitr.digitize(signal, bins, right)
        self.check_string(actual.to_string())

    def test_heaviside1(self) -> None:
        heaviside = carsigen.get_heaviside(-10, 20, 1, 1)
        bins = [0, 0.2, 0.4]
        right = False
        actual = csprmitr.digitize(heaviside, bins, right)
        self.check_string(actual.to_string())


class Test_compute_weighted_sum1(hunitest.TestCase):
    @staticmethod
    def get_test_df() -> pd.DataFrame:
        df = pd.DataFrame(
            [[1, -1, np.nan], [0, 1, 1], [-1, -1, -1]],
            [0, 1, 2],
            ["col1", "col2", 3],
        )
        return df

    def test1(self) -> None:
        df = Test_compute_weighted_sum1.get_test_df()
        weights = pd.Series([1, 1, 0], ["col1", "col2", 3], name="test_weights")
        actual = csprmitr.compute_weighted_sum(df, weights)
        actual_str = hpandas.df_to_str(actual)
        expected_str = r"""
        test_weights
0           0.0
1           1.0
2          -2.0"""
        self.assert_equal(actual_str, expected_str, fuzzy_match=True)
