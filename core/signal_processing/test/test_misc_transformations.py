import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as carsigen
import core.signal_processing.misc_transformations as csprmitr
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_sign_normalize(hunitest.TestCase):
    """
    Check that signal values are normalized according to sign correctly.
    """

    def test1(self) -> None:
        """
        - input signal is `pd.Series`
        - atol = 0
        """
        signal = pd.Series([-5, -1, 0, 1, 5])
        res = csprmitr.sign_normalize(signal)
        actual_signature = hpandas.df_to_str(res)
        expected_signature = r"""
           0
        0 -1
        1 -1
        2  0
        3  1
        4  1
        """
        self.assert_equal(actual_signature, expected_signature, fuzzy_match=True)

    def test2(self) -> None:
        """
        - input signal is `pd.Series`
        - atol = 2
        """
        signal = pd.Series([-3, -2, 0, 2, 3])
        atol = 2
        res = csprmitr.sign_normalize(signal, atol)
        actual_signature = hpandas.df_to_str(res)
        expected_signature = r"""
           0
        0 -1
        1 -1
        2  0
        3  1
        4  1
        """
        self.assert_equal(actual_signature, expected_signature, fuzzy_match=True)

    def test3(self) -> None:
        """
        - input signal is `pd.DataFrame`
        - atol = 0
        """
        signal = pd.DataFrame([-5, -1, 0, 1, 5])
        actual = csprmitr.sign_normalize(signal)
        expected = pd.DataFrame([-1, -1, 0, 1, 1])
        self.assertTrue(actual.equals(expected))

    def test4(self) -> None:
        """
        - input signal is `pd.DataFrame`
        - atol = 2
        """
        signal = pd.DataFrame([-3, -2, 0, 2, 3])
        atol = 2
        actual = csprmitr.sign_normalize(signal, atol)
        expected = pd.DataFrame([-1, -1, 0, 1, 1])
        self.assertTrue(actual.equals(expected))


class Test_compress_tails(hunitest.TestCase):

    def test1(self) -> None:
        """
        Check that an input with valid scale processed correctly.
        """
        signal = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        scale = 2
        actual = csprmitr.compress_tails(signal, scale = scale)
        actual = hpandas.df_to_str(actual)
        expected = r"""          A         B
        0  0.924234  1.928055
        1  1.523188  1.973229
        2  1.810297  1.990110
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that an input with valid rescale processed correctly.
        """
        signal = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        rescale = 10
        actual = csprmitr.compress_tails(signal, rescale = rescale)
        actual = hpandas.df_to_str(actual)
        expected = r"""     A    B
        0  1.0  1.0
        1  1.0  1.0
        2  1.0  1.0
        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test3(self) -> None:
        """
        Check that an input with valid scale and rescale processed correctly.
        """
        signal = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        rescale = 4
        scale = 0.5
        actual = csprmitr.compress_tails(signal, scale = scale, rescale = rescale)
        actual = hpandas.df_to_str(actual)
        expected = r"""     A    B
        0  0.5  0.5
        1  0.5  0.5
        2  0.5  0.5
        """
        self.assert_equal(actual, expected, fuzzy_match=True)
    
    def test4(self) -> None:
        """
        Check that an empty input is processed correctly.
        """
        signal = pd.Series([])
        result_empty_series = csprmitr.compress_tails(signal)
        actual = str(True)
        expected = str(result_empty_series.empty)
        self.assert_equal(actual, expected)

    def test5(self) -> None:
        """
        Check that an error is raised if scale is lower than 0.
        """
        signal = pd.Series([1, 2, 3, 4, 5])
        scale = -1
        with self.assertRaises(AssertionError) as cm:
            csprmitr.compress_tails(signal, scale=scale)
        actual = str(cm.exception)
        expected = r"""
        ################################################################################
        * Failed assertion *
        0 < -1
        ################################################################################
        """
        self.assert_equal(
            actual, expected, fuzzy_match=True
        )

    def test6(self) -> None:
        """
        Check that an error is raised if rescale is lower than 0.
        """
        signal = pd.DataFrame({"A": [1, 2, 3, 4, 5], "B": [1, 2, 3, 4, 5]})
        rescale = -1
        with self.assertRaises(AssertionError) as cm:
            csprmitr.compress_tails(signal, rescale=rescale)
        actual = str(cm.exception)
        expected = r"""
        ################################################################################
        * Failed assertion *
        0 < -1
        ################################################################################
        """
        self.assert_equal(
            actual, expected, fuzzy_match=True
        )

    def test7(self) -> None:
        """
        Check that an error is raised if input contains non-numeric values.
        """
        signal = pd.DataFrame({"A": ["x", "y", "z"], "B": [1, 2, 3]})
        with self.assertRaises(TypeError) as cm:
            csprmitr.compress_tails(signal)
        actual = str(cm.exception)
        expected = (
            "unsupported operand type(s) for /: 'str' and 'int'"
        )
        self.assert_equal(
            actual, expected, fuzzy_match=True
        )

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
