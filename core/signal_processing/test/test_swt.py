import logging
from typing import Optional

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as carsigen
import core.signal_processing.swt as csiprswt
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_swt(hunitest.TestCase):
    def helper(
        self,
        *,
        timing_mode: Optional[str] = None,
        output_mode: Optional[str] = None,
    ) -> str:
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(
            series,
            timing_mode=timing_mode,
            output_mode=output_mode,
        )
        if isinstance(actual, tuple):
            smooth_df_string = hpandas.df_to_str(
                actual[0],
                num_rows=None,
            )
            detail_df_string = hpandas.df_to_str(
                actual[1],
                num_rows=None,
            )
            output_str = (
                f"smooth_df:\n{smooth_df_string}\n"
                f"\ndetail_df\n{detail_df_string}\n"
            )
        else:
            output_str = hpandas.df_to_str(actual, num_rows=None)
        return output_str

    def test_clean1(self) -> None:
        """
        Test for default values.
        """
        output_str = self.helper()
        self.check_string(output_str)

    def test_timing_mode1(self) -> None:
        """
        Test for timing_mode="knowledge_time".
        """
        timing_mode = "knowledge_time"
        output_str = self.helper(timing_mode=timing_mode)
        self.check_string(output_str)

    def test_timing_mode2(self) -> None:
        """
        Test for timing_mode="zero_phase".
        """
        timing_mode = "zero_phase"
        output_str = self.helper(timing_mode=timing_mode)
        self.check_string(output_str)

    def test_timing_mode3(self) -> None:
        """
        Test for timing_mode="raw".
        """
        timing_mode = "raw"
        output_str = self.helper(timing_mode=timing_mode)
        self.check_string(output_str)

    def test_output_mode1(self) -> None:
        """
        Test for output_mode="tuple".
        """
        output_mode = "tuple"
        output_str = self.helper(output_mode=output_mode)
        self.check_string(output_str)

    def test_output_mode2(self) -> None:
        """
        Test for output_mode="smooth".
        """
        output_mode = "smooth"
        output_str = self.helper(output_mode=output_mode)
        self.check_string(output_str)

    def test_output_mode3(self) -> None:
        """
        Test for output_mode="detail".
        """
        output_mode = "detail"
        output_str = self.helper(output_mode=output_mode)
        self.check_string(output_str)

    def test_depth(self) -> None:
        """
        Test for sufficient input data length given `depth`.
        """
        series = self._get_series(seed=1, periods=10)
        # The test should not raise on this call.
        csiprswt.get_swt(series, depth=2, output_mode="detail")
        with pytest.raises(ValueError):
            # The raise comes from the `get_swt` implementation.
            csiprswt.get_swt(series, depth=3, output_mode="detail")
        with pytest.raises(ValueError):
            # This raise comes from `pywt`.
            csiprswt.get_swt(series, depth=5, output_mode="detail")

    @staticmethod
    def _get_series(seed: int, periods: int = 20) -> pd.Series:
        arma_process = carsigen.ArmaProcess([0], [0])
        date_range = {"start": "1/1/2010", "periods": periods, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        )
        return series


def get_daily_gaussian(seed: int) -> pd.Series:
    process = carsigen.ArmaProcess([], [])
    realization = process.generate_sample(
        {"start": "2000-01-01", "end": "2005-01-01", "freq": "B"}, seed=seed
    )
    return realization


class Test_compute_swt_var(hunitest.TestCase):
    def test_num_non_nan(self) -> None:
        """
        Verify the number of non-NaN data points to be used for the variance.
        """
        srs = get_daily_gaussian(seed=0)
        swt_var = csiprswt.compute_swt_var(srs, depth=6)
        actual = swt_var.count().values[0]
        np.testing.assert_equal(actual, 1179)

    def test_variance_preservation1(self) -> None:
        """
        Verify variance is preserved across time.
        """
        srs = get_daily_gaussian(seed=0)
        swt_var = csiprswt.compute_swt_var(srs, depth=6)
        actual = swt_var.sum()
        np.testing.assert_allclose(actual, [1102.66], atol=0.01)

    def test_variance_preservation2(self) -> None:
        """
        Verify level variance is preserved in sum.
        """
        srs = get_daily_gaussian(seed=0)
        swt_var = csiprswt.compute_swt_var(srs, depth=6, axis=1)
        actual = swt_var.sum()
        np.testing.assert_allclose(actual, [1102.66], atol=0.01)


class Test_compute_swt_var_summary(hunitest.TestCase):
    def test1(self) -> None:
        srs = get_daily_gaussian(seed=0)
        swt_var_summary = csiprswt.compute_swt_var_summary(srs, depth=6)
        actual = hpandas.df_to_str(
            swt_var_summary,
            num_rows=None,
        )
        expected = r"""
    swt_var  cum_swt_var      perc  cum_perc
1  0.492558     0.492558  0.517159  0.517159
2  0.234882     0.727440  0.246613  0.763773
3  0.116992     0.844432  0.122835  0.886608
4  0.049948     0.894380  0.052443  0.939051
5  0.024626     0.919006  0.025856  0.964907
6  0.016248     0.935254  0.017060  0.981966
"""
        self.assert_equal(actual, expected, fuzzy_match=True)


class Test_get_knowledge_time_warmup_lengths(hunitest.TestCase):
    def helper(self, wavelets, depth, expected) -> None:
        lengths = csiprswt.get_knowledge_time_warmup_lengths(wavelets, depth)
        actual = hpandas.df_to_str(
            lengths,
            num_rows=None,
        )
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test1(self) -> None:
        wavelets = [
            "bior1.3",
            "coif1",
            "db2",
            "dmey",
            "haar",
            "rbio1.3",
            "sym3",
        ]
        depth = 10
        expected = r"""
wavelet  bior1.3  coif1   db2   dmey  haar  rbio1.3  sym3
level
1              6      6     4     62     2        6     6
2             18     18    12    186     6       18    18
3             42     42    28    434    14       42    42
4             90     90    60    930    30       90    90
5            186    186   124   1922    62      186   186
6            378    378   252   3906   126      378   378
7            762    762   508   7874   254      762   762
8           1530   1530  1020  15810   510     1530  1530
9           3066   3066  2044  31682  1022     3066  3066
10          6138   6138  4092  63426  2046     6138  6138
"""
        self.helper(wavelets, depth, expected)


class Test_summarize_discrete_wavelets(hunitest.TestCase):
    def test1(self) -> None:
        df = csiprswt.summarize_discrete_wavelets()
        actual = hpandas.df_to_str(df, num_rows=None)
        self.check_string(actual)