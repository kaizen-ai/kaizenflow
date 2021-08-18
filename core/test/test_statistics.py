"""
Import as:

import core.test.test_statistics as cttsta
"""

import logging
from typing import List

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as casgen
import core.finance as cfinan
import core.signal_processing as csproc
import core.statistics as cstati
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestComputeMoments(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_moments(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_moments(series, prefix="moments_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        cstati.compute_moments(series)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = cstati.compute_moments(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = cstati.compute_moments(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test6(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        cstati.compute_moments(series)

    def test7(self) -> None:
        """
        Test series with `inf`.
        """
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[4] = np.inf
        actual = cstati.compute_moments(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestComputeFracZero(hut.TestCase):
    def test1(self) -> None:
        data = [0.466667, 0.2, 0.13333, 0.2, 0.33333]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = cstati.compute_frac_zero(self._get_df(seed=1))
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test2(self) -> None:
        data = [
            0.4,
            0.0,
            0.2,
            0.4,
            0.4,
            0.2,
            0.4,
            0.0,
            0.6,
            0.4,
            0.6,
            0.2,
            0.0,
            0.0,
            0.2,
        ]
        index = pd.date_range(start="1-04-2018", periods=15, freq="30T")
        expected = pd.Series(data=data, index=index)
        actual = cstati.compute_frac_zero(self._get_df(seed=1), axis=1)
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test3(self) -> None:
        # Equals 20 / 75 = num_zeros / num_points.
        expected = 0.266666
        actual = cstati.compute_frac_zero(self._get_df(seed=1), axis=None)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test4(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.466667
        actual = cstati.compute_frac_zero(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test5(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.466667
        actual = cstati.compute_frac_zero(series, axis=0)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        cstati.compute_frac_zero(series)

    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        nrows = 15
        ncols = 5
        num_nans = 15
        num_infs = 5
        num_zeros = 20
        #
        np.random.seed(seed=seed)
        mat = np.random.randn(nrows, ncols)
        mat.ravel()[np.random.choice(mat.size, num_nans, replace=False)] = np.nan
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = np.inf
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = -np.inf
        mat.ravel()[np.random.choice(mat.size, num_zeros, replace=False)] = 0
        #
        index = pd.date_range(start="01-04-2018", periods=nrows, freq="30T")
        df = pd.DataFrame(data=mat, index=index)
        return df


class TestComputeFracNan(hut.TestCase):
    def test1(self) -> None:
        data = [0.4, 0.133333, 0.133333, 0.133333, 0.2]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = cstati.compute_frac_nan(self._get_df(seed=1))
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test2(self) -> None:
        data = [
            0.4,
            0.0,
            0.2,
            0.4,
            0.2,
            0.2,
            0.2,
            0.0,
            0.4,
            0.2,
            0.6,
            0.0,
            0.0,
            0.0,
            0.2,
        ]
        index = pd.date_range(start="1-04-2018", periods=15, freq="30T")
        expected = pd.Series(data=data, index=index)
        actual = cstati.compute_frac_nan(self._get_df(seed=1), axis=1)
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test3(self) -> None:
        # Equals 15 / 75 = num_nans / num_points.
        expected = 0.2
        actual = cstati.compute_frac_nan(self._get_df(seed=1), axis=None)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test4(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.4
        actual = cstati.compute_frac_nan(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test5(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.4
        actual = cstati.compute_frac_nan(series, axis=0)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        cstati.compute_frac_nan(series)

    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        nrows = 15
        ncols = 5
        num_nans = 15
        num_infs = 5
        num_zeros = 20
        #
        np.random.seed(seed=seed)
        mat = np.random.randn(nrows, ncols)
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = np.inf
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = -np.inf
        mat.ravel()[np.random.choice(mat.size, num_zeros, replace=False)] = 0
        mat.ravel()[np.random.choice(mat.size, num_nans, replace=False)] = np.nan
        #
        index = pd.date_range(start="01-04-2018", periods=nrows, freq="30T")
        df = pd.DataFrame(data=mat, index=index)
        return df


class TestComputeNumFiniteSamples(hut.TestCase):
    @staticmethod
    # Smoke test for empty input.
    def test1() -> None:
        series = pd.Series([])
        cstati.count_num_finite_samples(series)


class TestComputeNumUniqueValues(hut.TestCase):
    @staticmethod
    # Smoke test for empty input.
    def test1() -> None:
        series = pd.Series([])
        cstati.count_num_unique_values(series)


class TestComputeDenominatorAndPackage(hut.TestCase):
    @staticmethod
    # Smoke test for empty input.
    def test1() -> None:
        series = pd.Series([])
        cstati._compute_denominator_and_package(reduction=1, data=series)


class TestTTest1samp(hut.TestCase):

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        cstati.ttest_1samp(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = cstati.ttest_1samp(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = cstati.ttest_1samp(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test4(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        cstati.ttest_1samp(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestMultipleTests(hut.TestCase):

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        cstati.multipletests(series)

    # Test if error is raised with default arguments when input contains NaNs.
    @pytest.mark.xfail()
    def test2(self) -> None:
        series_with_nans = self._get_series(seed=1)
        series_with_nans[0:5] = np.nan
        actual = cstati.multipletests(series_with_nans)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series_with_nans = self._get_series(seed=1)
        series_with_nans[0:5] = np.nan
        actual = cstati.multipletests(series_with_nans, nan_mode="drop")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = hut.get_random_df(
            num_cols=1,
            seed=seed,
            date_range_kwargs=date_range,
        )[0]
        return series


class TestMultiTTest(hut.TestCase):

    # Smoke test for empty input.
    def test1(self) -> None:
        df = pd.DataFrame(columns=["series_name"])
        cstati.multi_ttest(df)

    def test2(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = cstati.multi_ttest(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = cstati.multi_ttest(df, prefix="multi_ttest_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = cstati.multi_ttest(df, popmean=1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = cstati.multi_ttest(df, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = cstati.multi_ttest(df, method="sidak")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @pytest.mark.xfail()
    def test7(self) -> None:
        df = self._get_df_of_series(seed=1)
        df.iloc[:, 0] = np.nan
        actual = cstati.multi_ttest(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @staticmethod
    def _get_df_of_series(seed: int) -> pd.DataFrame:
        n_series = 7
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        # Generating a dataframe from different series.
        df = pd.DataFrame(
            [
                arma_process.generate_sample(
                    date_range_kwargs=date_range, seed=seed + i
                )
                for i in range(n_series)
            ],
            index=["series_" + str(i) for i in range(n_series)],
        ).T
        return df


class TestApplyNormalityTest(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_normality_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_normality_test(series, prefix="norm_test_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        cstati.apply_normality_test(series)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = cstati.apply_normality_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = cstati.apply_normality_test(
            series, nan_mode="ffill_and_drop_leading"
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test6(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        cstati.apply_normality_test(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestApplyAdfTest(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_adf_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_adf_test(series, regression="ctt")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_adf_test(series, maxlag=5)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_adf_test(series, autolag="t-stat")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_adf_test(series, prefix="adf_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        cstati.apply_adf_test(series)

    def test7(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = cstati.apply_adf_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test8(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        cstati.apply_adf_test(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestApplyKpssTest(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_kpss_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_kpss_test(series, regression="ct")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_kpss_test(series, nlags="auto")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_kpss_test(series, nlags=5)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_kpss_test(series, prefix="kpss_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        cstati.apply_kpss_test(series)

    def test7(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = cstati.apply_kpss_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test8(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        cstati.apply_kpss_test(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestApplyLjungBoxTest(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_ljung_box_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_ljung_box_test(series, lags=3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_ljung_box_test(series, model_df=3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_ljung_box_test(series, period=5)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_ljung_box_test(series, prefix="lb_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.apply_ljung_box_test(series, return_df=False)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test7(self) -> None:
        series = pd.Series([])
        cstati.apply_ljung_box_test(series)

    def test8(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = cstati.apply_ljung_box_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test9(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        cstati.apply_ljung_box_test(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestComputeSpecialValueStats(hut.TestCase):
    def test1(self) -> None:
        """
        Test for default arguments.
        """
        series = self._get_messy_series(seed=1)
        actual = cstati.compute_special_value_stats(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for prefix.
        """
        series = self._get_messy_series(seed=1)
        actual = cstati.compute_special_value_stats(series, prefix="data_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        cstati.compute_special_value_stats(series)

    @staticmethod
    def _get_messy_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        series[:5] = 0
        series[-5:] = np.nan
        series[10:13] = np.inf
        series[13:16] = -np.inf
        return series


class TestCalculateHitRate(hut.TestCase):
    def test1(self) -> None:
        """
        Test for default parameters.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             55.5556
        hit_rate_97.50%CI_lower_bound_(%)  25.4094
        hit_rate_97.50%CI_upper_bound_(%)  82.7032
        """
        series = self._get_test_series()
        actual = cstati.calculate_hit_rate(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for the case when NaNs compose the half of the input.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             55.5556
        hit_rate_97.50%CI_lower_bound_(%)  25.4094
        hit_rate_97.50%CI_upper_bound_(%)  82.7032
        """
        series = self._get_test_series()
        nan_series = pd.Series([np.nan for i in range(len(series))])
        series = pd.concat([series, nan_series])
        actual = cstati.calculate_hit_rate(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        """
        Test for the case when np.inf compose the half of the input.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             55.5556
        hit_rate_97.50%CI_lower_bound_(%)  25.4094
        hit_rate_97.50%CI_upper_bound_(%)  82.7032
        """
        series = self._get_test_series()
        inf_series = pd.Series([np.inf for i in range(len(series))])
        inf_series[:5] = -np.inf
        series = pd.concat([series, inf_series])
        actual = cstati.calculate_hit_rate(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        """
        Test for the case when 0 compose the half of the input.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             55.5556
        hit_rate_97.50%CI_lower_bound_(%)  25.4094
        hit_rate_97.50%CI_upper_bound_(%)  82.7032
        """
        series = self._get_test_series()
        zero_series = pd.Series([0 for i in range(len(series))])
        series = pd.concat([series, zero_series])
        actual = cstati.calculate_hit_rate(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        """
        Test threshold.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             57.1429
        hit_rate_97.50%CI_lower_bound_(%)  23.4501
        hit_rate_97.50%CI_upper_bound_(%)  86.1136
        """
        series = self._get_test_series()
        actual = cstati.calculate_hit_rate(series, threshold=10e-3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        """
        Test alpha.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             55.5556
        hit_rate_95.00%CI_lower_bound_(%)  29.6768
        hit_rate_95.00%CI_upper_bound_(%)  79.1316
        """
        series = self._get_test_series()
        actual = cstati.calculate_hit_rate(series, alpha=0.1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test7(self) -> None:
        """
        Test prefix.

        Expected outcome:                                              0
        hit_hit_rate_point_est_(%)             55.5556
        hit_hit_rate_97.50%CI_lower_bound_(%)  25.4094
        hit_hit_rate_97.50%CI_upper_bound_(%)  82.7032
        """
        series = self._get_test_series()
        actual = cstati.calculate_hit_rate(series, prefix="hit_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test8(self) -> None:
        """
        Test method.

        Expected outcome:                                          0
        hit_rate_point_est_(%)             55.5556
        hit_rate_97.50%CI_lower_bound_(%)  26.6651
        hit_rate_97.50%CI_upper_bound_(%)  81.1221
        """
        series = self._get_test_series()
        actual = cstati.calculate_hit_rate(series, method="wilson")
        self.check_string(hut.convert_df_to_string(actual, index=True))

    # Smoke test for empty input.
    def test_smoke(self) -> None:
        series = pd.Series([])
        cstati.calculate_hit_rate(series)

    # Smoke test for input of `np.nan`s.
    def test_nan(self) -> None:
        series = pd.Series([np.nan] * 10)
        cstati.calculate_hit_rate(series)

    @staticmethod
    def _get_test_series() -> pd.Series:
        series = pd.Series([0, -0.001, 0.001, -0.01, 0.01, -0.1, 0.1, -1, 1, 10])
        return series


class Test_compute_jensen_ratio(hut.TestCase):
    def test1(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_jensen_ratio(
            signal,
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_jensen_ratio(
            signal,
            p_norm=3,
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        signal = self._get_signal(seed=1)
        signal[5:8] = np.inf
        actual = cstati.compute_jensen_ratio(
            signal,
            inf_mode="drop",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_jensen_ratio(
            signal,
            nan_mode="ffill",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_jensen_ratio(
            signal,
            prefix="commodity_",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test6(self) -> None:
        signal = pd.Series([])
        cstati.compute_jensen_ratio(signal)

    @staticmethod
    def _get_signal(seed: int) -> pd.Series:
        np.random.seed(seed)
        n = 1000
        signal = pd.Series(np.random.randn(n))
        signal[30:50] = np.nan
        return signal


class Test_compute_t_distribution_j_2(hut.TestCase):
    def test_almost_normal(self) -> None:
        actual = cstati.compute_t_distribution_j_2(200)
        np.testing.assert_allclose(actual, np.sqrt(2 / np.pi), atol=1e-2)

    def test_4dof(self) -> None:
        actual = cstati.compute_t_distribution_j_2(4)
        np.testing.assert_allclose(actual, 0.707107, atol=1e-5)

    def test_2dof(self) -> None:
        actual = cstati.compute_t_distribution_j_2(2)
        np.testing.assert_allclose(actual, 0)


class Test_compute_hill_number(hut.TestCase):
    def test_equally_distributed1(self) -> None:
        length = 10
        data = pd.Series(index=range(0, length), data=1)
        actual = cstati.compute_hill_number(data, 1)
        np.testing.assert_allclose(actual, 10)

    def test_equally_distributed2(self) -> None:
        length = 10
        data = pd.Series(index=range(0, length), data=1)
        actual = cstati.compute_hill_number(data, 2)
        np.testing.assert_allclose(actual, 10)

    def test_equally_distributed3(self) -> None:
        length = 10
        data = pd.Series(index=range(0, length), data=1)
        actual = cstati.compute_hill_number(data, np.inf)
        np.testing.assert_allclose(actual, 10)

    def test_scale_invariance1(self) -> None:
        length = 32
        np.random.seed(137)
        data = pd.Series(data=np.random.rand(length, 1).flatten())
        actual_1 = cstati.compute_hill_number(data, 2)
        actual_2 = cstati.compute_hill_number(7 * data, 2)
        np.testing.assert_allclose(actual_1, actual_2)

    def test_exponentially_distributed1(self) -> None:
        data = pd.Series([2 ** j for j in range(10, 0, -1)])
        actual = cstati.compute_hill_number(data, 1)
        np.testing.assert_allclose(actual, 3.969109, atol=1e-5)

    def test_exponentially_distributed2(self) -> None:
        data = pd.Series([2 ** j for j in range(10, 0, -1)])
        actual = cstati.compute_hill_number(data, 2)
        np.testing.assert_allclose(actual, 2.994146, atol=1e-5)

    def test_exponentially_distributed3(self) -> None:
        data = pd.Series([2 ** j for j in range(10, 0, -1)])
        actual = cstati.compute_hill_number(data, np.inf)
        np.testing.assert_allclose(actual, 1.998047, atol=1e-5)


class Test_compute_forecastability(hut.TestCase):
    def test1(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_forecastability(
            signal,
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_forecastability(
            signal,
            mode="periodogram",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_forecastability(
            signal,
            nan_mode="ffill_and_drop_leading",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        signal = self._get_signal(seed=1)
        actual = cstati.compute_forecastability(
            signal,
            prefix="commodity_",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test5(self) -> None:
        signal = self._get_signal(seed=1)
        cstati.compute_forecastability(signal)

    @staticmethod
    def _get_signal(seed: int) -> pd.Series:
        np.random.seed(seed)
        n = 1000
        signal = pd.Series(np.random.randn(n))
        signal[30:50] = np.nan
        return signal


class Test_compute_annualized_return_and_volatility(hut.TestCase):
    def test1(self) -> None:
        """
        Test for default parameters.
        """
        series = self._get_series(seed=1)
        actual = cstati.compute_annualized_return_and_volatility(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test prefix.
        """
        series = self._get_series(seed=1)
        actual = cstati.compute_annualized_return_and_volatility(
            series, prefix="test_"
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test3(self) -> None:
        series = pd.Series([])
        cstati.compute_annualized_return_and_volatility(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = casgen.ArmaProcess([0], [0])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        )
        return series


class TestComputeMaxDrawdown(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_max_drawdown(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_max_drawdown(series, prefix="new_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        """
        Smoke test for empty input.
        """
        series = pd.Series([])
        cstati.compute_max_drawdown(series)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = casgen.ArmaProcess([0], [0])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        )
        return series


class Test_compute_bet_stats(hut.TestCase):
    def test1(self) -> None:
        log_rets = Test_compute_bet_stats._get_series(42)
        positions = csproc.compute_smooth_moving_average(log_rets, 4)
        actual = cstati.compute_bet_stats(positions, log_rets)
        bet_rets = cfinan.compute_returns_per_bet(positions, log_rets)
        rets_pos_bet_rets = pd.concat(
            {"pos": positions, "rets": log_rets, "bet_rets": bet_rets}, axis=1
        )
        self._check_results(rets_pos_bet_rets, actual)

    def test2(self) -> None:
        log_rets = Test_compute_bet_stats._get_series(42)
        positions = csproc.compute_smooth_moving_average(log_rets, 4)
        log_rets.iloc[:10] = np.nan
        actual = cstati.compute_bet_stats(positions, log_rets)
        bet_rets = cfinan.compute_returns_per_bet(positions, log_rets)
        rets_pos_bet_rets = pd.concat(
            {"pos": positions, "rets": log_rets, "bet_rets": bet_rets}, axis=1
        )
        self._check_results(rets_pos_bet_rets, actual)

    def test3(self) -> None:
        idx = pd.date_range("2010-12-29", freq="D", periods=8)
        log_rets = pd.Series([1, 2, 3, 5, 7, 11, -13, -5], index=idx)
        positions = pd.Series([1, 2, 0, 1, -3, -2, 0, -1], index=idx)
        actual = cstati.compute_bet_stats(positions, log_rets)
        bet_rets = cfinan.compute_returns_per_bet(positions, log_rets)
        rets_pos_bet_rets = pd.concat(
            {"pos": positions, "rets": log_rets, "bet_rets": bet_rets}, axis=1
        )
        self._check_results(rets_pos_bet_rets, actual)

    def _check_results(
        self, rets_pos_bet_rets: pd.DataFrame, actual: pd.Series
    ) -> None:
        act = []
        act.append(hprint.frame("rets_pos"))
        act.append(
            hut.convert_df_to_string(rets_pos_bet_rets, index=True, decimals=3)
        )
        act.append(hprint.frame("stats"))
        act.append(hut.convert_df_to_string(actual, index=True, decimals=3))
        act = "\n".join(act)
        self.check_string(act, fuzzy_match=True)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = casgen.ArmaProcess([], [])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed, scale=0.1
        )
        return series


class Test_compute_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        ar_params: List[float] = []
        ma_params: List[float] = []
        arma_process = casgen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        sr = cstati.compute_sharpe_ratio(realization)
        np.testing.assert_almost_equal(sr, 0.057670899)


class Test_compute_sharpe_ratio_standard_error(hut.TestCase):
    def test1(self) -> None:
        ar_params: List[float] = []
        ma_params: List[float] = []
        arma_process = casgen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        sr_se = cstati.compute_sharpe_ratio_standard_error(realization)
        np.testing.assert_almost_equal(sr_se, 0.160261242)


class Test_compute_annualized_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        """
        Demonstrate the approximate invariance of the annualized SR (at least
        for iid returns) under resampling.
        """
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Calculate SR from minutely time series.
        srs_sr = cstati.compute_annualized_sharpe_ratio(srs)
        np.testing.assert_almost_equal(srs_sr, -2.6182, decimal=3)
        # Resample to hourly and calculate SR.
        hourly_srs = csproc.resample(srs, rule="60T").sum()
        hourly_sr = cstati.compute_annualized_sharpe_ratio(hourly_srs)
        np.testing.assert_almost_equal(hourly_sr, -2.6483, decimal=3)
        # Resample to daily and calculate SR.
        daily_srs = csproc.resample(srs, rule="D").sum()
        daily_srs_sr = cstati.compute_annualized_sharpe_ratio(daily_srs)
        np.testing.assert_almost_equal(daily_srs_sr, -2.4890, decimal=3)
        # Resample to weekly and calculate SR.
        weekly_srs = csproc.resample(srs, rule="W").sum()
        weekly_srs_sr = cstati.compute_annualized_sharpe_ratio(weekly_srs)
        np.testing.assert_almost_equal(weekly_srs_sr, -2.7717, decimal=3)

    def test2(self) -> None:
        """
        Demonstrate the approximate invariance of the annualized SR in moving
        from a "uniform" ATH-only time grid to a truly uniform time grid.
        """
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Filter out non-trading time points.
        filtered_srs = cfinan.set_non_ath_to_nan(srs)
        filtered_srs = cfinan.set_weekends_to_nan(filtered_srs)
        filtered_srs = filtered_srs.dropna()
        # Treat srs as an intraday trading day-only series, e.g.,
        # approximately 252 trading days per year, ATH only.
        n_samples = filtered_srs.size
        points_per_year = 2.52 * n_samples
        filtered_srs_sr = cstati.compute_sharpe_ratio(
            filtered_srs, time_scaling=points_per_year
        )
        np.testing.assert_almost_equal(filtered_srs_sr, -2.7203, decimal=3)
        # Compare to SR annualized using `freq`.
        srs_sr = cstati.compute_annualized_sharpe_ratio(srs)
        np.testing.assert_almost_equal(srs_sr, -2.6182, decimal=3)

    def test3(self) -> None:
        """
        Test for pd.DataFrame input with NaN values.
        """
        srs = self._generate_minutely_series(n_days=1, seed=1)[:40]
        srs[5:10] = np.nan
        df = pd.DataFrame([srs, srs.shift()]).T
        df.columns = ["Series 1", "Series 2"]
        actual = cstati.compute_annualized_sharpe_ratio(df)
        df_string = hut.convert_df_to_string(df, index=True)
        actual_string = hut.convert_df_to_string(actual, index=True)
        txt = f"Input:\n{df_string}\n\n" f"Output:\n{actual_string}\n"
        self.check_string(txt)

    def _generate_minutely_series(self, n_days: float, seed: int) -> pd.Series:
        arma_process = casgen.ArmaProcess([], [])
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": n_days * 24 * 60, "freq": "T"},
            scale=1,
            seed=seed,
        )
        return realization


class Test_compute_annualized_sharpe_ratio_standard_error(hut.TestCase):
    def test1(self) -> None:
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Calculate SR from minutely time series.
        srs_sr_se = cstati.compute_annualized_sharpe_ratio_standard_error(srs)
        np.testing.assert_almost_equal(srs_sr_se, 1.9108, decimal=3)
        # Resample to hourly and calculate SR.
        hourly_srs = csproc.resample(srs, rule="60T").sum()
        hourly_sr_se = cstati.compute_annualized_sharpe_ratio_standard_error(
            hourly_srs
        )
        np.testing.assert_almost_equal(hourly_sr_se, 1.9116, decimal=3)
        # Resample to daily and calculate SR.
        daily_srs = csproc.resample(srs, rule="D").sum()
        daily_sr_se_sr = cstati.compute_annualized_sharpe_ratio_standard_error(
            daily_srs
        )
        np.testing.assert_almost_equal(daily_sr_se_sr, 1.9192, decimal=3)
        # Resample to weekly and calculate SR.
        weekly_srs = csproc.resample(srs, rule="W").sum()
        weekly_sr_se_sr = cstati.compute_annualized_sharpe_ratio_standard_error(
            weekly_srs
        )
        np.testing.assert_almost_equal(weekly_sr_se_sr, 2.0016, decimal=3)

    def test2(self) -> None:
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Filter out non-trading time points.
        filtered_srs = cfinan.set_non_ath_to_nan(srs)
        filtered_srs = cfinan.set_weekends_to_nan(filtered_srs)
        filtered_srs = filtered_srs.dropna()
        # Treat srs as an intraday trading day-only series, e.g.,
        # approximately 252 trading days per year, ATH only.
        n_samples = filtered_srs.size
        points_per_year = 2.52 * n_samples
        filtered_srs_se = cstati.compute_sharpe_ratio_standard_error(
            filtered_srs, time_scaling=points_per_year
        )
        np.testing.assert_almost_equal(filtered_srs_se, 1.5875, decimal=3)
        # Compare to SR annualized using `freq`.
        srs_sr_se = cstati.compute_annualized_sharpe_ratio_standard_error(srs)
        np.testing.assert_almost_equal(srs_sr_se, 1.9108, decimal=3)

    def _generate_minutely_series(self, n_days: float, seed: int) -> pd.Series:
        arma_process = casgen.ArmaProcess([], [])
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": n_days * 24 * 60, "freq": "T"},
            scale=1,
            seed=seed,
        )
        return realization


class Test_summarize_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        ar_params: List[float] = []
        ma_params: List[float] = []
        arma_process = casgen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        res = cstati.summarize_sharpe_ratio(realization)
        self.check_string(hut.convert_df_to_string(res, index=True))


class Test_zscore_oos_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        oos_start = "2010-02-25"
        sr_stats = cstati.zscore_oos_sharpe_ratio(series, oos_start)
        output_str = (
            f"OOS start: {oos_start}\n"
            f"{hprint.frame('input series')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(sr_stats, index=True)}"
        )
        self.check_string(output_str)

    def test2(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        oos_start = "2010-04-10"
        sr_stats = cstati.zscore_oos_sharpe_ratio(series, oos_start)
        output_str = (
            f"OOS start: {oos_start}\n"
            f"{hprint.frame('input series')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(sr_stats, index=True)}"
        )
        self.check_string(output_str)

    def test3(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        oos_start = "2010-02-25"
        series.loc[oos_start:] = series.loc[oos_start:].apply(  # type: ignore
            lambda x: 0.1 * x if x > 0 else x
        )
        sr_stats = cstati.zscore_oos_sharpe_ratio(series, oos_start)
        output_str = (
            f"OOS start: {oos_start}\n"
            f"{hprint.frame('input series')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(sr_stats, index=True)}"
        )
        self.check_string(output_str)

    def test4(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        oos_start = "2010-02-25"
        series.loc[oos_start:] = series.loc[oos_start:].apply(  # type: ignore
            lambda x: 0.1 * x if x < 0 else x
        )
        sr_stats = cstati.zscore_oos_sharpe_ratio(series, oos_start)
        output_str = (
            f"OOS start: {oos_start}\n"
            f"{hprint.frame('input series')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(sr_stats, index=True)}"
        )
        self.check_string(output_str)

    def test_nans1(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        series.iloc[:10] = np.nan
        series.iloc[40:50] = np.nan
        oos_start = "2010-02-25"
        series.loc[oos_start:"2010-03-03"] = np.nan  # type: ignore
        series.loc["2010-04-01":"2010-04-30"] = np.nan  # type: ignore
        sr_stats = cstati.zscore_oos_sharpe_ratio(series, oos_start)
        output_str = (
            f"OOS start: {oos_start}\n"
            f"{hprint.frame('input series')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(sr_stats, index=True)}"
        )
        self.check_string(output_str)

    def test_zeros1(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        series.iloc[:10] = 0
        series.iloc[40:50] = 0
        oos_start = "2010-02-25"
        series.loc[oos_start:"2010-03-03"] = np.nan  # type: ignore
        series.loc["2010-04-01":"2010-04-30"] = np.nan  # type: ignore
        sr_stats = cstati.zscore_oos_sharpe_ratio(series, oos_start)
        output_str = (
            f"OOS start: {oos_start}\n"
            f"{hprint.frame('input series')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(sr_stats, index=True)}"
        )
        self.check_string(output_str)

    def test_oos_not_from_interval1(self) -> None:
        series = Test_zscore_oos_sharpe_ratio._get_series(42)
        with self.assertRaises(AssertionError):
            _ = cstati.zscore_oos_sharpe_ratio(series, "2012-01-01")

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = casgen.ArmaProcess([], [])
        date_range = {"start": "2010-01-01", "periods": 100, "freq": "B"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed, scale=0.1
        )
        return series


class Test_sharpe_ratio_correlation_conversion(hut.TestCase):
    def test1(self) -> None:
        actual = cstati.apply_sharpe_ratio_correlation_conversion(
            points_per_year=252 * 78, sharpe_ratio=3
        )
        np.testing.assert_allclose(actual, 0.0214, atol=0.0001)

    def test2(self) -> None:
        actual = cstati.apply_sharpe_ratio_correlation_conversion(
            points_per_year=252 * 78, correlation=0.0214
        )
        np.testing.assert_allclose(actual, 3, atol=0.01)


class Test_compute_hit_rate_implied_by_correlation(hut.TestCase):
    def zero_corr(self) -> None:
        actual = cstati.compute_hit_rate_implied_by_correlation(0)
        np.testing.assert_allclose(actual, 0.5)

    def one_percent_corr(self) -> None:
        actual = cstati.compute_hit_rate_implied_by_correlation(0.01)
        np.testing.assert_allclose(actual, 0.5049999)

    def heavy_tails(self) -> None:
        actual = cstati.compute_hit_rate_implied_by_correlation(0.01, 0.6)
        np.testing.assert_allclose(actual, 0.5066487)


class Test_compute_correlation_implied_by_hit_rate(hut.TestCase):
    def zero_hit_rate(self) -> None:
        actual = cstati.compute_correlation_implied_by_hit_rate(0)
        np.testing.assert_allclose(actual, 0)

    def small_edge_to_one_percent_corr(self) -> None:
        actual = cstati.compute_correlation_implied_by_hit_rate(0.5049999)
        np.testing.assert_allclose(actual, 0.0100001)

    def heavy_tails(self) -> None:
        actual = cstati.compute_correlation_implied_by_hit_rate(0.5049999, 0.6)
        np.testing.assert_allclose(actual, 0.0075120)

    def fifty_one_percent(self) -> None:
        actual = cstati.compute_correlation_implied_by_hit_rate(0.51)
        np.testing.assert_allclose(actual, 0.0200002)


class Test_compute_drawdown_cdf(hut.TestCase):
    def test1(self) -> None:
        sharpe_ratio = 1
        volatility = 0.15
        drawdown = 0.05
        time = 1
        probability = cstati.compute_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            drawdown=drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probability, 0.52500, decimal=3)

    def test2(self) -> None:
        sharpe_ratio = 3
        volatility = 0.15
        drawdown = 0.05
        time = 1
        probalility = cstati.compute_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            drawdown=drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.86475, decimal=3)

    def test3(self) -> None:
        sharpe_ratio = 3
        volatility = 0.15
        drawdown = 0.05
        time = 10
        probalility = cstati.compute_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            drawdown=drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.86466, decimal=3)


class Test_compute_normalized_drawdown_cdf(hut.TestCase):
    def test1(self) -> None:
        sharpe_ratio = 1
        normalized_drawdown = 0.5
        time = 1
        probalility = cstati.compute_normalized_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            normalized_drawdown=normalized_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.67881, decimal=3)

    def test2(self) -> None:
        sharpe_ratio = 3
        normalized_drawdown = 1
        time = 1
        probalility = cstati.compute_normalized_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            normalized_drawdown=normalized_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.99754, decimal=3)


class Test_compute_max_drawdown_approximate_cdf(hut.TestCase):
    def test1(self) -> None:
        sharpe_ratio = 1
        volatility = 0.15
        max_drawdown = 0.05
        time = 1
        probalility = cstati.compute_max_drawdown_approximate_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            max_drawdown=max_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.59844, decimal=3)

    def test2(self) -> None:
        sharpe_ratio = 3
        volatility = 0.15
        max_drawdown = 0.05
        time = 1
        probalility = cstati.compute_max_drawdown_approximate_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            max_drawdown=max_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.87342, decimal=3)

    def test3(self) -> None:
        sharpe_ratio = 3
        volatility = 0.15
        max_drawdown = 0.05
        time = 10
        probalility = cstati.compute_max_drawdown_approximate_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            max_drawdown=max_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.25837, decimal=3)


class TestComputeZeroDiffProportion(hut.TestCase):
    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_zero_diff_proportion(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_zero_diff_proportion(series, atol=1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_zero_diff_proportion(series, rtol=0.3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_zero_diff_proportion(
            series, nan_mode="fill_with_zero"
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_zero_diff_proportion(series, prefix="prefix_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test7(self) -> None:
        series = pd.Series([])
        cstati.compute_zero_diff_proportion(series)

    def test8(self) -> None:
        """
        Test constant series with `NaN`s in between, default `nan_mode`.
        """
        series = pd.Series(
            [1, np.nan, 1, np.nan],
            index=pd.date_range(start="2010-01-01", periods=4),
        )
        actual = cstati.compute_zero_diff_proportion(series)
        output_str = (
            f"{hprint.frame('input')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame('output')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test9(self) -> None:
        """
        Test constant series with `NaN`s in between, "drop".
        """
        series = pd.Series(
            [1, np.nan, 1, np.nan],
            index=pd.date_range(start="2010-01-01", periods=4),
        )
        nan_mode = "drop"
        actual = cstati.compute_zero_diff_proportion(series, nan_mode=nan_mode)
        output_str = (
            f"{hprint.frame('input')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame(f'output, `nan_mode`=`{nan_mode}`')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test10(self) -> None:
        """
        Test constant series with `NaN`s in between for "ffill".
        """
        series = pd.Series(
            [1, np.nan, np.nan, 1, np.nan],
            index=pd.date_range(start="2010-01-01", periods=5),
        )
        nan_mode = "ffill"
        actual = cstati.compute_zero_diff_proportion(series, nan_mode=nan_mode)
        output_str = (
            f"{hprint.frame('input')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame(f'output, `nan_mode`=`{nan_mode}`')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test11(self) -> None:
        """
        Test series with consecutive `NaN`s.
        """
        series = pd.Series(
            [1, np.nan, np.nan, 2],
            index=pd.date_range(start="2010-01-01", periods=4),
        )
        nan_mode = "leave_unchanged"
        actual = cstati.compute_zero_diff_proportion(series, nan_mode=nan_mode)
        output_str = (
            f"{hprint.frame('input')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame(f'output, `nan_mode`=`{nan_mode}`')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    def test12(self) -> None:
        """
        Test series with consecutive `NaN`s, "ffill".
        """
        series = pd.Series(
            [1, np.nan, np.nan, 2],
            index=pd.date_range(start="2010-01-01", periods=4),
        )
        nan_mode = "ffill"
        actual = cstati.compute_zero_diff_proportion(series, nan_mode=nan_mode)
        output_str = (
            f"{hprint.frame('input')}\n"
            f"{hut.convert_df_to_string(series, index=True)}\n"
            f"{hprint.frame(f'output, `nan_mode`=`{nan_mode}`')}\n"
            f"{hut.convert_df_to_string(actual, index=True)}"
        )
        self.check_string(output_str)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        np.random.seed(seed=seed)
        n_elements = 100
        index = pd.date_range(start="1-04-2018", periods=n_elements, freq="D")
        data = list(np.random.randint(10, size=n_elements))
        series = pd.Series(data=data, index=index, name="test")
        series[45:47] = np.nan
        series[47:50] = np.inf
        return series


class TestGetInterarrivalTime(hut.TestCase):

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        cstati.get_interarrival_time(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.get_interarrival_time(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.get_interarrival_time(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        n = 100
        start = pd.to_datetime("2015-01-01")
        end = pd.to_datetime("2018-01-01")
        start_u = start.value // 10 ** 9
        end_u = end.value // 10 ** 9
        np.random.seed(seed=seed)
        dates = pd.to_datetime(np.random.randint(start_u, end_u, n), unit="s")
        sorted_dates = dates.sort_values()
        data = list(np.random.randint(10, size=n))
        series = pd.Series(data=data, index=sorted_dates, name="test")
        series[45:50] = np.nan
        return series


class Test_compute_avg_turnover_and_holding_period(hut.TestCase):
    def test1(self) -> None:
        """
        Test for default parameters.
        """
        pos = self._get_pos(seed=1)
        actual = cstati.compute_avg_turnover_and_holding_period(pos)
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for unit.
        """
        pos = self._get_pos(seed=1)
        actual = cstati.compute_avg_turnover_and_holding_period(pos, unit="M")
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test for nan_mode.
        """
        pos = self._get_pos(seed=1)
        pos[5:10] = np.nan
        actual = cstati.compute_avg_turnover_and_holding_period(
            pos, nan_mode="fill_with_zero"
        )
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test4(self) -> None:
        """
        Test for prefix.
        """
        pos = self._get_pos(seed=1)
        actual = cstati.compute_avg_turnover_and_holding_period(
            pos, prefix="test_"
        )
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    @staticmethod
    def _get_pos(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "D"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series


class TestComputeInterarrivalTimeStats(hut.TestCase):

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        cstati.compute_interarrival_time_stats(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_interarrival_time_stats(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = cstati.compute_interarrival_time_stats(series, nan_mode="ffill")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = casgen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        series.drop(series.index[15:20], inplace=True)
        series[5:10] = np.nan
        return series


class Test_summarize_time_index_info(hut.TestCase):
    def test1(self) -> None:
        """
        Test for the case when index freq is not None.
        """
        series = self._get_series(seed=1)
        actual = cstati.summarize_time_index_info(series)
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for the case when index freq is None.
        """
        series = self._get_series(seed=1)
        series = series.drop(series.index[1:3])
        actual = cstati.summarize_time_index_info(series)
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test3(self) -> None:
        """
        Test for default nan_mode.
        """
        series = self._get_series(seed=1)
        series[0] = np.nan
        series[-1] = np.nan
        series[5:25] = np.nan
        actual = cstati.summarize_time_index_info(series)
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test4(self) -> None:
        """
        Test for specified nan_mode.
        """
        series = self._get_series(seed=1)
        series[0] = np.nan
        series[-1] = np.nan
        series[5:25] = np.nan
        actual = cstati.summarize_time_index_info(
            series, nan_mode="fill_with_zero"
        )
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test5(self) -> None:
        """
        Test for prefix.
        """
        series = self._get_series(seed=1)
        actual = cstati.summarize_time_index_info(series, prefix="test_")
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test6(self) -> None:
        """
        Test `NaN` series.
        """
        date_range_kwargs = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = pd.Series(np.nan, index=pd.date_range(**date_range_kwargs))
        actual = cstati.summarize_time_index_info(series)
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    def test7(self) -> None:
        """
        Test empty series.
        """
        series = pd.Series(index=pd.DatetimeIndex([]))
        actual = cstati.summarize_time_index_info(series)
        actual_string = hut.convert_df_to_string(actual, index=True, decimals=3)
        self.check_string(actual_string, fuzzy_match=True)

    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = hut.get_random_df(
            num_cols=1,
            seed=seed,
            date_range_kwargs=date_range,
        )[0]
        return series


class TestComputeRegressionCoefficients1(hut.TestCase):
    def test1(self) -> None:
        mvnp = casgen.MultivariateNormalProcess()
        mvnp.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mvnp.generate_sample(
            date_range_kwargs={"start": "2010-01-04", "periods": 40, "freq": "B"},
            seed=20,
        )
        actual = cstati.compute_regression_coefficients(
            df,
            x_cols=list(range(0, 9)),
            y_col=9,
        )
        actual_string = hut.convert_df_to_string(
            actual.round(3), index=True, decimals=3
        )
        self.check_string(actual_string, fuzzy_match=True)

    def test2(self) -> None:
        cov = pd.DataFrame(
            np.array(
                [
                    [1, 0.05, 0.3, -0.2, 0.1],
                    [0.05, 1, 0, 0, 0],
                    [0.3, 0, 1, 0, 0],
                    [-0.2, 0, 0, 1, 0],
                    [0.1, 0, 0, 0, 1],
                ],
            )
        )
        mvnp = casgen.MultivariateNormalProcess(cov=cov)
        df = mvnp.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=1
        )
        actual = cstati.compute_regression_coefficients(
            df,
            x_cols=list(range(1, 5)),
            y_col=0,
        )
        actual_string = hut.convert_df_to_string(
            actual.round(3), index=True, decimals=3
        )
        self.check_string(actual_string, fuzzy_match=True)
