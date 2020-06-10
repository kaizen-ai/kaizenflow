import logging

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as sig_gen
import core.statistics as stats
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestComputeMoments1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_moments(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_moments(series, prefix="moments_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_moments(series)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.compute_moments(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.compute_moments(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s
    def test6(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.compute_moments(series)


class TestComputeFracZero1(hut.TestCase):
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

    def test1(self) -> None:
        data = [0.466667, 0.2, 0.13333, 0.2, 0.33333]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_zero(self._get_df(seed=1))
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
        actual = stats.compute_frac_zero(self._get_df(seed=1), axis=1)
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test3(self) -> None:
        # Equals 20 / 75 = num_zeros / num_points.
        expected = 0.266666
        actual = stats.compute_frac_zero(self._get_df(seed=1), axis=None)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test4(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.466667
        actual = stats.compute_frac_zero(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test5(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.466667
        actual = stats.compute_frac_zero(series, axis=0)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    # Smoke test for empty input
    def test6(self) -> None:
        series = pd.Series([])
        stats.compute_frac_zero(series)


class TestComputeFracNan1(hut.TestCase):
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

    def test1(self) -> None:
        data = [0.4, 0.133333, 0.133333, 0.133333, 0.2]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_nan(self._get_df(seed=1))
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
        actual = stats.compute_frac_nan(self._get_df(seed=1), axis=1)
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test3(self) -> None:
        # Equals 15 / 75 = num_nans / num_points.
        expected = 0.2
        actual = stats.compute_frac_nan(self._get_df(seed=1), axis=None)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test4(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.4
        actual = stats.compute_frac_nan(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test5(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.4
        actual = stats.compute_frac_nan(series, axis=0)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    # Smoke test for empty input
    def test6(self) -> None:
        series = pd.Series([])
        stats.compute_frac_nan(series)


class TestComputeFracConstant1(hut.TestCase):
    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        nrows = 15
        ncols = 5
        num_nans = 4
        num_infs = 2
        #
        np.random.seed(seed=seed)
        mat = np.random.randint(-1, 1, (nrows, ncols)).astype("float")
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = np.inf
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = -np.inf
        mat.ravel()[np.random.choice(mat.size, num_nans, replace=False)] = np.nan
        #
        index = pd.date_range(start="01-04-2018", periods=nrows, freq="30T")
        df = pd.DataFrame(data=mat, index=index)
        return df

    def test1(self) -> None:
        data = [0.357143, 0.5, 0.285714, 0.285714, 0.071429]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_constant(self._get_df(seed=1))
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test2(self) -> None:
        series = self._get_df(seed=1)[0]
        expected = 0.357143
        actual = stats.compute_frac_constant(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    # Smoke test for empty input
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_frac_constant(series)


class TestComputeNumFiniteSamples1(hut.TestCase):
    @staticmethod
    # Smoke test for empty input
    def test1() -> None:
        series = pd.Series([])
        stats.count_num_finite_samples(series)


class TestComputeNumUniqueValues1(hut.TestCase):
    @staticmethod
    # Smoke test for empty input
    def test1() -> None:
        series = pd.Series([])
        stats.count_num_unique_values(series)


class TestComputeDenominatorAndPackage1(hut.TestCase):
    @staticmethod
    # Smoke test for empty input
    def test1() -> None:
        series = pd.Series([])
        stats._compute_denominator_and_package(reduction=1, data=series)


class TestTTest1samp1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    # Smoke test for empty input
    def test1(self) -> None:
        series = pd.Series([])
        stats.ttest_1samp(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.ttest_1samp(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.ttest_1samp(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s
    def test4(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.ttest_1samp(series)


class TestMultipleTests1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = hut.get_random_df(num_cols=1, seed=seed, **date_range,)[0]
        return series

    # Smoke test for empty input
    def test1(self) -> None:
        series = pd.Series([])
        stats.multipletests(series)

    # Test if error is raised with default arguments when input contains NaNs
    @pytest.mark.xfail()
    def test2(self) -> None:
        series_with_nans = self._get_series(seed=1)
        series_with_nans[0:5] = np.nan
        actual = stats.multipletests(series_with_nans)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series_with_nans = self._get_series(seed=1)
        series_with_nans[0:5] = np.nan
        actual = stats.multipletests(series_with_nans, nan_mode="ignore")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class TestMultiTTest1(hut.TestCase):
    @staticmethod
    def _get_df_of_series(seed: int) -> pd.DataFrame:
        n_series = 7
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
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

    # Smoke test for empty input
    def test1(self) -> None:
        df = pd.DataFrame(columns=["series_name"])
        stats.multi_ttest(df)

    def test2(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = stats.multi_ttest(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = stats.multi_ttest(df, prefix="multi_ttest_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = stats.multi_ttest(df, popmean=1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = stats.multi_ttest(df, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        df = self._get_df_of_series(seed=1)
        actual = stats.multi_ttest(df, method="sidak")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    @pytest.mark.xfail()
    def test7(self) -> None:
        df = self._get_df_of_series(seed=1)
        df.iloc[:, 0] = np.nan
        actual = stats.multi_ttest(df)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class TestApplyNormalityTest1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_normality_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_normality_test(series, prefix="norm_test_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test3(self) -> None:
        series = pd.Series([])
        stats.apply_normality_test(series)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.apply_normality_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.apply_normality_test(
            series, nan_mode="ffill_and_drop_leading"
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s
    def test6(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_normality_test(series)


class TestApplyAdfTest1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_adf_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_adf_test(series, regression="ctt")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_adf_test(series, maxlag=5)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_adf_test(series, autolag="t-stat")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_adf_test(series, prefix="adf_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test6(self) -> None:
        series = pd.Series([])
        stats.apply_adf_test(series)

    def test7(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = stats.apply_adf_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s
    def test8(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_adf_test(series)


class TestApplyKpssTest1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_kpss_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_kpss_test(series, regression="ct")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_kpss_test(series, nlags="auto")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_kpss_test(series, nlags=5)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_kpss_test(series, prefix="kpss_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test6(self) -> None:
        series = pd.Series([])
        stats.apply_kpss_test(series)

    def test7(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = stats.apply_kpss_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s
    def test8(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_kpss_test(series)


class TestApplyLjungBoxTest1(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_ljung_box_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_ljung_box_test(series, lags=3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_ljung_box_test(series, model_df=3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_ljung_box_test(series, period=5)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_ljung_box_test(series, prefix="lb_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.apply_ljung_box_test(series, return_df=False)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test7(self) -> None:
        series = pd.Series([])
        stats.apply_ljung_box_test(series)

    def test8(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = stats.apply_ljung_box_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s
    def test9(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_ljung_box_test(series)


class TestComputeZeroNanInfStats1(hut.TestCase):
    @staticmethod
    def _get_messy_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        series[:5] = 0
        series[-5:] = np.nan
        series[10:13] = np.inf
        series[13:16] = -np.inf
        return series

    def test1(self) -> None:
        series = self._get_messy_series(seed=1)
        actual = stats.compute_zero_nan_inf_stats(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_messy_series(seed=1)
        actual = stats.compute_zero_nan_inf_stats(series, prefix="data_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_zero_nan_inf_stats(series)


class TestCalculateHitRate1(hut.TestCase):
    def test1(self) -> None:
        series = pd.Series([0, 1, 0, 0, 1, None])
        actual = stats.calculate_hit_rate(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        np.random.seed(42)
        series = pd.Series(np.random.choice([0, 1, np.nan], size=(100,)))
        actual = stats.calculate_hit_rate(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        np.random.seed(42)
        series = pd.Series(np.random.choice([0, 1, np.nan], size=(100,)))
        actual = stats.calculate_hit_rate(series, alpha=0.1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = pd.Series([0, 1, 0, 0, 1, None])
        actual = stats.calculate_hit_rate(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = pd.Series([0, 1, 0, 0, 1, None])
        actual = stats.calculate_hit_rate(series, prefix="hit_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test6(self) -> None:
        series = pd.Series([])
        stats.calculate_hit_rate(series)

    # Smoke test for input of `np.nan`s
    def test7(self) -> None:
        series = pd.Series([np.nan] * 10)
        stats.calculate_hit_rate(series)

    def test_sign1(self) -> None:
        np.random.seed(42)
        data = list(np.random.randn(100)) + [np.inf, np.nan]
        series = pd.Series(data)
        actual = stats.calculate_hit_rate(series, mode="sign")
        self.check_string(hut.convert_df_to_string(actual, index=True))


class Test_compute_jensen_ratio1(hut.TestCase):
    @staticmethod
    def _get_signal(seed: int) -> pd.Series:
        np.random.seed(seed)
        n = 1000
        signal = pd.Series(np.random.randn(n))
        signal[30:50] = np.nan
        return signal

    def test1(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_jensen_ratio(signal,)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_jensen_ratio(signal, p_norm=3,)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        signal = self._get_signal(seed=1)
        signal[5:8] = np.inf
        actual = stats.compute_jensen_ratio(signal, inf_mode="ignore",)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_jensen_ratio(signal, nan_mode="ffill",)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_jensen_ratio(signal, prefix="commodity_",)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test6(self) -> None:
        signal = pd.Series([])
        stats.compute_jensen_ratio(signal)


class Test_compute_forecastability1(hut.TestCase):
    @staticmethod
    def _get_signal(seed: int) -> pd.Series:
        np.random.seed(seed)
        n = 1000
        signal = pd.Series(np.random.randn(n))
        signal[30:50] = np.nan
        return signal

    def test1(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_forecastability(signal,)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_forecastability(signal, mode="periodogram",)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_forecastability(
            signal, nan_mode="ffill_and_drop_leading",
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        signal = self._get_signal(seed=1)
        actual = stats.compute_forecastability(signal, prefix="commodity_",)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test5(self) -> None:
        signal = self._get_signal(seed=1)
        stats.compute_forecastability(signal)


class TestComputeMaxDrawdown(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([0], [0])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_max_drawdown(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_max_drawdown(series, prefix="new")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_max_drawdown(series)


class Test_compute_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        ar_params = []
        ma_params = []
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        sr = stats.compute_sharpe_ratio(realization)
        np.testing.assert_almost_equal(sr, 0.057670899)


class Test_compute_sharpe_ratio_standard_error(hut.TestCase):
    def test1(self) -> None:
        ar_params = []
        ma_params = []
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        sr_se = stats.compute_sharpe_ratio_standard_error(realization)
        np.testing.assert_almost_equal(sr_se, 0.158245297)


class Test_summarize_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        ar_params = []
        ma_params = []
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        res = stats.summarize_sharpe_ratio(realization)
        self.check_string(hut.convert_df_to_string(res, index=True))
