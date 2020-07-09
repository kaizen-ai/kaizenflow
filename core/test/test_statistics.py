import logging

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestComputeMoments(hut.TestCase):
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

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_moments(series)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.compute_moments(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.compute_moments(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test6(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.compute_moments(series)


class TestComputeFracZero(hut.TestCase):
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

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        stats.compute_frac_zero(series)


class TestComputeFracNan(hut.TestCase):
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

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        stats.compute_frac_nan(series)


class TestComputeFracConstant(hut.TestCase):
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

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_frac_constant(series)


class TestComputeNumFiniteSamples(hut.TestCase):
    @staticmethod
    # Smoke test for empty input.
    def test1() -> None:
        series = pd.Series([])
        stats.count_num_finite_samples(series)


class TestComputeNumUniqueValues(hut.TestCase):
    @staticmethod
    # Smoke test for empty input.
    def test1() -> None:
        series = pd.Series([])
        stats.count_num_unique_values(series)


class TestComputeDenominatorAndPackage(hut.TestCase):
    @staticmethod
    # Smoke test for empty input.
    def test1() -> None:
        series = pd.Series([])
        stats._compute_denominator_and_package(reduction=1, data=series)


class TestTTest1samp(hut.TestCase):
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

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        stats.ttest_1samp(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.ttest_1samp(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.ttest_1samp(series, nan_mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test4(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.ttest_1samp(series)


class TestMultipleTests(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = hut.get_random_df(num_cols=1, seed=seed, **date_range,)[0]
        return series

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        stats.multipletests(series)

    # Test if error is raised with default arguments when input contains NaNs.
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


class TestMultiTTest(hut.TestCase):
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

    # Smoke test for empty input.
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


class TestApplyNormalityTest(hut.TestCase):
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

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        stats.apply_normality_test(series)

    def test4(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.apply_normality_test(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        # Place some `NaN` values in the series.
        series[:5] = np.nan
        series[8:10] = np.nan
        actual = stats.apply_normality_test(
            series, nan_mode="ffill_and_drop_leading"
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test6(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_normality_test(series)


class TestApplyAdfTest(hut.TestCase):
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

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        stats.apply_adf_test(series)

    def test7(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = stats.apply_adf_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test8(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_adf_test(series)


class TestApplyKpssTest(hut.TestCase):
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

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        stats.apply_kpss_test(series)

    def test7(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = stats.apply_kpss_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test8(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_kpss_test(series)


class TestApplyLjungBoxTest(hut.TestCase):
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

    # Smoke test for empty input.
    def test7(self) -> None:
        series = pd.Series([])
        stats.apply_ljung_box_test(series)

    def test8(self) -> None:
        series = self._get_series(seed=1)
        series[3:5] = np.nan
        actual = stats.apply_ljung_box_test(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for input of `np.nan`s.
    def test9(self) -> None:
        series = pd.Series([np.nan for i in range(10)])
        stats.apply_ljung_box_test(series)


class TestComputeZeroNanInfStats(hut.TestCase):
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

    # Smoke test for empty input.
    def test3(self) -> None:
        series = pd.Series([])
        stats.compute_zero_nan_inf_stats(series)


class TestCalculateHitRate(hut.TestCase):
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

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        stats.calculate_hit_rate(series)

    # Smoke test for input of `np.nan`s.
    def test7(self) -> None:
        series = pd.Series([np.nan] * 10)
        stats.calculate_hit_rate(series)

    def test_sign1(self) -> None:
        np.random.seed(42)
        data = list(np.random.randn(100)) + [np.inf, np.nan]
        series = pd.Series(data)
        actual = stats.calculate_hit_rate(series, mode="sign")
        self.check_string(hut.convert_df_to_string(actual, index=True))


class Test_compute_jensen_ratio(hut.TestCase):
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

    # Smoke test for empty input.
    def test6(self) -> None:
        signal = pd.Series([])
        stats.compute_jensen_ratio(signal)


class Test_compute_forecastability(hut.TestCase):
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

    # Smoke test for empty input.
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


class Test_compute_bet_returns_stats(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        log_rets = Test_compute_bet_returns_stats._get_series(42)
        positions = sigp.compute_smooth_moving_average(log_rets, 4)
        actual = stats.compute_bet_returns_stats(positions, log_rets)
        self.check_string(hut.convert_df_to_string(actual, index=True))

    def test2(self) -> None:
        log_rets = Test_compute_bet_returns_stats._get_series(42)
        positions = sigp.compute_smooth_moving_average(log_rets, 4)
        log_rets.iloc[:10] = np.nan
        actual = stats.compute_bet_returns_stats(positions, log_rets)
        self.check_string(hut.convert_df_to_string(actual, index=True))

    def test3(self) -> None:
        idx = pd.to_datetime(
            [
                "2010-01-01",
                "2010-01-03",
                "2010-01-05",
                "2010-01-06",
                "2010-01-10",
                "2010-01-12",
                "2010-01-13",
                "2010-01-15",
            ]
        )
        log_rets = pd.Series([1, 2, 3, 5, 7, 11, -13, -5], index=idx)
        positions = pd.Series([1, 2, 0, 1, -3, -2, 0, -1], index=idx)
        actual = stats.compute_bet_returns_stats(positions, log_rets)
        expected = pd.Series(
            {
                "num_positions": 6,
                "num_bets": 4,
                "average_return_winning_bets": 5,
                "average_return_losing_bets": -43,
                "average_return_long_bet": 5,
                "average_return_short_bet": -19,
            },
            dtype=float,
        )
        pd.testing.assert_series_equal(actual, expected)


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
        np.testing.assert_almost_equal(sr_se, 0.160261242)


class Test_compute_annualized_sharpe_ratio(hut.TestCase):
    def _generate_minutely_series(self, n_days: float, seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": n_days * 24 * 60, "freq": "T"},
            scale=1,
            seed=seed,
        )
        return realization

    def test1(self) -> None:
        """
        Demonstrate the approximate invariance of the annualized SR (at least
        for iid returns) under resampling.
        """
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Calculate SR from minutely time series.
        srs_sr = stats.compute_annualized_sharpe_ratio(srs)
        np.testing.assert_almost_equal(srs_sr, -2.6182, decimal=3)
        # Resample to hourly and calculate SR.
        hourly_srs = srs.resample("60T").sum()
        hourly_sr = stats.compute_annualized_sharpe_ratio(hourly_srs)
        np.testing.assert_almost_equal(hourly_sr, -2.6412, decimal=3)
        # Resample to daily and calculate SR.
        daily_srs = srs.resample("D").sum()
        daily_srs_sr = stats.compute_annualized_sharpe_ratio(daily_srs)
        np.testing.assert_almost_equal(daily_srs_sr, -2.5167, decimal=3)
        # Resample to weekly and calculate SR.
        weekly_srs = srs.resample("W").sum()
        weekly_srs_sr = stats.compute_annualized_sharpe_ratio(weekly_srs)
        np.testing.assert_almost_equal(weekly_srs_sr, -2.7717, decimal=3)

    def test2(self) -> None:
        """
        Demonstrate the approximate invariance of the annualized SR in moving
        from a "uniform" ATH-only time grid to a truly uniform time grid.
        """
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Filter out non-trading time points.
        filtered_srs = fin.set_non_ath_to_nan(srs)
        filtered_srs = fin.set_weekends_to_nan(filtered_srs)
        filtered_srs = filtered_srs.dropna()
        # Treat srs as an intraday trading day-only series, e.g.,
        # approximately 252 trading days per year, ATH only.
        n_samples = filtered_srs.size
        points_per_year = 2.52 * n_samples
        filtered_srs_sr = stats.compute_sharpe_ratio(
            filtered_srs, time_scaling=points_per_year
        )
        np.testing.assert_almost_equal(filtered_srs_sr, -2.7093, decimal=3)
        # Compare to SR annualized using `freq`.
        srs_sr = stats.compute_annualized_sharpe_ratio(srs)
        np.testing.assert_almost_equal(srs_sr, -2.6182, decimal=3)


class Test_compute_annualized_sharpe_ratio_standard_error(hut.TestCase):
    def _generate_minutely_series(self, n_days: float, seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([], [])
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": n_days * 24 * 60, "freq": "T"},
            scale=1,
            seed=seed,
        )
        return realization

    def test1(self) -> None:
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Calculate SR from minutely time series.
        srs_sr_se = stats.compute_annualized_sharpe_ratio_standard_error(srs)
        np.testing.assert_almost_equal(srs_sr_se, 1.9108, decimal=3)
        # Resample to hourly and calculate SR.
        hourly_srs = srs.resample("60T").sum()
        hourly_sr_se = stats.compute_annualized_sharpe_ratio_standard_error(
            hourly_srs
        )
        np.testing.assert_almost_equal(hourly_sr_se, 1.9116, decimal=3)
        # Resample to daily and calculate SR.
        daily_srs = srs.resample("D").sum()
        daily_sr_se_sr = stats.compute_annualized_sharpe_ratio_standard_error(
            daily_srs
        )
        np.testing.assert_almost_equal(daily_sr_se_sr, 1.9290, decimal=3)
        # Resample to weekly and calculate SR.
        weekly_srs = srs.resample("W").sum()
        weekly_sr_se_sr = stats.compute_annualized_sharpe_ratio_standard_error(
            weekly_srs
        )
        np.testing.assert_almost_equal(weekly_sr_se_sr, 2.0016, decimal=3)

    def test2(self) -> None:
        srs = self._generate_minutely_series(n_days=100, seed=10)
        # Filter out non-trading time points.
        filtered_srs = fin.set_non_ath_to_nan(srs)
        filtered_srs = fin.set_weekends_to_nan(filtered_srs)
        filtered_srs = filtered_srs.dropna()
        # Treat srs as an intraday trading day-only series, e.g.,
        # approximately 252 trading days per year, ATH only.
        n_samples = filtered_srs.size
        points_per_year = 2.52 * n_samples
        filtered_srs_se = stats.compute_sharpe_ratio_standard_error(
            filtered_srs, time_scaling=points_per_year
        )
        np.testing.assert_almost_equal(filtered_srs_se, 1.5875, decimal=3)
        # Compare to SR annualized using `freq`.
        srs_sr_se = stats.compute_annualized_sharpe_ratio_standard_error(srs)
        np.testing.assert_almost_equal(srs_sr_se, 1.9108, decimal=3)


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


class Test_compute_drawdown_cdf(hut.TestCase):
    def test1(self) -> None:
        sharpe_ratio = 1
        volatility = 0.15
        drawdown = 0.05
        time = 1
        probalility = stats.compute_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            drawdown=drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.52500, decimal=3)

    def test2(self) -> None:
        sharpe_ratio = 3
        volatility = 0.15
        drawdown = 0.05
        time = 1
        probalility = stats.compute_drawdown_cdf(
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
        probalility = stats.compute_drawdown_cdf(
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
        probalility = stats.compute_normalized_drawdown_cdf(
            sharpe_ratio=sharpe_ratio,
            normalized_drawdown=normalized_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.67881, decimal=3)

    def test2(self) -> None:
        sharpe_ratio = 3
        normalized_drawdown = 1
        time = 1
        probalility = stats.compute_normalized_drawdown_cdf(
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
        probalility = stats.compute_max_drawdown_approximate_cdf(
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
        probalility = stats.compute_max_drawdown_approximate_cdf(
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
        probalility = stats.compute_max_drawdown_approximate_cdf(
            sharpe_ratio=sharpe_ratio,
            volatility=volatility,
            max_drawdown=max_drawdown,
            time=time,
        )
        np.testing.assert_almost_equal(probalility, 0.25837, decimal=3)


class TestComputeZeroDiffProportion(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        np.random.seed(seed=seed)
        n_elements = 100
        index = pd.date_range(start="1-04-2018", periods=n_elements, freq="D")
        data = list(np.random.randint(10, size=n_elements))
        series = pd.Series(data=data, index=index, name="test")
        series[45:50] = np.nan
        return series

    def test1(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_zero_diff_proportion(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_zero_diff_proportion(series, atol=1)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_zero_diff_proportion(series, rtol=0.3)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_zero_diff_proportion(
            series, nan_mode="fill_with_zero"
        )
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test6(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_zero_diff_proportion(series, prefix="prefix_")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test7(self) -> None:
        series = pd.Series([])
        stats.compute_zero_diff_proportion(series)


class TestGetInterarrivalTime(hut.TestCase):
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

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        stats.get_interarrival_time(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.get_interarrival_time(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.get_interarrival_time(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class TestComputeInterarrivalTimeStats(hut.TestCase):
    @staticmethod
    def _get_series(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        series.drop(series.index[15:20], inplace=True)
        series[5:10] = np.nan
        return series

    # Smoke test for empty input.
    def test1(self) -> None:
        series = pd.Series([])
        stats.compute_interarrival_time_stats(series)

    def test2(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_interarrival_time_stats(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        actual = stats.compute_interarrival_time_stats(series, nan_mode="ffill")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)
