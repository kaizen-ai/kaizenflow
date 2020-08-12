import collections
import logging
import os
import pprint
from typing import Any, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as sig_gen
import core.signal_processing as sigp
import helpers.git as git
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_accumulate(hut.TestCase):
    def test1(self) -> None:
        srs = pd.Series(
            range(0, 20), index=pd.date_range("2010-01-01", periods=20)
        )
        actual = sigp.accumulate(srs, num_steps=1)
        expected = srs.astype(float)
        pd.testing.assert_series_equal(actual, expected)

    def test2(self) -> None:
        idx = pd.date_range("2010-01-01", periods=10)
        srs = pd.Series([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], index=idx)
        actual = sigp.accumulate(srs, num_steps=2)
        expected = pd.Series([np.nan, 1, 3, 5, 7, 9, 11, 13, 15, 17], index=idx)
        pd.testing.assert_series_equal(actual, expected)

    def test3(self) -> None:
        idx = pd.date_range("2010-01-01", periods=10)
        srs = pd.Series([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], index=idx)
        actual = sigp.accumulate(srs, num_steps=3)
        expected = pd.Series(
            [np.nan, np.nan, 3, 6, 9, 12, 15, 18, 21, 24], index=idx
        )
        pd.testing.assert_series_equal(actual, expected)

    def test4(self) -> None:
        srs = pd.Series(
            np.random.randn(100), index=pd.date_range("2010-01-01", periods=100)
        )
        actual = sigp.accumulate(srs, num_steps=5)
        self.check_string(hut.convert_df_to_string(actual, index=True))

    def test_long_step1(self) -> None:
        idx = pd.date_range("2010-01-01", periods=3)
        srs = pd.Series([1, 2, 3], index=idx)
        actual = sigp.accumulate(srs, num_steps=5)
        expected = pd.Series([np.nan, np.nan, np.nan], index=idx)
        pd.testing.assert_series_equal(actual, expected)

    def test_nans1(self) -> None:
        idx = pd.date_range("2010-01-01", periods=10)
        srs = pd.Series([0, 1, np.nan, 2, 3, 4, np.nan, 5, 6, 7], index=idx)
        actual = sigp.accumulate(srs, num_steps=3)
        expected = pd.Series(
            [
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                9,
                np.nan,
                np.nan,
                np.nan,
                18,
            ],
            index=idx,
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_nans2(self) -> None:
        idx = pd.date_range("2010-01-01", periods=6)
        srs = pd.Series([np.nan, np.nan, np.nan, 2, 3, 4], index=idx)
        actual = sigp.accumulate(srs, num_steps=3)
        expected = pd.Series(
            [np.nan, np.nan, np.nan, np.nan, np.nan, 9], index=idx
        )
        pd.testing.assert_series_equal(actual, expected)

    def test_nans3(self) -> None:
        idx = pd.date_range("2010-01-01", periods=6)
        srs = pd.Series([np.nan, np.nan, np.nan, 2, 3, 4], index=idx)
        actual = sigp.accumulate(srs, num_steps=2)
        expected = pd.Series([np.nan, np.nan, np.nan, np.nan, 5, 7], index=idx)
        pd.testing.assert_series_equal(actual, expected)


class Test_get_symmetric_equisized_bins(hut.TestCase):
    def test_zero_in_bin_interior_false(self) -> None:
        input_ = pd.Series([-1, 3])
        expected = np.array([-3, -2, -1, 0, 1, 2, 3])
        actual = sigp.get_symmetric_equisized_bins(input_, 1)
        np.testing.assert_array_equal(actual, expected)

    def test_zero_in_bin_interior_true(self) -> None:
        input_ = pd.Series([-1, 3])
        expected = np.array([-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5])
        actual = sigp.get_symmetric_equisized_bins(input_, 1, True)
        np.testing.assert_array_equal(actual, expected)

    def test_infs(self) -> None:
        data = pd.Series([-1, np.inf, -np.inf, 3])
        expected = np.array([-4, -2, 0, 2, 4])
        actual = sigp.get_symmetric_equisized_bins(data, 2)
        np.testing.assert_array_equal(actual, expected)


class Test_compute_rolling_zscore1(hut.TestCase):
    @staticmethod
    def _get_arma_series(seed: int) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([1], [1])
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        ).rename("input")
        return series

    def test_default_values1(self) -> None:
        """
        Test with default parameters on a heaviside series.
        """
        heaviside = sig_gen.get_heaviside(-10, 252, 1, 1).rename("input")
        actual = sigp.compute_rolling_zscore(heaviside, tau=40).rename("output")
        output_df = pd.concat([heaviside, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_default_values2(self) -> None:
        """
        Test for tau with default parameters on a heaviside series.
        """
        heaviside = sig_gen.get_heaviside(-10, 252, 1, 1).rename("input")
        actual = sigp.compute_rolling_zscore(heaviside, tau=20).rename("output")
        output_df = pd.concat([heaviside, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_clean1(self) -> None:
        """
        Test on a clean arma series.
        """
        series = self._get_arma_series(seed=1)
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_nan1(self) -> None:
        """
        Test on an arma series with leading NaNs.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = np.nan
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_nan2(self) -> None:
        """
        Test on an arma series with interspersed NaNs.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = np.nan
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_zero1(self) -> None:
        """
        Test on an arma series with leading zeros.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = 0
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_zero2(self) -> None:
        """
        Test on an arma series with interspersed zeros.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = 0
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_inf1(self) -> None:
        """
        Test on an arma series with leading infs.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = np.inf
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_arma_inf2(self) -> None:
        """
        Test on an arma series with interspersed infs.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = np.inf
        actual = sigp.compute_rolling_zscore(series, tau=20).rename("output")
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_clean1(self) -> None:
        """
        Test on a clean arma series when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_nan1(self) -> None:
        """
        Test on an arma series with leading NaNs when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = np.nan
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_nan2(self) -> None:
        """
        Test on an arma series with interspersed NaNs when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = np.nan
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_zero1(self) -> None:
        """
        Test on an arma series with leading zeros when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = 0
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_zero2(self) -> None:
        """
        Test on an arma series with interspersed zeros when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = 0
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_inf1(self) -> None:
        """
        Test on an arma series with leading infs when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = np.inf
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay1_arma_inf2(self) -> None:
        """
        Test on an arma series with interspersed infs when `delay=1`.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = np.inf
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=1).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_clean1(self) -> None:
        """
        Test on a clean arma series when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_nan1(self) -> None:
        """
        Test on an arma series with leading NaNs when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = np.nan
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_nan2(self) -> None:
        """
        Test on an arma series with interspersed NaNs when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = np.nan
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_zero1(self) -> None:
        """
        Test on an arma series with leading zeros when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = 0
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_zero2(self) -> None:
        """
        Test on an arma series with interspersed zeros when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = 0
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_inf1(self) -> None:
        """
        Test on an arma series with leading infs when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        series[:5] = np.inf
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)

    def test_delay2_arma_inf2(self) -> None:
        """
        Test on an arma series with interspersed infs when `delay=2`.
        """
        series = self._get_arma_series(seed=1)
        series[5:10] = np.inf
        actual = sigp.compute_rolling_zscore(series, tau=20, delay=2).rename(
            "output"
        )
        output_df = pd.concat([series, actual], axis=1)
        output_df_string = hut.convert_df_to_string(output_df, index=True)
        self.check_string(output_df_string)


class Test_process_outliers1(hut.TestCase):
    def _helper(
        self,
        srs: pd.Series,
        mode: str,
        lower_quantile: float,
        num_df_rows: int = 10,
        window: int = 100,
        min_periods: Optional[int] = 2,
        **kwargs: Any,
    ) -> None:
        info: collections.OrderedDict = collections.OrderedDict()
        srs_out = sigp.process_outliers(
            srs,
            mode,
            lower_quantile,
            window=window,
            min_periods=min_periods,
            info=info,
            **kwargs,
        )
        txt = []
        txt.append("# info")
        txt.append(pprint.pformat(info))
        txt.append("# srs_out")
        txt.append(str(srs_out.head(num_df_rows)))
        self.check_string("\n".join(txt))

    @staticmethod
    def _get_data1() -> pd.Series:
        np.random.seed(100)
        n = 100000
        data = np.random.normal(loc=0.0, scale=1.0, size=n)
        return pd.Series(data)

    def test_winsorize1(self) -> None:
        srs = self._get_data1()
        mode = "winsorize"
        lower_quantile = 0.01
        # Check.
        self._helper(srs, mode, lower_quantile)

    def test_set_to_nan1(self) -> None:
        srs = self._get_data1()
        mode = "set_to_nan"
        lower_quantile = 0.01
        # Check.
        self._helper(srs, mode, lower_quantile)

    def test_set_to_zero1(self) -> None:
        srs = self._get_data1()
        mode = "set_to_zero"
        lower_quantile = 0.01
        # Check.
        self._helper(srs, mode, lower_quantile)

    @staticmethod
    def _get_data2() -> pd.Series:
        return pd.Series(range(1, 10))

    def test_winsorize2(self) -> None:
        srs = self._get_data2()
        mode = "winsorize"
        lower_quantile = 0.2
        # Check.
        self._helper(srs, mode, lower_quantile, num_df_rows=len(srs))

    def test_set_to_nan2(self) -> None:
        srs = self._get_data2()
        mode = "set_to_nan"
        lower_quantile = 0.2
        # Check.
        self._helper(srs, mode, lower_quantile, num_df_rows=len(srs))

    def test_set_to_zero2(self) -> None:
        srs = self._get_data2()
        mode = "set_to_zero"
        lower_quantile = 0.2
        upper_quantile = 0.5
        # Check.
        self._helper(
            srs,
            mode,
            lower_quantile,
            num_df_rows=len(srs),
            upper_quantile=upper_quantile,
        )


class Test_compute_smooth_derivative1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        scaling = 2
        order = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_smooth_derivative(
            signal, tau, min_periods, scaling, order
        )
        self.check_string(actual.to_string())


class Test_compute_smooth_moving_average1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_smooth_moving_average(
            signal, tau, min_periods, min_depth, max_depth
        )
        self.check_string(actual.to_string())


class Test_digitize1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        bins = [0, 0.2, 0.4]
        right = False
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.digitize(signal, bins, right)
        self.check_string(actual.to_string())

    def test_heaviside1(self) -> None:
        heaviside = sig_gen.get_heaviside(-10, 20, 1, 1)
        bins = [0, 0.2, 0.4]
        right = False
        actual = sigp.digitize(heaviside, bins, right)
        self.check_string(actual.to_string())


class Test_compute_rolling_moment1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_moment(
            signal, tau, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_norm1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_norm(
            signal, tau, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_var1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_var(
            signal, tau, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_std1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_std(
            signal, tau, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_demean1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_demean(
            signal, tau, min_periods, min_depth, max_depth
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_skew1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau_z = 40
        tau_s = 20
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_skew(
            signal, tau_z, tau_s, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_kurtosis1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau_z = 40
        tau_s = 20
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_kurtosis(
            signal, tau_z, tau_s, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_sharpe_ratio1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_rolling_sharpe_ratio(
            signal, tau, min_periods, min_depth, max_depth, p_moment
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_corr1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        demean = True
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        df = pd.DataFrame(np.random.randn(n, 2))
        signal1 = df[0]
        signal2 = df[1]
        actual = sigp.compute_rolling_corr(
            signal1,
            signal2,
            tau,
            demean,
            min_periods,
            min_depth,
            max_depth,
            p_moment,
        )
        self.check_string(actual.to_string())


class Test_compute_rolling_zcorr1(hut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        tau = 40
        demean = True
        min_periods = 20
        min_depth = 1
        max_depth = 5
        p_moment = 2
        n = 1000
        df = pd.DataFrame(np.random.randn(n, 2))
        signal1 = df[0]
        signal2 = df[1]
        actual = sigp.compute_rolling_zcorr(
            signal1,
            signal2,
            tau,
            demean,
            min_periods,
            min_depth,
            max_depth,
            p_moment,
        )
        self.check_string(actual.to_string())


class Test_compute_ipca(hut.TestCase):
    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        """
        Generate a dataframe via `sig_gen.MultivariateNormalProcess()`.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=10, seed=seed)
        df = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=seed
        )
        return df

    def test1(self) -> None:
        """
        Test for a clean input.
        """
        df = self._get_df(seed=1)
        num_pc = 3
        tau = 16
        lambda_df, unit_eigenvec_dfs = sigp.compute_ipca(df, num_pc, tau)
        unit_eigenvec_dfs_txt = "\n".join(
            [f"{i}:\n{df.to_string()}" for i, df in enumerate(unit_eigenvec_dfs)]
        )
        txt = (
            f"lambda_df:\n{lambda_df.to_string()}\n, "
            f"unit_eigenvecs_dfs:\n{unit_eigenvec_dfs_txt}"
        )
        self.check_string(txt)

    def test2(self) -> None:
        """
        Test for an input with leading NaNs in only a subset of cols.
        """
        df = self._get_df(seed=1)
        df.iloc[0:3, :-3] = np.nan
        num_pc = 3
        tau = 16
        lambda_df, unit_eigenvec_dfs = sigp.compute_ipca(df, num_pc, tau)
        unit_eigenvec_dfs_txt = "\n".join(
            [f"{i}:\n{df.to_string()}" for i, df in enumerate(unit_eigenvec_dfs)]
        )
        txt = (
            f"lambda_df:\n{lambda_df.to_string()}\n, "
            f"unit_eigenvecs_dfs:\n{unit_eigenvec_dfs_txt}"
        )
        self.check_string(txt)

    def test3(self) -> None:
        """
        Test for an input with interspersed NaNs.
        """
        df = self._get_df(seed=1)
        df.iloc[5:8, 3:5] = np.nan
        df.iloc[2:4, 8:] = np.nan
        num_pc = 3
        tau = 16
        lambda_df, unit_eigenvec_dfs = sigp.compute_ipca(df, num_pc, tau)
        unit_eigenvec_dfs_txt = "\n".join(
            [f"{i}:\n{df.to_string()}" for i, df in enumerate(unit_eigenvec_dfs)]
        )
        txt = (
            f"lambda_df:\n{lambda_df.to_string()}\n, "
            f"unit_eigenvecs_dfs:\n{unit_eigenvec_dfs_txt}"
        )
        self.check_string(txt)

    def test4(self) -> None:
        """
        Test for an input with a full-NaN row among the 3 first rows.

        The eigenvalue estimates aren't in sorted order but should be.
        TODO(*): Fix problem with not sorted eigenvalue estimates.
        """
        df = self._get_df(seed=1)
        df.iloc[1:2, :] = np.nan
        num_pc = 3
        tau = 16
        lambda_df, unit_eigenvec_dfs = sigp.compute_ipca(df, num_pc, tau)
        unit_eigenvec_dfs_txt = "\n".join(
            [f"{i}:\n{df.to_string()}" for i, df in enumerate(unit_eigenvec_dfs)]
        )
        txt = (
            f"lambda_df:\n{lambda_df.to_string()}\n, "
            f"unit_eigenvecs_dfs:\n{unit_eigenvec_dfs_txt}"
        )
        self.check_string(txt)

    def test5(self) -> None:
        """
        Test for an input with 5 leading NaNs in all cols.
        """
        df = self._get_df(seed=1)
        df.iloc[:5, :] = np.nan
        num_pc = 3
        tau = 16
        lambda_df, unit_eigenvec_dfs = sigp.compute_ipca(df, num_pc, tau)
        unit_eigenvec_dfs_txt = "\n".join(
            [f"{i}:\n{df.to_string()}" for i, df in enumerate(unit_eigenvec_dfs)]
        )
        txt = (
            f"lambda_df:\n{lambda_df.to_string()}\n, "
            f"unit_eigenvecs_dfs:\n{unit_eigenvec_dfs_txt}"
        )
        self.check_string(txt)


class Test__compute_ipca_step(hut.TestCase):
    @staticmethod
    def _get_output_txt(
        u: pd.Series, v: pd.Series, u_next: pd.Series, v_next: pd.Series
    ) -> str:
        """
        Create string output for tests results.
        """
        u_string = hut.convert_df_to_string(u, index=True)
        v_string = hut.convert_df_to_string(v, index=True)
        u_next_string = hut.convert_df_to_string(u_next, index=True)
        v_next_string = hut.convert_df_to_string(v_next, index=True)
        txt = (
            f"u:\n{u_string}\n"
            f"v:\n{v_string}\n"
            f"u_next:\n{u_next_string}\n"
            f"v_next:\n{v_next_string}"
        )
        return txt

    def test1(self) -> None:
        """
        Test for clean input series.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 10, "freq": "B"}, seed=1
        )
        u = df.iloc[1]
        v = df.iloc[2]
        alpha = 0.5
        u_next, v_next = sigp._compute_ipca_step(u, v, alpha)
        txt = self._get_output_txt(u, v, u_next, v_next)
        self.check_string(txt)

    def test2(self) -> None:
        """
        Test for input series with all zeros.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 10, "freq": "B"}, seed=1
        )
        u = df.iloc[1]
        v = df.iloc[2]
        u[:] = 0
        v[:] = 0
        alpha = 0.5
        u_next, v_next = sigp._compute_ipca_step(u, v, alpha)
        txt = self._get_output_txt(u, v, u_next, v_next)
        self.check_string(txt)

    def test3(self) -> None:
        """
        Test that u == u_next for the case when np.linalg.norm(v)=0.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 10, "freq": "B"}, seed=1
        )
        u = df.iloc[1]
        v = df.iloc[2]
        v[:] = 0
        alpha = 0.5
        u_next, v_next = sigp._compute_ipca_step(u, v, alpha)
        txt = self._get_output_txt(u, v, u_next, v_next)
        self.check_string(txt)

    def test4(self) -> None:
        """
       Test for input series with all NaNs.

       Output is not intended.
       TODO(Dan): implement a way to deal with NaNs in the input.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 10, "freq": "B"}, seed=1
        )
        u = df.iloc[1]
        v = df.iloc[2]
        u[:] = np.nan
        v[:] = np.nan
        alpha = 0.5
        u_next, v_next = sigp._compute_ipca_step(u, v, alpha)
        txt = self._get_output_txt(u, v, u_next, v_next)
        self.check_string(txt)

    def test5(self) -> None:
        """
        Test for input series with some NaNs.

        Output is not intended.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=10, seed=1)
        df = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 10, "freq": "B"}, seed=1
        )
        u = df.iloc[1]
        v = df.iloc[2]
        u[3:6] = np.nan
        v[5:8] = np.nan
        alpha = 0.5
        u_next, v_next = sigp._compute_ipca_step(u, v, alpha)
        txt = self._get_output_txt(u, v, u_next, v_next)
        self.check_string(txt)


@pytest.mark.slow
class Test_gallery_signal_processing1(hut.TestCase):
    def test_notebook1(self) -> None:
        file_name = os.path.join(
            git.get_amp_abs_path(),
            "core/notebooks/gallery_signal_processing.ipynb",
        )
        scratch_dir = self.get_scratch_space()
        hut.run_notebook(file_name, scratch_dir)


class TestProcessNonfinite1(hut.TestCase):
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
        series = self._get_messy_series(1)
        actual = sigp.process_nonfinite(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_messy_series(1)
        actual = sigp.process_nonfinite(series, remove_nan=False)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_messy_series(1)
        actual = sigp.process_nonfinite(series, remove_inf=False)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_rolling_annualized_sharpe_ratio(hut.TestCase):
    def test1(self) -> None:
        ar_params = []
        ma_params = []
        arma_process = sig_gen.ArmaProcess(ar_params, ma_params)
        realization = arma_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"},
            scale=1,
            burnin=5,
        )
        rolling_sr = sigp.compute_rolling_annualized_sharpe_ratio(
            realization, tau=16
        )
        self.check_string(hut.convert_df_to_string(rolling_sr, index=True))


class Test_get_swt(hut.TestCase):
    @staticmethod
    def _get_series(seed: int, periods: int = 20) -> pd.Series:
        arma_process = sig_gen.ArmaProcess([0], [0])
        date_range = {"start": "1/1/2010", "periods": periods, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        )
        return series

    @staticmethod
    def _get_tuple_output_txt(
        output: Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]
    ) -> str:
        """
        Create string output for a tuple type return.
        """
        smooth_df_string = hut.convert_df_to_string(output[0], index=True)
        detail_df_string = hut.convert_df_to_string(output[1], index=True)
        output_str = (
            f"smooth_df:\n{smooth_df_string}\n"
            f"\ndetail_df\n{detail_df_string}\n"
        )
        return output_str

    def test_clean1(self) -> None:
        """
        Test for default values.
        """
        series = self._get_series(seed=1, periods=40)
        actual = sigp.get_swt(series, wavelet="haar")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_timing_mode1(self) -> None:
        """
        Test for timing_mode="knowledge_time".
        """
        series = self._get_series(seed=1)
        actual = sigp.get_swt(
            series, wavelet="haar", timing_mode="knowledge_time"
        )
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_timing_mode2(self) -> None:
        """
        Test for timing_mode="zero_phase".
        """
        series = self._get_series(seed=1)
        actual = sigp.get_swt(series, wavelet="haar", timing_mode="zero_phase")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_timing_mode3(self) -> None:
        """
        Test for timing_mode="raw".
        """
        series = self._get_series(seed=1)
        actual = sigp.get_swt(series, wavelet="haar", timing_mode="raw")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_output_mode1(self) -> None:
        """
        Test for output_mode="tuple".
        """
        series = self._get_series(seed=1)
        actual = sigp.get_swt(series, wavelet="haar", output_mode="tuple")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_output_mode2(self) -> None:
        """
        Test for output_mode="smooth".
        """
        series = self._get_series(seed=1)
        actual = sigp.get_swt(series, wavelet="haar", output_mode="smooth")
        actual_str = hut.convert_df_to_string(actual, index=True)
        output_str = f"smooth_df:\n{actual_str}\n"
        self.check_string(output_str)

    def test_output_mode3(self) -> None:
        """
        Test for output_mode="detail".
        """
        series = self._get_series(seed=1)
        actual = sigp.get_swt(series, wavelet="haar", output_mode="detail")
        actual_str = hut.convert_df_to_string(actual, index=True)
        output_str = f"detail_df:\n{actual_str}\n"
        self.check_string(output_str)


class Test_resample(hut.TestCase):
    @staticmethod
    def _get_series(seed: int, periods: int, freq: str) -> pd.Series:
        """
        Periods include:

        26/12/2014 - Friday,    workday,    5th DoW
        27/12/2014 - Saturday,  weekend,    6th DoW
        28/12/2014 - Sunday,    weekend,    7th DoW
        29/12/2014 - Monday,    workday,    1th DoW
        30/12/2014 - Tuesday,   workday,    2th DoW
        31/12/2014 - Wednesday, workday,    3th DoW
        01/12/2014 - Thursday,  workday,    4th DoW
        02/12/2014 - Friday,    workday,    5th DoW
        03/12/2014 - Saturday,  weekend,    6th DoW
        """
        arma_process = sig_gen.ArmaProcess([1], [1])
        date_range = {"start": "2014-12-26", "periods": periods, "freq": freq}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, scale=0.1, seed=seed
        ).rename(f"Input in freq='{freq}'")
        return series

    def _get_df(self, seed: int, periods: int, freq: str) -> pd.DataFrame:
        srs_1 = self._get_series(seed=seed, periods=periods, freq=freq).rename(
            f"1st input in freq='{freq}'"
        )
        srs_2 = self._get_series(
            seed=seed + 1, periods=periods, freq=freq
        ).rename(f"2nd input in freq='{freq}'")
        df = pd.DataFrame([srs_1, srs_2]).T
        return df

    @staticmethod
    def _get_output_txt(
        input_data: Union[pd.Series, pd.DataFrame],
        output: Union[pd.Series, pd.DataFrame],
    ) -> str:
        """
        Create string output for tests results.
        """
        input_string = hut.convert_df_to_string(input_data, index=True)
        output_string = hut.convert_df_to_string(output, index=True)
        txt = f"input:\n{input_string}\n\n" f"output:\n{output_string}\n"
        return txt

    def test_srs_day_to_year1(self) -> None:
        """
        Test pd.Series input, freq="D", unit='Y', aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        actual = (
            sigp.resample(series, rule="Y").sum().rename("Output in freq='Y'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_day_to_year1(self) -> None:
        """
        Test pd.DataFrame input, freq="D", unit='Y', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="D")
        actual = sigp.resample(df, rule="Y").sum()
        actual.columns = ["1st output in freq='Y'", "2nd output in freq='Y'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_day_to_month1(self) -> None:
        """
        Test pd.Series input, freq="D", unit='M', aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        actual = (
            sigp.resample(series, rule="M").sum().rename("Output in freq='M'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_day_to_month1(self) -> None:
        """
        Test pd.DataFrame input, freq="D", unit='M', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="D")
        actual = sigp.resample(df, rule="M").sum()
        actual.columns = ["1st output in freq='M'", "2nd output in freq='M'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_day_to_week1(self) -> None:
        """
        Test pd.Series input, freq="D", unit='W', aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        actual = (
            sigp.resample(series, rule="W").sum().rename("Output in freq='W'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_day_to_week1(self) -> None:
        """
        Test pd.DataFrame input, freq="D", unit='W', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="D")
        actual = sigp.resample(df, rule="W").sum()
        actual.columns = ["1st output in freq='W'", "2nd output in freq='W'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_day_to_business_day1(self) -> None:
        """
        Test pd.Series input, freq="D", unit='B', aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        actual = (
            sigp.resample(series, rule="B").sum().rename("Output in freq='B'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_srs_day_to_business_day_left_kwargs1(self) -> None:
        """
        Test for specified kwargs.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        actual = (
            sigp.resample(series, rule="B", closed="left", label="left")
            .sum()
            .rename("Output in freq='B'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_day_to_business_day1(self) -> None:
        """
        Test pd.DataFrame input, freq="D", unit='B', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="D")
        actual = sigp.resample(df, rule="B").sum()
        actual.columns = ["1st output in freq='B'", "2nd output in freq='B'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_df_day_to_business_day_left_kwargs1(self) -> None:
        """
        Test pd.DataFrame input, freq="D", unit='B', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="D")
        actual = sigp.resample(df, rule="B", closed="left", label="left").sum()
        actual.columns = ["1st output in freq='B'", "2nd output in freq='B'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_equal_unit_freq_days1(self) -> None:
        """
        Test pd.Series input, freq="D", unit="D", aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        actual = (
            sigp.resample(series, rule="D").sum().rename("Output in freq='D'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_equal_unit_freq_days1(self) -> None:
        """
        Test pd.DataFrame input, freq="D", unit='D', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="D")
        actual = sigp.resample(df, rule="D").sum()
        actual.columns = ["1st output in freq='D'", "2nd output in freq='D'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_equal_unit_freq_minutes1(self) -> None:
        """
        Test pd.Series input, freq="T", unit="T", aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="T")
        actual = (
            sigp.resample(series, rule="T").sum().rename("Output in freq='T'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_equal_unit_freq_minutes1(self) -> None:
        """
        Test pd.DataFrame input, freq="T", unit='T', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="T")
        actual = sigp.resample(df, rule="T").sum()
        actual.columns = ["1st output in freq='T'", "2nd output in freq='T'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_upsample_month_to_days1(self) -> None:
        """
        Test pd.Series input, freq="M", unit='D', aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=3, freq="M")
        actual = (
            sigp.resample(series, rule="D").sum().rename("Output in freq='D'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_upsample_month_to_days1(self) -> None:
        """
        Test pd.DataFrame input, freq="M", unit='D', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=3, freq="M")
        actual = sigp.resample(df, rule="D").sum()
        actual.columns = ["1st output in freq='D'", "2nd output in freq='D'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_upsample_business_days_to_days1(self) -> None:
        """
        Test pd.Series input, freq="B", unit='D', aggregate with `.sum()`.
        """
        series = self._get_series(seed=1, periods=9, freq="B")
        actual = (
            sigp.resample(series, rule="D").sum().rename("Output in freq='D'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_df_upsample_business_days_to_days1(self) -> None:
        """
        Test pd.DataFrame input, freq="B", unit='D', aggregate with `.sum()`.
        """
        df = self._get_df(seed=1, periods=9, freq="B")
        actual = sigp.resample(df, rule="D").sum()
        actual.columns = ["1st output in freq='D'", "2nd output in freq='D'"]
        txt = self._get_output_txt(df, actual)
        self.check_string(txt)

    def test_srs_left_kwargs_only_business_day1(self) -> None:
        """
        Test for specified kwargs and the same frequency and unit.
        """
        series = self._get_series(seed=1, periods=9, freq="B")
        actual = (
            sigp.resample(series, rule="B", closed="left", label="left")
            .sum()
            .rename("Output in freq='B'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)

    def test_srs_no_freq_input_day_to_business_day1(self) -> None:
        """
        Test for an input without `freq`.
        """
        series = self._get_series(seed=1, periods=9, freq="D")
        # Remove some observations in order to make `freq` None.
        series[2:6] = np.nan
        series.dropna()
        actual = (
            sigp.resample(series, rule="B").sum().rename("Output in freq='B'")
        )
        txt = self._get_output_txt(series, actual)
        self.check_string(txt)
