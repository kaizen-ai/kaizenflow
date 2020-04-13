import collections
import logging
import os
import pprint
from typing import Any, Optional

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as sig_gen
import core.signal_processing as sigp
import helpers.git as git
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


class Test_get_symmetric_equisized_bins(ut.TestCase):
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


class Test_compute_rolling_zscore1(ut.TestCase):
    def test_default_values1(self) -> None:
        heaviside = sig_gen.get_heaviside(-10, 252, 1, 1)
        zscored = sigp.compute_rolling_zscore(heaviside, tau=40)
        self.check_string(zscored.to_string())

    def test_default_values2(self) -> None:
        heaviside = sig_gen.get_heaviside(-10, 252, 1, 1)
        zscored = sigp.compute_rolling_zscore(heaviside, tau=20)
        self.check_string(zscored.to_string())


class Test_process_outliers1(ut.TestCase):
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
        info = collections.OrderedDict()
        srs_out = sigp.process_outliers(
            srs, window, min_periods, mode, lower_quantile, info=info, **kwargs
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


class Test_compute_smooth_derivative1(ut.TestCase):
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


class Test_compute_smooth_moving_average1(ut.TestCase):
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


class Test_compute_forecastability1(ut.TestCase):
    def test_welch(self) -> None:
        np.random.seed(42)
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_forecastability(signal, mode="welch")
        expected = 0.014599675035670168
        np.testing.assert_almost_equal(actual, expected)

    def test_periodogram(self) -> None:
        np.random.seed(42)
        n = 1000
        signal = pd.Series(np.random.randn(n))
        actual = sigp.compute_forecastability(signal, mode="periodogram")
        expected = 0.06319779777321788
        np.testing.assert_almost_equal(actual, expected)


class Test_digitize1(ut.TestCase):
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


class Test_compute_rolling_moment1(ut.TestCase):
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


class Test_compute_rolling_norm1(ut.TestCase):
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


class Test_compute_rolling_var1(ut.TestCase):
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


class Test_compute_rolling_std1(ut.TestCase):
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


class Test_compute_rolling_demean1(ut.TestCase):
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


class Test_compute_rolling_skew1(ut.TestCase):
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


class Test_compute_rolling_kurtosis1(ut.TestCase):
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


class Test_compute_rolling_sharpe_ratio1(ut.TestCase):
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


class Test_compute_rolling_corr1(ut.TestCase):
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


class Test_compute_rolling_zcorr1(ut.TestCase):
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


class Test_compute_ipca(ut.TestCase):
    def test1(self) -> None:
        np.random.seed(42)
        num_pc = 3
        alpha = 0.5
        n = 100
        m = 10
        df = pd.DataFrame(np.random.randn(n, m))
        lambda_df, unit_eigenvec_dfs = sigp.compute_ipca(df, num_pc, alpha)
        unit_eigenvec_dfs_txt = "\n".join(
            [f"{i}:\n{df.to_string()}" for i, df in enumerate(unit_eigenvec_dfs)]
        )
        txt = (
            f"lambda_df:\n{lambda_df.to_string()}\n, "
            f"unit_eigenvecs_dfs:\n{unit_eigenvec_dfs_txt}"
        )
        self.check_string(txt)


@pytest.mark.slow
class Test_gallery_signal_processing1(ut.TestCase):
    def test_notebook1(self) -> None:
        file_name = os.path.join(
            git.get_amp_abs_path(),
            "core/notebooks/gallery_signal_processing.ipynb",
        )
        scratch_dir = self.get_scratch_space()
        ut.run_notebook(file_name, scratch_dir)
