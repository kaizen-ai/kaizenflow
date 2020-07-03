import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_aggregate_log_rets(hut.TestCase):
    @staticmethod
    def _get_sample(seed: int) -> pd.DataFrame:
        mean = pd.Series([1, 2])
        cov = pd.DataFrame([[0.5, 0.2], [0.2, 0.3]])
        date_range = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        mn_process = sig_gen.MultivariateNormalProcess(mean=mean, cov=cov)
        sample = mn_process.generate_sample(date_range, seed=seed)
        return sample

    def test1(self) -> None:
        """
        Test for a clean input.
        """
        sample = self._get_sample(seed=1)
        rescaled_srs, relative_weights = fin.aggregate_log_rets(sample, 0.1)
        rescaled_srs_string = hut.convert_df_to_string(rescaled_srs, index=True)
        txt = (
            f"rescaled_srs:\n{rescaled_srs_string}\n, "
            f"relative_weights:\n{str(relative_weights)}"
        )
        self.check_string(txt)

    def test2(self) -> None:
        """
        Test for an input with NaNs.
        """
        sample = self._get_sample(seed=1)
        sample.iloc[1, 1] = np.nan
        sample.iloc[0:5, 0] = np.nan
        rescaled_srs, relative_weights = fin.aggregate_log_rets(sample, 0.1)
        rescaled_srs_string = hut.convert_df_to_string(rescaled_srs, index=True)
        txt = (
            f"rescaled_srs:\n{rescaled_srs_string}\n, "
            f"relative_weights:\n{str(relative_weights)}"
        )
        self.check_string(txt)


class Test_compute_drawdown(hut.TestCase):
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
        series = self._get_series(1)
        actual = fin.compute_drawdown(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_turnover(hut.TestCase):
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
        series[5:10] = np.nan
        actual = fin.compute_turnover(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        positive_series = self._get_series(seed=1).abs()
        actual = fin.compute_turnover(positive_series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)


class Test_compute_average_holding_period(hut.TestCase):
    @staticmethod
    def _get_series_in_unit(seed: int, freq: str = "D") -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": freq}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        return series

    def test1(self) -> None:
        series = self._get_series_in_unit(seed=1)
        series[5:10] = np.nan
        actual = fin.compute_average_holding_period(series)
        expected = 1.23458
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test2(self) -> None:
        positive_series = self._get_series_in_unit(seed=1).abs()
        actual = fin.compute_average_holding_period(positive_series)
        expected = 1.23620
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test3(self) -> None:
        series = self._get_series_in_unit(seed=1)
        actual = fin.compute_average_holding_period(series, unit="M")
        expected = 0.05001
        np.testing.assert_almost_equal(actual, expected, decimal=3)
