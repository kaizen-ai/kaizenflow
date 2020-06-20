import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import core.finance as fin
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


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
        series = self._get_series(1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        positive_series = self._get_series(1).abs()
        actual = fin.compute_turnover(positive_series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series(1)
        series[5:10] = np.nan
        actual = fin.compute_turnover(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)
