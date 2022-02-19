import logging
from typing import Tuple, Union

import numpy as np
import pandas as pd
import pytest

import core.artificial_signal_generators as carsigen
import core.signal_processing.swt as csiprswt
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_swt(hunitest.TestCase):
    def test_clean1(self) -> None:
        """
        Test for default values.
        """
        series = self._get_series(seed=1, periods=40)
        actual = csiprswt.get_swt(series, wavelet="haar")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_timing_mode1(self) -> None:
        """
        Test for timing_mode="knowledge_time".
        """
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(
            series, wavelet="haar", timing_mode="knowledge_time"
        )
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_timing_mode2(self) -> None:
        """
        Test for timing_mode="zero_phase".
        """
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(
            series, wavelet="haar", timing_mode="zero_phase"
        )
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_timing_mode3(self) -> None:
        """
        Test for timing_mode="raw".
        """
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(series, wavelet="haar", timing_mode="raw")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_output_mode1(self) -> None:
        """
        Test for output_mode="tuple".
        """
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(series, wavelet="haar", output_mode="tuple")
        output_str = self._get_tuple_output_txt(actual)
        self.check_string(output_str)

    def test_output_mode2(self) -> None:
        """
        Test for output_mode="smooth".
        """
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(series, wavelet="haar", output_mode="smooth")
        actual_str = hunitest.convert_df_to_string(actual, index=True)
        output_str = f"smooth_df:\n{actual_str}\n"
        self.check_string(output_str)

    def test_output_mode3(self) -> None:
        """
        Test for output_mode="detail".
        """
        series = self._get_series(seed=1)
        actual = csiprswt.get_swt(series, wavelet="haar", output_mode="detail")
        actual_str = hunitest.convert_df_to_string(actual, index=True)
        output_str = f"detail_df:\n{actual_str}\n"
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

    @staticmethod
    def _get_tuple_output_txt(
        output: Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]
    ) -> str:
        """
        Create string output for a tuple type return.
        """
        smooth_df_string = hunitest.convert_df_to_string(output[0], index=True)
        detail_df_string = hunitest.convert_df_to_string(output[1], index=True)
        output_str = (
            f"smooth_df:\n{smooth_df_string}\n"
            f"\ndetail_df\n{detail_df_string}\n"
        )
        return output_str


class Test_compute_swt_var(hunitest.TestCase):
    def test1(self) -> None:
        srs = self._get_data(seed=0)
        swt_var = csiprswt.compute_swt_var(srs, depth=6)
        actual = swt_var.count().values[0]
        np.testing.assert_equal(actual, 1179)

    def test2(self) -> None:
        srs = self._get_data(seed=0)
        swt_var = csiprswt.compute_swt_var(srs, depth=6)
        actual = swt_var.sum()
        np.testing.assert_allclose(actual, [1102.66], atol=0.01)

    def test3(self) -> None:
        srs = self._get_data(seed=0)
        swt_var = csiprswt.compute_swt_var(srs, depth=6, axis=1)
        actual = swt_var.sum()
        np.testing.assert_allclose(actual, [1102.66], atol=0.01)

    def _get_data(self, seed: int) -> pd.Series:
        process = carsigen.ArmaProcess([], [])
        realization = process.generate_sample(
            {"start": "2000-01-01", "end": "2005-01-01", "freq": "B"}, seed=seed
        )
        return realization
