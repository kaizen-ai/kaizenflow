import collections
import logging

import numpy as np
import pandas as pd

import core.artificial_signal_generators as sig_gen
import helpers.dataframe as hdf
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_filter_data_by_values1(hut.TestCase):
    def test_conjunction1(self) -> None:
        data = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        data = data.add_prefix("col_")
        filters = {"col_0": (1, 12), "col_1": (2, 11), "col_2": (3, 6)}
        info: collections.OrderedDict = collections.OrderedDict()
        filtered_data = hdf.filter_data_by_values(data, filters, "and", info)
        str_output = (
            f"{prnt.frame('data')}\n"
            f"{hut.convert_df_to_string(data, index=True)}\n"
            f"{prnt.frame('filters')}\n{filters}\n"
            f"{prnt.frame('filtered_data')}\n"
            f"{hut.convert_df_to_string(filtered_data, index=True)}\n"
            f"{hut.convert_info_to_string(info)}"
        )
        self.check_string(str_output)

    def test_disjunction1(self) -> None:
        data = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        data = data.add_prefix("col_")
        filters = {"col_0": (1, 12), "col_1": (2, 11), "col_2": (3, 6)}
        info: collections.OrderedDict = collections.OrderedDict()
        filtered_data = hdf.filter_data_by_values(data, filters, "or", info)
        str_output = (
            f"{prnt.frame('data')}\n"
            f"{hut.convert_df_to_string(data, index=True)}\n"
            f"{prnt.frame('filters')}\n{filters}\n"
            f"{prnt.frame('filtered_data')}"
            f"\n{hut.convert_df_to_string(filtered_data, index=True)}\n"
            f"{hut.convert_info_to_string(info)}"
        )
        self.check_string(str_output)


class Test_filter_data_by_comparison(hut.TestCase):
    def test_conjunction1(self) -> None:
        data = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
        data = data.add_prefix("col_")
        filters = {"col_0": (("gt", 1), ("lt", 7)), "col_1": ("eq", 5)}
        info: collections.OrderedDict = collections.OrderedDict()
        filtered_data = hdf.filter_data_by_comparison(data, filters, "and", info)
        str_output = (
            f"{prnt.frame('data')}\n"
            f"{hut.convert_df_to_string(data, index=True)}\n"
            f"{prnt.frame('filters')}\n{filters}\n"
            f"{prnt.frame('filtered_data')}\n"
            f"{hut.convert_df_to_string(filtered_data, index=True)}\n"
            f"{hut.convert_info_to_string(info)}"
        )
        self.check_string(str_output)

    def test_disjunction1(self) -> None:
        data = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
        data = data.add_prefix("col_")
        filters = {"col_0": ("gt", 2), "col_1": ("eq", 5)}
        info: collections.OrderedDict = collections.OrderedDict()
        filtered_data = hdf.filter_data_by_comparison(data, filters, "or", info)
        str_output = (
            f"{prnt.frame('data')}\n"
            f"{hut.convert_df_to_string(data, index=True)}\n"
            f"{prnt.frame('filters')}\n{filters}\n"
            f"{prnt.frame('filtered_data')}"
            f"\n{hut.convert_df_to_string(filtered_data, index=True)}\n"
            f"{hut.convert_info_to_string(info)}"
        )
        self.check_string(str_output)


class Test_apply_nan_mode(hut.TestCase):
    @staticmethod
    def _get_series_with_nans(seed: int) -> pd.Series:
        arparams = np.array([0.75, -0.25])
        maparams = np.array([0.65, 0.35])
        arma_process = sig_gen.ArmaProcess(arparams, maparams)
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range, seed=seed
        )
        series[:3] = np.nan
        series[-3:] = np.nan
        series[5:7] = np.nan
        return series

    def test1(self) -> None:
        series = self._get_series_with_nans(1)
        actual = hdf.apply_nan_mode(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        series = self._get_series_with_nans(1)
        actual = hdf.apply_nan_mode(series, nan_mode="ignore")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        series = self._get_series_with_nans(1)
        actual = hdf.apply_nan_mode(series, nan_mode="ffill")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        series = self._get_series_with_nans(1)
        actual = hdf.apply_nan_mode(series, nan_mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)
