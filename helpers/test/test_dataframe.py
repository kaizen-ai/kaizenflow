import collections
import logging
import os

import numpy as np
import pandas as pd

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


class TestFilterDataByMethod(hut.TestCase):
    """This test was generated automatically with Playback."""

    def test1(self) -> None:
        # Define input variables.
        input_path = os.path.join(self.get_input_dir(), "test.txt")
        data = pd.read_csv(input_path, index_col=0)
        filters = {
            "Frequency": {"isin": {"values": ["Monthly", "Weekly", "Daily"]}},
            "source_code": {"isin": {"values": ["WIND"]}},
            "is_downloaded": {"isin": {"values": ["success"]}},
        }
        mode = "and"
        info: collections.OrderedDict = collections.OrderedDict()
        # Call function to test.
        act = hdf.filter_data_by_method(
            data=data, filters=filters, mode=mode, info=info
        )
        act = hut.convert_df_to_string(act)
        act = str(act)
        # Check output.
        self.check_string(act)


class Test_apply_nan_mode(hut.TestCase):
    @staticmethod
    def _get_series_with_nans(seed: int) -> pd.Series:
        date_range = {"start": "1/1/2010", "periods": 40, "freq": "M"}
        series = hut.get_random_df(
            num_cols=1,
            seed=seed,
            **date_range,
        )[0]
        series[:3] = np.nan
        series[-3:] = np.nan
        series[5:7] = np.nan
        return series

    def test1(self) -> None:
        """
        Test for `mode=leave_unchanged`.
        """
        series = self._get_series_with_nans(seed=1)
        actual = hdf.apply_nan_mode(series)
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test for `mode="drop"`.
        """
        series = self._get_series_with_nans(seed=1)
        actual = hdf.apply_nan_mode(series, mode="drop")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test3(self) -> None:
        """
        Test for `mode="ffill"`.
        """
        series = self._get_series_with_nans(seed=1)
        actual = hdf.apply_nan_mode(series, mode="ffill")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test4(self) -> None:
        """
        Test for `mode="ffill_and_drop_leading"`.
        """
        series = self._get_series_with_nans(seed=1)
        actual = hdf.apply_nan_mode(series, mode="ffill_and_drop_leading")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test5(self) -> None:
        """
        Test for `mode="fill_with_zero"`.
        """
        series = self._get_series_with_nans(seed=1)
        actual = hdf.apply_nan_mode(series, mode="fill_with_zero")
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    # Smoke test for empty input.
    def test6(self) -> None:
        series = pd.Series([])
        hdf.apply_nan_mode(series)


class Test_compute_points_per_year_for_given_freq(hut.TestCase):
    def test1(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("T")
        np.testing.assert_equal(actual, 525780.125)

    def test2(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("B")
        np.testing.assert_equal(actual, 260.875)

    def test3(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("D")
        np.testing.assert_equal(actual, 365.25)

    def test4(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("W")
        np.testing.assert_equal(actual, 52.25)

    def test5(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("M")
        np.testing.assert_equal(actual, 12.0)

    def test6(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("Y")
        np.testing.assert_equal(actual, 1.0)

    def test7(self) -> None:
        actual = hdf.compute_points_per_year_for_given_freq("0D")
        np.testing.assert_equal(actual, 0.0)
