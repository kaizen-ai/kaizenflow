import collections
import logging

import pandas as pd

import helpers.dataframe as hdf
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class Test_filter_data1(hut.TestCase):
    def test_conjunction1(self) -> None:
        data = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        data = data.add_prefix("col_")
        filters = {"col_0": (1, 12), "col_1": (2, 11), "col_2": (3, 6)}
        info: collections.OrderedDict = collections.OrderedDict()
        filtered_data = hdf.filter_data(data, filters, "and", info)
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
        filtered_data = hdf.filter_data(data, filters, "or", info)
        str_output = (
            f"{prnt.frame('data')}\n"
            f"{hut.convert_df_to_string(data, index=True)}\n"
            f"{prnt.frame('filters')}\n{filters}\n"
            f"{prnt.frame('filtered_data')}"
            f"\n{hut.convert_df_to_string(filtered_data, index=True)}\n"
            f"{hut.convert_info_to_string(info)}"
        )
        self.check_string(str_output)
