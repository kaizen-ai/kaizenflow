import logging

import pandas as pd

import core.dataflow as dtf
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_df_info_as_string(hunitest.TestCase):
    def test1(self):
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtf.get_df_info_as_string(df, exclude_memory_usage=False)
        self.check_string(info)

    def test2(self):
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtf.get_df_info_as_string(df)
        self.check_string(info)
