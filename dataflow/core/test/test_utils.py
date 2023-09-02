import logging

import pandas as pd
import pdb
import dataflow.core.utils as dtfcorutil
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class Test_get_df_info_as_string(hunitest.TestCase):
    def test1(self) -> None:
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtfcorutil.get_df_info_as_string(df, exclude_memory_usage=False)
        self.check_string(info)

    def test2(self) -> None:
        df = pd.DataFrame({"col_1": [1, 2], "col_2": [3, 4]})
        info = dtfcorutil.get_df_info_as_string(df)
        self.check_string(info)

class Test_convert_to_multiindex(hunitest.TestCase):
    def test1(self) -> None:
       num_rows = 5
       time_stamps = [
           pd.Timestamp("2000-01-01 9:00") + pd.Timedelta(minutes=i)
           for i in range(num_rows)
       ]
       ids = [i for i in range(num_rows)]
       data = ids
       df = pd.DataFrame({"time_stamps":time_stamps, "ids":ids, "data":data})
       pivot_col_id = "ids"
       df = dtfcorutil.convert_to_multiindex(df = df, asset_id_col = pivot_col_id)
       expected_df_format = """data                         time_stamps                                                                                \n   0   1   2   3   4                   0                   1                   2                   3                   4\n 0.0 NaN NaN NaN NaN 2000-01-01 09:00:00                 NaT                 NaT                 NaT                 NaT\n NaN 1.0 NaN NaN NaN                NaT 2000-01-01 09:01:00                 NaT                 NaT                 NaT\n NaN NaN 2.0 NaN                            NaN                 NaT                 NaT 2000-01-01 09:02:00                 NaT                                             NaT\n NaN NaN NaN 3.0 NaN                 NaT                 NaT                 NaT 2000-01-01 09:0                            3:00                 NaT\n NaN NaN NaN NaN 4.0                 NaT                 NaT                 NaT                 NaT 2000-01-01 09:04:00""" 
       info = df.to_string(index=False)
       pdb.set_trace()
       self.check_string(info)

