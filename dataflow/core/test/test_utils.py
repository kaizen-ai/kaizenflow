import logging

import pandas as pd

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
       ids = [i for i in range(num_rows):]
       data = ids
       df = pd.DataFrame({"time_stamps":time_stamps}, "ids":ids, "data":data)
       pivot_col_id = "ids"
       df = hunitest._convert_to_multiindex(df = df, asset_id_col = pivot_col_id)
       expected_df_format = "
                                      close       volume
                                13684 17085  13684 17085
      end_time
      2022-01-04 09:01:00-05:00   NaN   NaN      0     0
      2022-01-04 09:02:00-05:00   NaN   NaN      0     0
      2022-01-04 09:03:00-05:00   NaN   NaN      0     0
      2022-01-04 09:04:00-05:00   NaN   NaN      0     0
      " 
      info = dtfcorutil.get_df_info_as_string(df)
      self.check_string(info,expected_df_format)

