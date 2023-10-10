import numpy as np
import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.transform_utils as imvcdttrut

# TODO(Juraj): Move test into test_transform_utils
class TestResampleBidAskData(hunitest.TestCase):
    @staticmethod
    def get_test_data() -> pd.DataFrame:
        data = {
            "bid_price": [387.2, 386.68, 390.12, 380.2, 382.45, 389.5],
            "bid_size": [60.2, 32.21, 45.5, 5.9, 67.7, 89.8],
            "ask_price": [384.7, 387.8, 387.5, 382.2, 384.9, 386.5],
            "ask_size": [160.2, 80.21, 18.5, 14.9, 32.7, 97.8],
            "year": [2020] * 6,
            "month": [10] * 6,
        }
        index = pd.date_range(start="2020-01-10", periods=6, freq="20s", tz="UTC")
        #
        index1 = index
        index2 = index + pd.Timedelta("19s")
        df1 = pd.DataFrame(data, index=index1)
        df2 = pd.DataFrame(data, index=index2)
        #
        df = pd.concat([df1, df2]).sort_index()
        return df

    def test_resample_bid_ask_data1(self) -> None:
        """ """
        df = self.get_test_data()
        resampled_df = imvcdttrut.resample_bid_ask_data_from_1sec_to_1min(df)
        expected_signature = r"""
                           bid_price.close  bid_size.close  ask_price.close  ask_size.close  bid_price.high  bid_size.max  ask_price.high  ask_size.max  bid_price.low  bid_size.min  ask_price.low  ask_size.min  bid_price.mean  bid_size.mean  ask_price.mean  ask_size.mean
2020-01-10 00:01:00+00:00           390.12            45.5            387.5            18.5          390.12          60.2           387.8         160.2         386.68         32.21          384.7          18.5          388.00      45.970000      386.666667      86.303333
2020-01-10 00:02:00+00:00           389.50            89.8            386.5            97.8          389.50          89.8           386.5          97.8         380.20          5.90          382.2          14.9          384.05      54.466667      384.533333      48.466667
"""
        actual = hpandas.df_to_str(resampled_df)
        self.assert_equal(actual, expected_signature, fuzzy_match=True)
