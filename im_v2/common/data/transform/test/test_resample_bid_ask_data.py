import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.resample_bid_ask_data as imvcdtrbad


class TestResampleBidAskData(hunitest.TestCase):
    def test_resample_bid_ask_data1(self) -> None:
        df = self._get_test_data()
        resampled_df = imvcdtrbad._resample_bid_ask_data(df)
        expected_signature = r"""     bid_price  bid_size  ask_price  ask_size exchange_id
        2020-01-10 00:00:00+00:00      387.2     60.20      384.7    160.20         ftx
        2020-01-10 00:01:00+00:00      380.2     83.61      382.2    113.61         ftx
        2020-01-10 00:02:00+00:00      389.5    157.50      386.5    130.50         ftx
        """
        actual = hpandas.df_to_str(resampled_df)
        self.assert_equal(actual, expected_signature, fuzzy_match=True)

    @staticmethod
    def _get_test_data() -> pd.DataFrame:
        data = {
            "bid_price": [387.2, 386.68, 390.12, 380.2, 382.45, 389.5],
            "bid_size": [60.2, 32.21, 45.5, 5.9, 67.7, 89.8],
            "ask_price": [384.7, 387.8, 387.5, 382.2, 384.9, 386.5],
            "ask_size": [160.2, 80.21, 18.5, 14.9, 32.7, 97.8],
            "exchange_id": ["ftx"] * 6,
            "year": [2020] * 6,
            "month": [10] * 6,
        }
        index = pd.date_range(start="1/10/2020", periods=6, freq="20s", tz="utc")
        df = pd.DataFrame(data, index=index)
        return df
