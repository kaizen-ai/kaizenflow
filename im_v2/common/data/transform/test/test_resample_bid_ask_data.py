import numpy as np
import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.common.data.transform.transform_utils as imvcdttrut

#TODO(Juraj): Move test into test_transform_utils
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
        index = pd.date_range(start="1/10/2020", periods=6, freq="20s", tz="utc")
        df = pd.DataFrame(data, index=index)
        return df

    def setUp(self) -> None:
        super().setUp()
        df = self.get_test_data()
        self._actual_vwap_df = imvcdttrut.resample_bid_ask_data_to_1min(df)
        self._actual_twap_df = imvcdttrut.resample_bid_ask_data_to_1min(df, mode="TWAP")

    def test_resample_bid_ask_data1(self) -> None:
        """
        `mode = VWAP`
        """
        expected_signature = r"""  bid_price  bid_size   ask_price  ask_size
        2020-01-10 00:01:00+00:00  388.041932    137.91  385.860446    258.91
        2020-01-10 00:02:00+00:00  386.243237    163.40  385.699519    145.40
        """
        actual = hpandas.df_to_str(self._actual_vwap_df)
        self.assert_equal(actual, expected_signature, fuzzy_match=True)

    def test_resample_bid_ask_data2(self) -> None:
        """
        `mode = VWAP`
        """
        decimal = 4
        # Check `bid_price`.
        bid_price1 = (387.2 * 60.2 + 386.68 * 32.21 + 390.12 * 45.5) / (
            60.2 + 32.21 + 45.5
        )
        np.testing.assert_almost_equal(
            float(self._actual_vwap_df["bid_price"].first("T")),
            bid_price1,
            decimal=decimal,
        )
        bid_price2 = (380.2 * 5.9 + 382.45 * 67.7 + 389.5 * 89.8) / (
            5.9 + 67.7 + 89.8
        )
        np.testing.assert_almost_equal(
            float(self._actual_vwap_df["bid_price"].last("T")),
            bid_price2,
            decimal=decimal,
        )
        # Check `ask_price`.
        ask_price1 = (384.7 * 160.2 + 387.8 * 80.21 + 387.5 * 18.5) / (
            160.2 + 80.21 + 18.5
        )
        np.testing.assert_almost_equal(
            float(self._actual_vwap_df["ask_price"].first("T")),
            ask_price1,
            decimal=decimal,
        )
        ask_price2 = (382.2 * 14.9 + 384.9 * 32.7 + 386.5 * 97.8) / (
            14.9 + 32.7 + 97.8
        )
        np.testing.assert_almost_equal(
            float(self._actual_vwap_df["ask_price"].last("T")),
            ask_price2,
            decimal=decimal,
        )

    def test_resample_bid_ask_data3(self) -> None:
        """
        `mode = TWAP`
        """
        decimal = 4
        # Check `bid_price`.
        bid_price1 = (60.2 + 32.21 + 45.5) / 3
        np.testing.assert_almost_equal(
            float(self._actual_twap_df["bid_price"].first("T")),
            bid_price1,
            decimal=decimal,
        )
        bid_price2 = (5.9 + 67.7 + 89.8) / 3
        np.testing.assert_almost_equal(
            float(self._actual_twap_df["bid_price"].last("T")),
            bid_price2,
            decimal=decimal,
        )
        # Check `ask_price`.
        ask_price1 = (160.2 + 80.21 + 18.5) / 3
        np.testing.assert_almost_equal(
            float(self._actual_twap_df["ask_price"].first("T")),
            ask_price1,
            decimal=decimal,
        )
        ask_price2 = (14.9 + 32.7 + 97.8) / 3
        np.testing.assert_almost_equal(
            float(self._actual_twap_df["ask_price"].last("T")),
            ask_price2,
            decimal=decimal,
        )

    def test_resample_bid_ask_data4(self) -> None:
        """
        `mode = TWAP`
        """
        expected_signature = r"""   bid_price  bid_size  ask_price  ask_size
        2020-01-10 00:01:00+00:00  45.970000    137.91  86.303333    258.91
        2020-01-10 00:02:00+00:00  54.466667    163.40  48.466667    145.40
        """
        actual = hpandas.df_to_str(self._actual_twap_df)
        self.assert_equal(actual, expected_signature, fuzzy_match=True)
