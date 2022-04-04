import pandas as pd

import im_v2.common.data.client.data_frame_im_clients_example as imvcdcdfimce
import im_v2.common.data.client.test.im_client_test_case as icdctictc

# #############################################################################
# TestHistoricalPqByTileClient1
# #############################################################################


# TODO(Nina): CmTask1589 "Extend tests for `DataFrameImClient`".
class TestDataFrameImClient1(icdctictc.ImClientTestCase):
    def test_read_data5(self) -> None:
        # Initialize client.
        im_client = imvcdcdfimce.get_DataFrameImClient_example1()
        # Set expected values.
        expected_length = 10
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "feature1",
        ]
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "binance::BTC_USDT"]
        }
        expected_signature = r"""# df=
        index=[2000-01-01 09:34:00+00:00, 2000-01-01 09:38:00+00:00]
        columns=full_symbol,open,high,low,close,volume,feature1
        shape=(10, 7)
                                         full_symbol  open  high  low  close  volume  feature1
        timestamp
        2000-01-01 09:34:00+00:00  binance::ADA_USDT   100   101   99  101.0       3       1.0
        2000-01-01 09:34:00+00:00  binance::BTC_USDT   100   101   99  101.0       3       1.0
        2000-01-01 09:35:00+00:00  binance::ADA_USDT   100   101   99  101.0       4       1.0
        ...
        2000-01-01 09:37:00+00:00  binance::BTC_USDT   100   101   99  100.0       6      -1.0
        2000-01-01 09:38:00+00:00  binance::ADA_USDT   100   101   99  100.0       7      -1.0
        2000-01-01 09:38:00+00:00  binance::BTC_USDT   100   101   99  100.0       7      -1.0"""
        # Run test.
        full_symbols = ["binance::ADA_USDT", "binance::BTC_USDT"]
        start_timestamp = pd.Timestamp("2000-01-01 09:34:00+00:00")
        end_timestamp = pd.Timestamp("2000-01-01 09:38:00+00:00")
        self._test_read_data5(
            im_client,
            full_symbols,
            start_timestamp,
            end_timestamp,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
