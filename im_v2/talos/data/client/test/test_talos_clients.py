from typing import List

import pandas as pd

import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.talos.data.client.talos_clients_example as imvtdctcex

# #############################################################################
# TestTalosParquetByTileClient1
# #############################################################################


class TestTalosParquetByTileClient1(icdctictc.ImClientTestCase):
    """
    For all the test methods see description of corresponding private method in
    the parent class.
    """

    def test_read_data5(self) -> None:
        talos_client = imvtdctcex.get_TalosParquetByTileClient_example1()
        full_symbols = ["binance::ADA_USDT", "coinbase::BTC_USDT"]
        start_ts = pd.Timestamp("2022-01-01T00:01:00-00:00")
        end_ts = pd.Timestamp("2022-01-01T00:05:00-00:00")
        #
        expected_length = 10
        expected_column_names = self.get_expected_column_names()
        expected_column_unique_values = {
            "full_symbol": ["binance::ADA_USDT", "coinbase::BTC_USDT"]
        }
        # pylint: disable=line-too-long
        expected_signature = r"""
        # df=
        index=[2022-01-01 00:01:00+00:00, 2022-01-01 00:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume
        shape=(10, 6)
                                          full_symbol        open        high         low       close           volume
        timestamp
        2022-01-01 00:01:00+00:00   binance::ADA_USDT  1.31000000  1.31400000  1.30800000  1.31200000  132189.40000000
        2022-01-01 00:01:00+00:00  coinbase::BTC_USDT    46219.13    46311.86    46219.13    46306.76       0.10478747
        2022-01-01 00:02:00+00:00   binance::ADA_USDT  1.31200000  1.31800000  1.31100000  1.31700000  708964.20000000
        ...
        2022-01-01 00:04:00+00:00  coinbase::BTC_USDT    46336.69    46336.69    46307.73    46307.73      0.04327141
        2022-01-01 00:05:00+00:00   binance::ADA_USDT  1.31500000  1.31800000  1.31300000  1.31800000  75423.50000000
        2022-01-01 00:05:00+00:00  coinbase::BTC_USDT    46292.52    46392.47    46287.87    46392.47      0.11990593
        """
        # pylint: enable=line-too-long
        self._test_read_data5(
            talos_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    # ////////////////////////////////////////////////////////////////////////

    @staticmethod
    def get_expected_column_names() -> List[str]:
        """
        Return a list of expected column names.
        """
        expected_column_names = [
            "full_symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        return expected_column_names