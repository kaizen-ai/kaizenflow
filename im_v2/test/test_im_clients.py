import pandas as pd
import pytest

import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.crypto_chassis.data.client as iccdc


@pytest.mark.slow("Slow via GH, fast on server.")
class TestHistoricalImClients(icdctictc.ImClientTestCase):
    """
    Test existing ImClients with one test for each.
    """

    def test_ccxt_historical_pq_by_tile_client(self) -> None:
        """
        Test `CcxtHistoricalPqByTileClient`.

        - dataset = ohlcv
        - contract_type = spot
        """
        # Initialize the client.
        universe_version = "v4"
        resample_1min = False
        dataset = "ohlcv"
        contract_type = "spot"
        data_snapshot = "20220530"
        im_client = icdcl.get_CcxtHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_names = None
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,open,high,low,close,volume,knowledge_timestamp
        shape=(12, 7)
                                         full_symbol        open        high         low       close       volume knowledge_timestamp
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.7731      0.7745      0.7731      0.7741  40082.50000          2022-05-10
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37969.9900  37998.2500  37969.9900  37987.2000     14.82533          2022-05-10
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.7740      0.7741      0.7732      0.7739  14064.20000          2022-05-10
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37951.8100  37951.8100  37938.4900  37939.3600     10.20479          2022-05-10
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.7726      0.7731      0.7724      0.7727  42522.60000          2022-05-10
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37939.3600  37956.1000  37936.0400  37950.7500     15.30911          2022-05-10
        """
        # Check.
        self._test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    def test_crypto_chassis_historical_pq_by_tile_client(self) -> None:
        """
        Test `CryptoChassisHistoricalPqByTileClient`.

        - dataset = bid_ask
        - contract_type = futures
        """
        # Initialize the client.
        universe_version = "v3"
        resample_1min = True
        dataset = "bid_ask"
        contract_type = "futures"
        data_snapshot = "20220620"
        im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example1(
            universe_version,
            resample_1min,
            dataset,
            contract_type,
            data_snapshot,
        )
        # Set expected values.
        full_symbols = ["binance::BTC_USDT", "binance::ADA_USDT"]
        start_ts = pd.Timestamp("2022-05-01 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-01 13:05:00+00:00")
        expected_length = 12
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::ADA_USDT"]
        }
        expected_column_names = None
        expected_signature = r"""# df=
        index=[2022-05-01 13:00:00+00:00, 2022-05-01 13:05:00+00:00]
        columns=full_symbol,bid_price,bid_size,ask_price,ask_size
        shape=(12, 5)
                                         full_symbol     bid_price     bid_size     ask_price     ask_size
        timestamp
        2022-05-01 13:00:00+00:00  binance::ADA_USDT      0.773418  1577685.000      0.773433   741874.000
        2022-05-01 13:00:00+00:00  binance::BTC_USDT  37973.770035      228.977  37974.633461      175.528
        2022-05-01 13:01:00+00:00  binance::ADA_USDT      0.773116   599395.000      0.773162  1076322.000
        ...
        2022-05-01 13:04:00+00:00  binance::BTC_USDT  37925.827424     165.176  37926.802980     192.241
        2022-05-01 13:05:00+00:00  binance::ADA_USDT      0.772086  852299.000      0.772323  817526.000
        2022-05-01 13:05:00+00:00  binance::BTC_USDT  37925.604266     254.863  37925.031683     183.618
        """
        # Check.
        self._test_read_data5(
            im_client,
            full_symbols,
            start_ts,
            end_ts,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
