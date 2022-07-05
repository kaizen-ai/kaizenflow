import pandas as pd
import pytest

import helpers.henv as henv
import im_v2.common.data.client.test.im_client_test_case as icdctictc
import im_v2.crypto_chassis.data.client.crypto_chassis_clients_example as imvccdcccce


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestCryptoChassisHistoricalPqByTileClient1(icdctictc.ImClientTestCase):
    @pytest.mark.slow("Slow via GH, fast on the server")
    def test1(self) -> None:
        """
        `dataset = bid_ask`
        `contract_type = futures`
        """
        resample_1min = True
        dataset = "bid_ask"
        contract_type = "futures"
        data_snapshot = "20220620"
        client = imvccdcccce.get_CryptoChassisHistoricalPqByTileClient_example4(
            resample_1min, contract_type, dataset, data_snapshot
        )
        full_symbols = ["binance::BTC_USDT", "binance::DOGE_USDT"]
        start_ts = pd.Timestamp("2022-05-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-15 16:00:00+00:00")
        expected_signature = r"""# df=
        index=[2022-05-15 13:00:00+00:00, 2022-05-15 16:00:00+00:00]
        columns=full_symbol,bid_price,bid_size,ask_price,ask_size
        shape=(362, 5)
                                          full_symbol     bid_price     bid_size     ask_price    ask_size
        timestamp
        2022-05-15 13:00:00+00:00   binance::BTC_USDT  30274.410812       94.184  30276.087000      52.984
        2022-05-15 13:00:00+00:00  binance::DOGE_USDT      0.089351  1056883.000      0.089372  783002.000
        2022-05-15 13:01:00+00:00   binance::BTC_USDT  30263.589387       94.028  30271.038506      48.787
        ...
        2022-05-15 15:59:00+00:00  binance::DOGE_USDT      0.088676  1944119.000      0.088708  1063193.000
        2022-05-15 16:00:00+00:00   binance::BTC_USDT  30005.735776       70.693  30010.941933       66.659
        2022-05-15 16:00:00+00:00  binance::DOGE_USDT      0.088676   961916.000      0.088743  1415354.000
        """
        expected_length = 362
        expected_column_names = None
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::DOGE_USDT"]
        }
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_signature=expected_signature,
            expected_length=expected_length,
            expected_column_names=expected_column_names,
            expected_column_unique_values=expected_column_unique_values,
        )

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test2(self) -> None:
        """
        `dataset = ohlcv`
        `contract_type = spot`
        """
        resample_1min = True
        dataset = "ohlcv"
        contract_type = "spot"
        data_snapshot = "20220530"
        client = imvccdcccce.get_CryptoChassisHistoricalPqByTileClient_example4(
            resample_1min, contract_type, dataset, data_snapshot
        )
        full_symbols = ["binance::BTC_USDT", "binance::DOGE_USDT"]
        start_ts = pd.Timestamp("2022-05-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-15 16:00:00+00:00")
        expected_signature = r"""# df=
        index=[2022-05-15 13:00:00+00:00, 2022-05-15 16:00:00+00:00]
        columns=full_symbol,open,high,low,close,volume,vwap,number_of_trades,twap,knowledge_timestamp
        shape=(362, 10)
                                          full_symbol        open        high         low       close        volume          vwap  number_of_trades          twap              knowledge_timestamp
        timestamp
        2022-05-15 13:00:00+00:00   binance::BTC_USDT  30309.9900  30315.9100  30280.0000  30280.9500      45.13335  30291.409100               607  30291.172400 2022-05-31 14:38:23.953697+00:00
        2022-05-15 13:00:00+00:00  binance::DOGE_USDT      0.0895      0.0895      0.0894      0.0894  606842.00000      0.089435                49      0.089469 2022-05-31 14:38:29.472387+00:00
        2022-05-15 13:01:00+00:00   binance::BTC_USDT  30280.9500  30299.5700  30263.2800  30263.3100      39.45772  30278.419700               577  30278.335000 2022-05-31 14:38:23.953697+00:00
        ...
        2022-05-15 15:59:00+00:00  binance::DOGE_USDT      0.0888      0.0889      0.0886      0.0888  2.485897e+06      0.088763                56      0.088791 2022-05-31 14:38:29.472387+00:00
        2022-05-15 16:00:00+00:00   binance::BTC_USDT  30013.4600  30045.6300  30007.4500  30015.1900  4.861676e+01  30024.202100               977  30024.244600 2022-05-31 14:38:23.953697+00:00
        2022-05-15 16:00:00+00:00  binance::DOGE_USDT      0.0888      0.0889      0.0886      0.0887  4.286810e+05      0.088748                40      0.088750 2022-05-31 14:38:29.472387+00:00
        """
        expected_length = 362
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::DOGE_USDT"]
        }
        expected_column_names = None
        self._test_read_data5(
            client,
            full_symbols,
            start_ts,
            end_ts,
            expected_signature=expected_signature,
            expected_length=expected_length,
            expected_column_names=expected_column_names,
            expected_column_unique_values=expected_column_unique_values,
        )
