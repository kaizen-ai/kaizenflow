import os

import pandas as pd
import pytest

import helpers.henv as henv
import helpers.hs3 as hs3
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.client.crypto_chassis_clients as imvccdcccc


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestCryptoChassisHistoricalPqByTileClient1(hunitest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.resample_1min = True
        self.aws_profile = "ck"
        s3_bucket_path = hs3.get_s3_bucket_path(self.aws_profile)
        self.root_dir = os.path.join(
            s3_bucket_path, "reorg", "historical.manual.pq"
        )
        self.partition_mode = "by_year_month"
        self.filter_data_mode = "assert"

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test1(self) -> None:
        """
        `dataset = bid_ask`
        `contract_type = futures`
        """
        universe_version = "v2"
        dataset = "bid_ask"
        contract_type = "futures"
        data_snapshot = "20220620"
        client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            self.resample_1min,
            self.root_dir,
            self.partition_mode,
            dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=self.aws_profile,
        )
        full_symbols = ["binance::BTC_USDT", "binance::DOGE_USDT"]
        start_ts = pd.Timestamp("2022-06-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-06-15 16:00:00+00:00")
        columns = ["full_symbol", "bid_size"]
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            self.filter_data_mode,
        )
        expected_length = 362
        expected_column_names = columns
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::DOGE_USDT"]
        }
        expected_signature = r"""# df=
        index=[2022-06-15 13:00:00+00:00, 2022-06-15 16:00:00+00:00]
        columns=full_symbol,bid_size
        shape=(362, 2)
                                          full_symbol     bid_size
        timestamp
        2022-06-15 13:00:00+00:00   binance::BTC_USDT       93.832
        2022-06-15 13:00:00+00:00  binance::DOGE_USDT  2130364.000
        2022-06-15 13:01:00+00:00   binance::BTC_USDT       75.307
        ...
        2022-06-15 15:59:00+00:00  binance::DOGE_USDT  2682318.000
        2022-06-15 16:00:00+00:00   binance::BTC_USDT       73.926
        2022-06-15 16:00:00+00:00  binance::DOGE_USDT  3317330.000
        """
        self.check_df_output(
            df,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test2(self) -> None:
        """
        `dataset = ohlcv`
        `contract_type = spot`
        """
        universe_version = "v2"
        dataset = "ohlcv"
        contract_type = "spot"
        data_snapshot = "20220530"
        client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            self.resample_1min,
            self.root_dir,
            self.partition_mode,
            dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=self.aws_profile,
        )
        full_symbols = ["binance::BTC_USDT", "binance::DOGE_USDT"]
        start_ts = pd.Timestamp("2022-05-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-15 16:00:00+00:00")
        columns = ["full_symbol", "close"]
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            self.filter_data_mode,
        )
        expected_length = 362
        expected_column_names = columns
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::DOGE_USDT"]
        }
        expected_signature = r"""# df=
        index=[2022-05-15 13:00:00+00:00, 2022-05-15 16:00:00+00:00]
        columns=full_symbol,close
        shape=(362, 2)
                                          full_symbol       close
        timestamp
        2022-05-15 13:00:00+00:00   binance::BTC_USDT  30280.9500
        2022-05-15 13:00:00+00:00  binance::DOGE_USDT      0.0894
        2022-05-15 13:01:00+00:00   binance::BTC_USDT  30263.3100
        ...
        2022-05-15 15:59:00+00:00  binance::DOGE_USDT      0.0888
        2022-05-15 16:00:00+00:00   binance::BTC_USDT  30015.1900
        2022-05-15 16:00:00+00:00  binance::DOGE_USDT      0.0887
        """
        self.check_df_output(
            df,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )
