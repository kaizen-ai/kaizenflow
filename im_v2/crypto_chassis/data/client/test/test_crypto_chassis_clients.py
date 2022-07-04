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
    def helper(
        self,
        dataset: str,
        contract_type: str,
        data_snapshot: str,
        columns: list,
        expected_signature: str,
    ) -> None:
        universe_version = "v2"
        resample_1min = True
        aws_profile = "ck"
        s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
        root_dir = os.path.join(s3_bucket_path, "reorg", "historical.manual.pq")
        partition_mode = "by_year_month"
        client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=aws_profile,
        )
        full_symbols = ["binance::BTC_USDT", "binance::DOGE_USDT"]
        start_ts = pd.Timestamp("2022-05-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-15 16:00:00+00:00")
        filter_data_mode = "assert"
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            filter_data_mode,
        )
        expected_length = 362
        expected_column_names = columns
        expected_column_unique_values = {
            "full_symbol": ["binance::BTC_USDT", "binance::DOGE_USDT"]
        }
        self.check_df_output(
            df,
            expected_length,
            expected_column_names,
            expected_column_unique_values,
            expected_signature,
        )

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test1(self) -> None:
        """
        `dataset = bid_ask`
        `contract_type = futures`
        """
        dataset = "bid_ask"
        contract_type = "futures"
        data_snapshot = "20220620"
        columns = ["full_symbol", "bid_size"]
        expected_signature = r"""# df=
        index=[2022-05-15 13:00:00+00:00, 2022-05-15 16:00:00+00:00]
        columns=full_symbol,bid_size
        shape=(362, 2)
                                          full_symbol     bid_size
        timestamp
        2022-05-15 13:00:00+00:00   binance::BTC_USDT       94.184
        2022-05-15 13:00:00+00:00  binance::DOGE_USDT  1056883.000
        2022-05-15 13:01:00+00:00   binance::BTC_USDT       94.028
        ...
        2022-05-15 15:59:00+00:00  binance::DOGE_USDT  1944119.000
        2022-05-15 16:00:00+00:00   binance::BTC_USDT       70.693
        2022-05-15 16:00:00+00:00  binance::DOGE_USDT   961916.000
        """
        self.helper(
            dataset, contract_type, data_snapshot, columns, expected_signature
        )

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test2(self) -> None:
        """
        `dataset = ohlcv`
        `contract_type = spot`
        """
        dataset = "ohlcv"
        contract_type = "spot"
        data_snapshot = "20220530"
        columns = ["full_symbol", "close"]
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
        self.helper(
            dataset, contract_type, data_snapshot, columns, expected_signature
        )
