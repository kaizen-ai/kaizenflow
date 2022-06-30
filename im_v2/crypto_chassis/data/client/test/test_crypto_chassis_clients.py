import os

import pandas as pd
import pytest

import helpers.henv as henv
import helpers.hpandas as hpandas
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
        self.dataset = "ohlcv"
        self.filter_data_mode = "assert"

    def test1(self) -> None:
        """
        `contract_type = futures`
        """
        universe_version = "v2"
        contract_type = "futures"
        data_snapshot = "20220620"
        client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            self.resample_1min,
            self.root_dir,
            self.partition_mode,
            self.dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=self.aws_profile,
        )
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2022-06-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-06-15 16:00:00+00:00")
        columns = ["full_symbol", "open", "high", "low"]
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            self.filter_data_mode,
        )
        df = hpandas.df_to_str(df)
        expected_signature = r"""
                                        full_symbol     open     high      low
        timestamp
        2022-06-15 13:00:00+00:00  binance::BTC_USDT  21183.7  21211.7  21156.0
        2022-06-15 13:01:00+00:00  binance::BTC_USDT  21162.8  21178.9  21142.0
        2022-06-15 13:02:00+00:00  binance::BTC_USDT  21158.0  21209.0  21158.0
        ...
        2022-06-15 15:58:00+00:00  binance::BTC_USDT  21396.0  21408.8  21384.2
        2022-06-15 15:59:00+00:00  binance::BTC_USDT  21402.0  21419.1  21393.0
        2022-06-15 16:00:00+00:00  binance::BTC_USDT  21408.8  21429.0  21367.9
        """
        self.assert_equal(df, expected_signature, fuzzy_match=True)

    @pytest.mark.slow("Slow via GH, fast on the server")
    def test2(self) -> None:
        """
        `contract_type = spot`
        """
        universe_version = "v2"
        contract_type = "spot"
        data_snapshot = "20220530"
        client = imvccdcccc.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            self.resample_1min,
            self.root_dir,
            self.partition_mode,
            self.dataset,
            contract_type,
            data_snapshot=data_snapshot,
            aws_profile=self.aws_profile,
        )
        full_symbols = ["binance::BTC_USDT"]
        start_ts = pd.Timestamp("2022-05-15 13:00:00+00:00")
        end_ts = pd.Timestamp("2022-05-15 16:00:00+00:00")
        columns = ["full_symbol", "open", "high", "low"]
        df = client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            self.filter_data_mode,
        )
        df = hpandas.df_to_str(df)
        expected_signature = r"""
                                        full_symbol      open      high       low
        timestamp
        2022-05-15 13:00:00+00:00  binance::BTC_USDT  30309.99  30315.91  30280.00
        2022-05-15 13:01:00+00:00  binance::BTC_USDT  30280.95  30299.57  30263.28
        2022-05-15 13:02:00+00:00  binance::BTC_USDT  30263.31  30277.61  30234.97
        ...
        2022-05-15 15:58:00+00:00  binance::BTC_USDT  29960.20  29976.90  29941.20
        2022-05-15 15:59:00+00:00  binance::BTC_USDT  29976.90  30040.40  29968.33
        2022-05-15 16:00:00+00:00  binance::BTC_USDT  30013.46  30045.63  30007.45
        """
        self.assert_equal(df, expected_signature, fuzzy_match=True)
