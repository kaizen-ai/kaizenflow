import os

import pandas as pd

import helpers.hs3 as hs3
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.client as imvccdc
import research_amp.cc.qa as ramccqa


class TestGetBadDataStats(hunitest.TestCase):
    """
    Test `get_bad_data_stats` quality assurance stats by full symbol, year, month columns.
    """
    @staticmethod
    def _get_crypto_chassis_test_data() -> pd.DataFrame:
        universe_version = "v1"
        resample_1min = False
        aws_profile = "ck"
        s3_bucket_path = hs3.get_s3_bucket_path(aws_profile)
        root_dir = os.path.join(
            s3_bucket_path, "reorg", "historical.manual.pq"
        )
        partition_mode = "by_year_month"
        crypto_chassis_client = imvccdc.crypto_chassis_clients.CryptoChassisHistoricalPqByTileClient(
            universe_version,
            resample_1min,
            root_dir,
            partition_mode,
            aws_profile=aws_profile,
        )
        full_symbols = ["binance::ADA_USDT", "ftx::BTC_USDT"]
        start_ts = pd.Timestamp("2021-09-01T00:00:00-00:00")
        end_ts = pd.Timestamp("2021-09-03T23:59:59-00:00")
        columns = None
        filter_data_mode = "assert"
        data = crypto_chassis_client.read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            filter_data_mode
        )
        return data

    def test_get_bad_data_stats1(self) -> None:
        crypto_chassis_data = self._get_crypto_chassis_test_data()
        agg_level = ["full_symbol", "year", "month"]
        crypto_chassis_bad_data_stats = ramccqa.get_bad_data_stats(crypto_chassis_data, agg_level)
        crypto_chassis_bad_data_stats = hpandas.df_to_str(crypto_chassis_bad_data_stats)
        expected_signature = """
                                  NaNs [%]  missing bars [%]  volume=0 [%]  bad data [%]
        binance::ADA_USDT 2021 9  0.000000               0.0           0.0      0.000000
        ftx::BTC_USDT     2021 9  0.162037               0.0           0.0      0.162037
        """
        # Check.
        self.assert_equal(crypto_chassis_bad_data_stats, expected_signature, fuzzy_match=True)

    def test_get_bad_data_stats2(self) -> None:
        """
        agg_level = ["full_symbol"]
        """
        crypto_chassis_data = self._get_crypto_chassis_test_data()
        agg_level = ["full_symbol"]
        crypto_chassis_bad_data_stats = ramccqa.get_bad_data_stats(crypto_chassis_data, agg_level)
        crypto_chassis_bad_data_stats = hpandas.df_to_str(crypto_chassis_bad_data_stats)
        expected_signature = """
                           NaNs [%]  missing bars [%]  volume=0 [%]  bad data [%]
        binance::ADA_USDT  0.000000               0.0           0.0      0.000000
        ftx::BTC_USDT      0.162037               0.0           0.0      0.162037
        """
        # Check.
        self.assert_equal(crypto_chassis_bad_data_stats, expected_signature, fuzzy_match=True)
