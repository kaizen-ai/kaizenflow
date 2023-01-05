import logging

import helpers.hunit_test as hunitest
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc

_LOG = logging.getLogger(__name__)


class TestImRawDataClient(hunitest.TestCase):
    def test_get_db_table_name1(self) -> None:
        """
        Test CCXT OHLCV futures table name.
        """
        signature = "periodic_daily.airflow.downloaded_1min.csv.ohlcv.futures.v7.ccxt.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        table_name = reader._get_db_table_name()
        self.assert_equal(table_name, "ccxt_ohlcv_futures")

    def test_get_db_table_name2(self) -> None:
        """
        Test CCXT bid-ask spot table name.
        """
        signature = "periodic_daily.airflow.downloaded_1sec.csv.bid_ask.spot.v7.ccxt.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        table_name = reader._get_db_table_name()
        self.assert_equal(table_name, "ccxt_bid_ask_raw")

    def test_get_db_table_name3(self) -> None:
        """
        Test CCXT bid-ask futures resampled table name.
        """
        signature = "periodic_daily.airflow.resampled_1min.csv.bid_ask.futures.v7.ccxt.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        table_name = reader._get_db_table_name()
        self.assert_equal(table_name, "ccxt_bid_ask_futures_resampled_1min")

    def test_build_s3_pq_file_path1(self) -> None:
        """
        Test CC bid-ask futures S3 path.
        """
        signature = "bulk.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        s3_path = reader._build_s3_pq_file_path()
        self.assert_equal(
            s3_path,
            "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis.resampled_1min/binance",
        )

    def test_build_s3_pq_file_path2(self) -> None:
        """
        Test CC bid-ask spot S3 path.
        """
        signature = "bulk.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        s3_path = reader._build_s3_pq_file_path()
        self.assert_equal(
            s3_path,
            "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.downloaded_1sec/binance",
        )

    def test_build_s3_pq_file_path3(self) -> None:
        """
        Test CCXT OHLCV futures S3 path.
        """
        signature = "bulk.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        s3_path = reader._build_s3_pq_file_path()
        self.assert_equal(
            s3_path,
            "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/ohlcv-futures/ccxt/binance",
        )