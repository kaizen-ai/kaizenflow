import logging

import helpers.hunit_test as hunitest
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
import pytest

_LOG = logging.getLogger(__name__)

# hasync, hpandas and hparquet all require docker
@pytest.mark.requires_docker
class TestImRawDataClient(hunitest.TestCase):
    def test_build_s3_pq_file_path1(self) -> None:
        """
        Test CC bid-ask futures S3 path.
        """
        signature = "bulk.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
        reader = imvcdcimrdc.RawDataReader(signature)
        s3_path = reader._build_s3_pq_file_path()
        self.assert_equal(
            s3_path,
            "s3://cryptokaizen-data/v3/bulk/airflow/resampled_1min/parquet/bid_ask/futures/v3/crypto_chassis/binance/v1_0_0",
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
            "s3://cryptokaizen-data/v3/bulk/airflow/downloaded_1sec/parquet/bid_ask/spot/v3/crypto_chassis/binance/v1_0_0",
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
            "s3://cryptokaizen-data/v3/bulk/airflow/downloaded_1min/parquet/ohlcv/futures/v7/ccxt/binance/v1_0_0",
        )
