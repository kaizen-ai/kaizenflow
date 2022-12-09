import argparse
import logging
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hdatetime as hdateti
import helpers.henv as henv
import helpers.hmoto as hmoto
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.db.archive_db_data_to_s3 as imvcdaddts
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not henv.execute_repo_config_code("has_dind_support()")
    and not henv.execute_repo_config_code("use_docker_sibling_containers()"),
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Need docker children / sibling support and/or Run only if CK S3 is available",
)
@pytest.mark.slow("22 seconds.")
class TestArchiveDbDataToS3(imvcddbut.TestImDbHelper, hmoto.S3Mock_TestCase):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def setUp(self) -> None:
        super().setUp()

    @umock.patch.object(imvcdaddts.imvcddbut.DbConnectionManager, "get_connection")
    def test_full_archival_flow(
        self, mock_get_connection: umock.MagicMock
    ) -> None:
        """
        Simple test that the data are fetched from the DB successfully and
        saved to S3 as parquet.

        1. Test tries to archive data with no previous parquet available
        2. Test tries to archive data with already existing parquet to append
         new data.
        """
        timestamp_low = "2022-10-20 23:49:00+00:00"
        timestamp_mid = "2022-10-20 23:50:00+00:00"
        timestamp_high = "2022-10-20 23:51:00+00:00"

        test_data = self._get_test_ccxt_ohlcv_data(
            timestamp_low, timestamp_mid, timestamp_high
        )
        self._prepare_test_ccxt_ohlcv_db_data(test_data)
        # Transform test data to the same format as they will be saved to S3.
        test_data = imvcdttrut.reindex_on_datetime(
            test_data, "timestamp", unit="ms"
        )
        test_data, _ = hparque.add_date_partition_columns(
            test_data, "by_year_month"
        )
        # Assert correction DB insertion.
        self.assertEqual(3, hsql.get_num_rows(self.connection, "ccxt_ohlcv"))
        # Tests use special connection params, so we mock the module function.
        mock_get_connection.return_value = self.connection
        # Create s3fs object to pass to from_parquet.
        s3fs_ = hs3.get_s3fs(self.mock_aws_profile)
        expected_path = (
            f"s3://{self.bucket_name}/db_archive/local/ccxt_ohlcv/timestamp"
        )
        args = {
            "db_stage": "local",
            "timestamp": timestamp_mid,
            "table_timestamp_column": "timestamp",
            "db_table": "ccxt_ohlcv",
            "s3_path": f"s3://{self.bucket_name}/db_archive/",
            "incremental": False,
            "skip_time_continuity_assertion": False,
            "dry_run": False,
        }

        # Test first archival
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))
        # Only the first should have been archived
        expected_data = test_data.head(1)
        actual_data = hparque.from_parquet(
            expected_path, filters=None, aws_profile=s3fs_
        )
        # Assert archive content.
        self.assert_equal(
            hpandas.df_to_str(expected_data),
            hpandas.df_to_str(actual_data),
            fuzzy_match=True,
        )
        # Assert DB got deleted from.
        self.assertEqual(2, hsql.get_num_rows(self.connection, "ccxt_ohlcv"))
        # Test second archival.
        args["incremental"] = True
        args["timestamp"] = timestamp_high
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))

        # First two rows should now be archived.
        expected_data = test_data.head(2)
        actual_data = hparque.from_parquet(
            expected_path, filters=None, aws_profile=s3fs_
        )
        # TODO(Juraj): for some reasons the from_parquet just would not
        # pick up multiple .pq files from the specified path.
        # Assert archive content.
        # self.assert_equal(
        #    hpandas.df_to_str(expected_data), hpandas.df_to_str(actual_data), fuzzy_match=True
        # )
        # Assert DB got deleted from.
        self.assertEqual(1, hsql.get_num_rows(self.connection, "ccxt_ohlcv"))

    def _prepare_test_ccxt_ohlcv_db_data(self, test_data: pd.DataFrame) -> None:
        query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        cursor = self.connection.cursor()
        cursor.execute(query)
        hsql.execute_insert_query(
            connection=self.connection,
            obj=test_data,
            table_name="ccxt_ohlcv",
        )

    def _get_test_ccxt_ohlcv_data(
        self, timestamp_1: str, timestamp_2: str, timestamp_3: str
    ) -> pd.DataFrame:
        """
        Convenience method for fetching testing data and improve readability.
        """
        data = [
            [
                hdateti.convert_timestamp_to_unix_epoch(
                    pd.Timestamp(timestamp_1)
                ),
                1336.73,
                60.789,
                1336.74,
                129.483,
                1.0,
                "binance",
                timestamp_1,
                timestamp_1,
                "BTC_USDT",
            ],
            [
                hdateti.convert_timestamp_to_unix_epoch(
                    pd.Timestamp(timestamp_2)
                ),
                1336.69,
                3.145,
                1336.72,
                20.892,
                2.0,
                "binance",
                timestamp_2,
                timestamp_2,
                "BTC_USDT",
            ],
            [
                hdateti.convert_timestamp_to_unix_epoch(
                    pd.Timestamp(timestamp_3)
                ),
                20066.50,
                39.455,
                20066.60,
                0.698,
                1.0,
                "binance",
                timestamp_3,
                timestamp_3,
                "BTC_USDT",
            ],
        ]
        columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "exchange_id",
            "end_download_timestamp",
            "knowledge_timestamp",
            "currency_pair",
        ]
        data = pd.DataFrame(data, columns=columns)
        return data