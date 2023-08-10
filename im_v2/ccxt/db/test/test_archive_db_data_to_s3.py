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
import helpers.hserver as hserver
import helpers.hs3 as hs3
import helpers.hsql as hsql
import helpers.hunit_test as hunitest
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

    @umock.patch.object(
        imvcdaddts.imvcddbut.DbConnectionManager, "get_connection"
    )
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
        self.assertEqual(6, hsql.get_num_rows(self.connection, "ccxt_ohlcv_spot"))
        # Tests use special connection params, so we mock the module function.
        mock_get_connection.return_value = self.connection
        # Create s3fs object to pass to from_parquet.
        s3fs_ = hs3.get_s3fs(self.mock_aws_profile)
        expected_path = f"s3://{self.bucket_name}/v3/bulk/airflow/archived_1min/parquet/ohlcv/spot/v7_3/ccxt/binance/v1_0_0/"
        args = {
            "db_stage": "local",
            "timestamp": timestamp_mid,
            "table_timestamp_column": "timestamp",
            "dataset_signature": "bulk.airflow.downloaded_1min.postgres.ohlcv.spot.v7_3.ccxt.binance.v1_0_0",
            "s3_path": f"s3://{self.bucket_name}/",
            "incremental": False,
            "skip_time_continuity_assertion": False,
            "dry_run": False,
            "mode": "archive_and_delete",
        }
        # Test first archival
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))
        # Only the first should have been archived.
        expected_data = test_data[test_data.exchange_id == "binance"].head(1)
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
        # Explanation: 6 rows were inserted (3 for Binance, 3 for OKX),
        # 1 from the binance was archived, 1 was deleted.
        self.assertEqual(5, hsql.get_num_rows(self.connection, "ccxt_ohlcv_spot"))
        # Test second archival.
        args["incremental"] = True
        args["timestamp"] = timestamp_high
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))
        # First two rows should now be archived.
        expected_data = test_data[test_data.exchange_id == "binance"].head(2)
        actual_data = hparque.from_parquet(
            expected_path, filters=None, aws_profile=s3fs_
        )
        # TODO(Juraj): for some reasons the from_parquet just would not
        # pick up multiple .pq files from the specified path.
        # Assert archive content.
        # self.assert_equal(
        #    hpandas.df_to_str(expected_data), hpandas.df_to_str(actual_data), fuzzy_match=True
        # )
        # Explanation:
        # 5 rows was before second archival (2 - binance, 3 - okx),
        # 1 from the binance was archived, 1 was deleted.
        self.assertEqual(4, hsql.get_num_rows(self.connection, "ccxt_ohlcv_spot"))
        okx_data = hsql.execute_query_to_df(
            self.connection,
            "SELECT * from ccxt_ohlcv_spot where exchange_id = 'okx'",
        )
        # Assert that the okx data were not deleted.
        self.assertEqual(3, len(okx_data))

    def _prepare_test_ccxt_ohlcv_db_data(self, test_data: pd.DataFrame) -> None:
        query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        cursor = self.connection.cursor()
        cursor.execute(query)
        hsql.execute_insert_query(
            connection=self.connection,
            obj=test_data,
            table_name="ccxt_ohlcv_spot",
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
        binance_data = pd.DataFrame(data, columns=columns)
        okx_data = binance_data.copy()
        okx_data["exchange_id"] = "okx"
        return pd.concat([binance_data, okx_data], ignore_index=True)


@pytest.mark.requires_aws 
@pytest.mark.requires_ck_infra
@pytest.mark.skipif(
    not hserver.is_dev_ck(),
    reason="Run only on CK infra"
)
class TestArchiveDbDataToS3Mode(hunitest.TestCase):
    def _prepare_test_mode(self, mode: str) -> dict:
        """
        Prepare the test mode environment.

        :param mode: mode to test:
            "archive_and_delete", "archive_only" or "delete_only".
        :return: arguments for the test.
        """
        # Prepare arguments.
        args = {
            "db_stage": "local",
            "timestamp": "2022-10-20 23:50:00+00:00",
            "table_timestamp_column": "timestamp",
            "dataset_signature": "bulk.airflow.downloaded_1min.postgres.ohlcv.spot.v7_3.ccxt.binance.v1_0_0",
            "s3_path": f"s3://some_mock_bucket/",
            "incremental": False,
            "skip_time_continuity_assertion": False,
            "dry_run": False,
            "mode": mode,
        }
        mock_fetch_data_by_age = umock.MagicMock()
        mock_fetch_data_by_age.return_value = pd.DataFrame({"col": ["value"]})
        imvcdaddts._assert_db_args = umock.MagicMock()
        imvcdaddts.imvcddbut = umock.MagicMock()
        imvcdaddts.imvcddbut.fetch_data_by_age = mock_fetch_data_by_age
        imvcdaddts.imvcdeexut = umock.MagicMock()
        return args

    def test_archive_and_delete_mode(self):
        """
        Test that the archive_and_delete mode works.
        """
        # Prepare arguments.
        args = self._prepare_test_mode("archive_and_delete")
        # Run.
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))
        # Check that the data was saved to S3 and deleted from DB.
        imvcdaddts.imvcdeexut.save_parquet.assert_called_once()
        imvcdaddts.imvcddbut.drop_db_data_by_age.assert_called_once()

    @pytest.mark.requires_aws 
    @pytest.mark.requires_ck_infra
    def test_archive_only_mode(self):
        """
        Test that the archive_only mode works.
        """
        # Prepare arguments.
        args = self._prepare_test_mode("archive_only")
        # Run.
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))
        # Check that the data was saved to S3 and not deleted from DB.
        imvcdaddts.imvcdeexut.save_parquet.assert_called_once()
        imvcdaddts.imvcddbut.drop_db_data_by_age.assert_not_called()

    @pytest.mark.requires_aws 
    @pytest.mark.requires_ck_infra
    def test_delete_only_mode(self):
        """
        Test that the delete_only mode works.
        """
        # Prepare arguments.
        args = self._prepare_test_mode("delete_only")
        # Run.
        imvcdaddts._archive_db_data_to_s3(argparse.Namespace(**args))
        # Check that the data was not saved to S3 and deleted from DB.
        imvcdaddts.imvcdeexut.save_parquet.assert_not_called()
        imvcdaddts.imvcddbut.drop_db_data_by_age.assert_called_once()
