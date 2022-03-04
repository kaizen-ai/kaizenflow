import argparse
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hparquet as hparque
import helpers.hsql as hsql
import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut


@pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestCompareRealtimeAndHistoricalData1(imvcddbut.TestImDbHelper):
    S3_PATH = "s3://cryptokaizen-data/unit-test/parquet/historical"
    FILTERS = [
        [
            ("year", "==", 2021),
            ("month", ">=", 12),
            ("year", "==", 2021),
            ("month", "<=", 12),
        ],
        [
            ("year", "==", 2022),
            ("month", ">=", 1),
            ("year", "==", 2022),
            ("month", "<=", 1),
        ],
    ]
    _ohlcv_dataframe_sample = None

    def setUp(self) -> None:
        super().setUp()
        # Initialize database.
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)

    def tearDown(self) -> None:
        super().tearDown()
        # Drop table used in tests.
        ccxt_ohlcv_drop_query = "DROP TABLE IF EXISTS ccxt_ohlcv;"
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)

    def test_function_call1(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Both realtime and daily data are the same.
        """
        sample = self.ohlcv_dataframe_sample()
        self._save_sample_in_db(sample)
        self._test_function_call()

    def test_function_call2(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing realtime data.
        """
        sample = self.ohlcv_dataframe_sample().head(90)
        self._save_sample_in_db(sample)
        self._test_function_call()

    def test_function_call3(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Missing daily data.
        """
        sample = self.ohlcv_dataframe_sample()
        self._save_sample_in_db(sample)
        # Mock `from_parquet` so data is obtained through mock instead of s3.
        mock_from_parquet_patch = umock.patch.object(
            imvcdecrah.hparque, "from_parquet"
        )
        mock_from_parquet = mock_from_parquet_patch.start()
        mock_from_parquet.return_value = self.ohlcv_dataframe_sample().head(90)
        self._test_function_call()
        # Stop `from_parquet` mock and check calls and arguments.
        mock_from_parquet_patch.stop()
        self.assertEqual(mock_from_parquet.call_count, 1)
        self.assertEqual(
            mock_from_parquet.call_args.args,
            (f"{self.S3_PATH}/binance/",),
        )
        self.assertEqual(
            mock_from_parquet.call_args.kwargs,
            {
                "filters": self.FILTERS,
                "aws_profile": "ck",
            },
        )

    def test_function_call4(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Mismatch between realtime and daily data.
        """
        sample = self.ohlcv_dataframe_sample()
        # TODO(Nikola): Edit data.
        self._save_sample_in_db(sample)
        # Mock `from_parquet` so data is obtained through mock instead of s3.
        mock_from_parquet_patch = umock.patch.object(
            imvcdecrah.hparque, "from_parquet"
        )
        mock_from_parquet = mock_from_parquet_patch.start()
        # TODO(Nikola): Edit data.
        mock_parquet_sample = self.ohlcv_dataframe_sample
        mock_from_parquet.return_value = mock_parquet_sample
        self._test_function_call()
        mock_from_parquet_patch.stop()

    def test_parser(self) -> None:
        """
        Tests arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvcdecrah._parse()
        cmd = []
        cmd.extend(["--start_timestamp", "20220216-000000"])
        cmd.extend(["--end_timestamp", "20220217-000000"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--db_stage", "dev"])
        cmd.extend(["--db_table", "ccxt_ohlcv"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/historical/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "20220216-000000",
            "end_timestamp": "20220217-000000",
            "db_stage": "dev",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/historical/",
        }
        self.assertDictEqual(actual, expected)

    def ohlcv_dataframe_sample(self) -> pd.DataFrame:
        if self._ohlcv_dataframe_sample is None:
            file_name = f"{self.S3_PATH}/binance/"
            ohlcv_sample = hparque.from_parquet(
                file_name, filters=self.FILTERS, aws_profile="ck"
            )
            # Matching exact timespan as in test function call.
            self._ohlcv_dataframe_sample = ohlcv_sample.loc[
                "2021-12-31 23:55:00":"2022-01-01 00:05:00"
            ]
        # Deep copy (which is default for `pd.DataFrame.copy()`) is used to
        # preserve original data for each test.
        return self._ohlcv_dataframe_sample.copy()

    def _save_sample_in_db(self, sample: pd.DataFrame) -> None:
        """
        Save small sample of ohlcv data in test db.

        Extra date columns `year` and `month` are removed from sample as
        those are only used for data partition.
        """
        columns = ["year", "month"]
        trimmed_sample = sample.drop(columns, axis=1)
        hsql.execute_insert_query(
            connection=self.connection,
            obj=trimmed_sample,
            table_name="ccxt_ohlcv",
        )

    def _test_function_call(self) -> None:
        """
        Tests directly _run function for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "20211231-235500",
            "end_timestamp": "20220101-000500",
            "db_stage": "local",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": f"{self.S3_PATH}/",
        }
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdecrah._run(args)
