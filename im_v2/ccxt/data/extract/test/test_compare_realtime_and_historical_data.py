import argparse
import io

import moto
import pandas as pd

import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.db.db_utils as imvcddbut
import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah


# @pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestCompareRealtimeAndHistoricalData1(imvcddbut.TestImDbHelper):
    # Mocked bucket.
    mock_s3 = moto.mock_s3()
    bucket_name = "mock_bucket"
    moto_client = None

    def setUp(self) -> None:
        super().setUp()
        # It is very important that boto3 is imported after moto.
        # If not, boto3 will access real AWS.
        import boto3

        # Start boto3 mock.
        self.mock_s3.start()
        # Initialize boto client and create bucket for testing.
        self.moto_client = boto3.client("s3")
        self.moto_client.create_bucket(Bucket=self.bucket_name)
        # Initialize database.
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)

    def tearDown(self) -> None:
        super().tearDown()
        # Stop boto3 mock.
        self.mock_s3.stop()
        # Drop table used in tests.
        ccxt_ohlcv_drop_query = "DROP TABLE IF EXISTS ccxt_ohlcv;"
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)

    def test_function_call1(self) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments.

        Both realtime and historical data are the same.
        """
        sample = self._get_ohlcv_dataframe_sample()
        self._save_sample_in_db(sample)
        self._save_sample_on_mocked_s3(sample)
        self._test_function_call()

    def test_parser(self) -> None:
        """
        Tests arg parser for predefined args in the script.

        Mostly for coverage.
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

    def _save_sample_on_mocked_s3(self, sample: pd.DataFrame) -> None:
        """
        Save small sample of ohlcv data via `moto` on s3.
        """
        columns = ["currency_pair", "year", "month"]
        hparque.to_partitioned_parquet(
            sample,
            columns,
            f"s3://{self.bucket_name}/binance/",
            filesystem=hs3.get_s3fs("ck"),
        )

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
            "start_timestamp": "20220208-000000",
            "end_timestamp": "20220208-000900",
            "db_stage": "local",
            "exchange_id": "binance",
            "db_table": "ccxt_ohlcv",
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": f"s3://{self.bucket_name}/binance/",
        }
        # Precaution to ensure that we are using mocked botocore.
        import boto3

        # Initialize boto client and check buckets.
        client = boto3.client("s3")
        buckets = client.list_buckets()["Buckets"]
        self.assertEqual(len(buckets), 1)
        self.assertEqual(buckets[0]["Name"], self.bucket_name)
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdecrah._run(args)

    @staticmethod
    def _get_ohlcv_dataframe_sample() -> pd.DataFrame:
        # pylint: disable=line-too-long
        df_string = """timestamp_index,timestamp,open,high,low,close,volume,end_download_timestamp,currency_pair,exchange_id,year,month,knowledge_timestamp
2022-02-08 00:00:00+00:00,1644278400000,1.198,1.202,1.198,1.201,291406.8,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:01:00+00:00,1644278460000,1.200,1.204,1.200,1.204,381973.5,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:02:00+00:00,1644278520000,1.204,1.208,1.203,1.208,288773.9,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:03:00+00:00,1644278580000,1.208,1.209,1.205,1.205,285991.5,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:04:00+00:00,1644278640000,1.205,1.206,1.201,1.201,196455.1,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:05:00+00:00,1644278700000,1.201,1.201,1.197,1.197,202025.8,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:06:00+00:00,1644278760000,1.196,1.198,1.195,1.195,245695.3,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:07:00+00:00,1644278820000,1.196,1.199,1.195,1.197,157989.7,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:08:00+00:00,1644278880000,1.197,1.201,1.196,1.201,130493.2,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00
2022-02-08 00:09:00+00:00,1644278940000,1.200,1.202,1.197,1.199,176062.8,2022-02-08 00:00:01.000000+00:00,ADA_USDT,binance,2022,2,2022-02-08 00:00:02.000000+00:00"""
        # Text I/O implementation using an in-memory buffer.
        df_string_buffer = io.StringIO(df_string)
        df = pd.read_csv(df_string_buffer)
        indexed_df = df.set_index("timestamp_index")
        return indexed_df
