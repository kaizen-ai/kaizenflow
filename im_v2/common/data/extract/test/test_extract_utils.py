import unittest.mock as umock

import pytest

import helpers.henv as henv
import helpers.hmoto as hmoto
import helpers.hpandas as hpandas
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.extract.extractor as ivcdexex
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.db.db_utils as imvcddbut


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadRealtimeForOneExchange1(
    hmoto.S3Mock_TestCase, imvcddbut.TestImDbHelper
):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

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

    def call_download_realtime_for_one_exchange(self, use_s3: bool) -> None:
        """
        Test directly function call for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "20211110-101100",
            "end_timestamp": "20211110-101200",
            "exchange_id": "binance",
            "universe": "v3",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "db_stage": "local",
            "db_table": "ccxt_ohlcv",
            "incremental": False,
            "log_level": "INFO",
            "aws_profile": None,
            "s3_path": None,
            "connection": self.connection,
        }
        extractor = ivcdexex.CcxtExtractor(
            kwargs["exchange_id"], kwargs["contract_type"]
        )
        if use_s3:
            # Update kwargs.
            kwargs.update(
                {
                    "aws_profile": self.mock_aws_profile,
                    "s3_path": f"s3://{self.bucket_name}/",
                }
            )
        # Run.
        imvcdeexut.download_realtime_for_one_exchange(kwargs, extractor)
        # Get saved data in db.
        select_all_query = "SELECT * FROM ccxt_ohlcv;"
        actual_df = hsql.execute_query_to_df(self.connection, select_all_query)
        # Check data output.
        actual = hpandas.df_to_str(actual_df, num_rows=5000, max_colwidth=15000)
        # pylint: disable=line-too-long
        expected = r"""id timestamp open high low close volume currency_pair exchange_id end_download_timestamp knowledge_timestamp
                        0 1 1636569000000 2.2440 2.2450 2.2410 2.2410 9.389970e+04 ADA_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        1 2 1636569000000 90.3800 90.4400 90.2800 90.2800 4.519300e+02 AVAX_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        2 3 1636569000000 643.5000 643.7000 643.1000 643.2000 3.907190e+02 BNB_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        3 4 1636569000000 68225.0400 68238.7900 68160.4100 68171.3100 1.118236e+01 BTC_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        4 5 1636569000000 0.2697 0.2697 0.2694 0.2696 1.239735e+06 DOGE_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        5 6 1636569000000 5.2640 5.2670 5.2600 5.2620 1.260730e+04 EOS_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        6 7 1636569000000 4812.1100 4812.8200 4806.6000 4806.6000 9.595610e+01 ETH_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        7 8 1636569000000 37.1600 37.2100 37.1400 37.1400 4.097370e+03 LINK_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00
                        8 9 1636569000000 241.6600 241.6700 241.4100 241.4300 7.629000e+02 SOL_USDT binance 2021-11-10 00:00:01+00:00 2021-11-10 00:00:01+00:00"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.slow
    @umock.patch.object(ivcdexex.hdateti, "get_current_timestamp_as_string")
    @umock.patch.object(imvcdeexut.hdateti, "get_current_time")
    @umock.patch.object(ivcdexex.hsecret, "get_secret")
    def test_function_call1(
        self,
        mock_get_secret: umock.MagicMock,
        mock_get_current_time: umock.MagicMock,
        mock_get_current_timestamp_as_string: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and checking saved content in database.

        Run without saving to s3.
        """
        # Set mock return values.
        mock_get_secret.return_value = self.binance_secret
        mock_get_current_time.return_value = "2021-11-10 00:00:01.000000+00:00"
        mock_get_current_timestamp_as_string.return_value = "20211110-000001"
        # Run.
        use_s3 = False
        self.call_download_realtime_for_one_exchange(use_s3)
        # Check mock state.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        self.assertEqual(mock_get_current_timestamp_as_string.call_count, 0)
        self.assertEqual(mock_get_current_timestamp_as_string.call_args, None)

    @pytest.mark.skip(reason="CMTask2089")
    @umock.patch.object(ivcdexex.hdateti, "get_current_timestamp_as_string")
    @umock.patch.object(imvcdeexut.hdateti, "get_current_time")
    @umock.patch.object(ivcdexex.hsecret, "get_secret")
    def test_function_call2(
        self,
        mock_get_secret: umock.MagicMock,
        mock_get_current_time: umock.MagicMock,
        mock_get_current_timestamp_as_string: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and checking saved content in database.

        Run and save to s3.
        """
        # Set mock return values.
        mock_get_secret.return_value = self.binance_secret
        mock_get_current_time.return_value = "2021-11-10 00:00:01.000000+00:00"
        mock_get_current_timestamp_as_string.return_value = "20211110-000001"
        # Run.
        use_s3 = True
        self.call_download_realtime_for_one_exchange(use_s3)
        # Check mock state.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        self.assertEqual(mock_get_current_timestamp_as_string.call_count, 9)
        self.assertEqual(
            mock_get_current_timestamp_as_string.call_args.args, ("UTC",)
        )
        # Prepare common `hs3.listdir` params.
        s3_bucket = f"s3://{self.bucket_name}"
        pattern = "*.csv"
        only_files = True
        use_relative_paths = True
        # Check csv files on s3.
        csv_path_list = hs3.listdir(
            s3_bucket,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=self.mock_aws_profile,
        )
        csv_path_list.sort()
        expected = [
            "binance/ADA_USDT_20211110-000001.csv",
            "binance/AVAX_USDT_20211110-000001.csv",
            "binance/BNB_USDT_20211110-000001.csv",
            "binance/BTC_USDT_20211110-000001.csv",
            "binance/DOGE_USDT_20211110-000001.csv",
            "binance/EOS_USDT_20211110-000001.csv",
            "binance/ETH_USDT_20211110-000001.csv",
            "binance/LINK_USDT_20211110-000001.csv",
            "binance/SOL_USDT_20211110-000001.csv",
        ]
        self.assertListEqual(csv_path_list, expected)


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadHistoricalData1(hmoto.S3Mock_TestCase):
    def call_download_historical_data(self, incremental: bool) -> None:
        """
        Test directly function call for coverage increase.
        """
        # Prepare inputs.
        args = {
            "start_timestamp": "2021-12-31 23:00:00",
            "end_timestamp": "2022-01-01 01:00:00",
            "exchange_id": "binance",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "universe": "v3",
            "incremental": incremental,
            "aws_profile": self.mock_aws_profile,
            "s3_path": f"s3://{self.bucket_name}/",
            "log_level": "INFO",
            "file_format": "parquet",
            "unit": "ms",
        }
        exchange = ivcdexex.CcxtExtractor(
            args["exchange_id"], args["contract_type"]
        )
        imvcdeexut.download_historical_data(args, exchange)

    @pytest.mark.skip(reason="CMTask2089")
    @umock.patch.object(imvcdeexut.hparque, "list_and_merge_pq_files")
    @umock.patch.object(ivcdexex.hsecret, "get_secret")
    @umock.patch.object(imvcdeexut.hdateti, "get_current_time")
    def test_function_call1(
        self,
        mock_get_current_time: umock.MagicMock,
        mock_get_secret: umock.MagicMock,
        mock_list_and_merge: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and comparing function output with predefined directory
        structure and file contents.
        """
        # Set mock return values.
        mock_get_current_time.return_value = "2022-02-08 00:00:01.000000+00:00"
        mock_get_secret.return_value = self.binance_secret
        # Create path for incremental mode.
        s3fs_ = hs3.get_s3fs(self.mock_aws_profile)
        with s3fs_.open("s3://mock_bucket/binance/dummy.txt", "w") as f:
            f.write("test")
        # Run.
        incremental = True
        self.call_download_historical_data(incremental)
        # Check mock state.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        expected_args = mock_list_and_merge.call_args.args
        expected_kwargs = mock_list_and_merge.call_args.kwargs
        self.assertEqual(len(expected_args), 1)
        self.assertEqual(expected_args[0], "s3://mock_bucket/binance")
        self.assertDictEqual(
            expected_kwargs,
            {
                "aws_profile": self.mock_aws_profile,
                "drop_duplicates_mode": "ohlcv",
            },
        )
        # Prepare common `hs3.listdir` params.
        s3_bucket = f"s3://{self.bucket_name}"
        pattern = "*.parquet"
        only_files = True
        use_relative_paths = True
        # Check parquet files on s3.
        parquet_path_list = hs3.listdir(
            s3_bucket,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=self.mock_aws_profile,
        )
        parquet_path_list.sort()
        parquet_path_list = [
            # Remove uuid names.
            "/".join(path.split("/")[:-1])
            for path in parquet_path_list
        ]
        expected_list = [
            "binance/currency_pair=ADA_USDT/year=2021/month=12",
            "binance/currency_pair=ADA_USDT/year=2022/month=1",
            "binance/currency_pair=AVAX_USDT/year=2021/month=12",
            "binance/currency_pair=AVAX_USDT/year=2022/month=1",
            "binance/currency_pair=BNB_USDT/year=2021/month=12",
            "binance/currency_pair=BNB_USDT/year=2022/month=1",
            "binance/currency_pair=BTC_USDT/year=2021/month=12",
            "binance/currency_pair=BTC_USDT/year=2022/month=1",
            "binance/currency_pair=DOGE_USDT/year=2021/month=12",
            "binance/currency_pair=DOGE_USDT/year=2022/month=1",
            "binance/currency_pair=EOS_USDT/year=2021/month=12",
            "binance/currency_pair=EOS_USDT/year=2022/month=1",
            "binance/currency_pair=ETH_USDT/year=2021/month=12",
            "binance/currency_pair=ETH_USDT/year=2022/month=1",
            "binance/currency_pair=LINK_USDT/year=2021/month=12",
            "binance/currency_pair=LINK_USDT/year=2022/month=1",
            "binance/currency_pair=SOL_USDT/year=2021/month=12",
            "binance/currency_pair=SOL_USDT/year=2022/month=1",
        ]
        self.assertListEqual(parquet_path_list, expected_list)

    def test_function_call2(self) -> None:
        """
        Verify error on non incremental run.
        """
        s3fs_ = hs3.get_s3fs(self.mock_aws_profile)
        with s3fs_.open("s3://mock_bucket/binance/dummy.txt", "w") as f:
            f.write("test")
        incremental = False
        with pytest.raises(AssertionError) as fail:
            self.call_download_historical_data(incremental)
        self.assertIn(
            "S3 path 's3://mock_bucket/binance' already exist!", str(fail.value)
        )

    def test_function_call3(self) -> None:
        """
        Verify error on incremental run.
        """
        incremental = True
        with pytest.raises(AssertionError) as fail:
            self.call_download_historical_data(incremental)
        self.assertIn(
            "S3 path 's3://mock_bucket/binance' doesn't exist!", str(fail.value)
        )
