import argparse
import unittest.mock as umock

import moto
import s3fs

import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.download_historical_data as imvcdedhda


class TestDownloadHistoricalData1(hunitest.TestCase):
    # Mocked bucket.
    mock_s3 = moto.mock_s3()
    bucket_name = "mock_bucket"
    bucket = None

    def setUp(self) -> None:
        super().setUp()
        # It is very important that boto3 is imported after moto.
        # If not, boto3 will access real AWS.
        import boto3

        self.mock_s3.start()

        client = boto3.client("s3")
        self.bucket = client.create_bucket(Bucket=self.bucket_name)

    def tearDown(self) -> None:
        super().tearDown()
        self.mock_s3.stop()

    # TODO(Nikola): We are using two similar functions in scripts. Unify?
    @umock.patch.object(
        imvcdedhda.imvcdeexcl.hdateti, "get_current_timestamp_as_string"
    )
    @umock.patch.object(imvcdedhda.hdateti, "get_current_time")
    def test_function_call1(
        self,
        mock_get_current_time: umock.MagicMock,
        mock_get_current_timestamp_as_string: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and comparing function output with predefined directory
        structure and file contents.
        """
        mock_get_current_time.return_value = "2022-02-08 00:00:01.000000+00:00"
        mock_get_current_timestamp_as_string.return_value = "20220208-000001"
        mock_list_and_merge_patch = umock.patch.object(
            imvcdedhda, "list_and_merge_pq_files"
        )
        mock_list_and_merge = mock_list_and_merge_patch.start()
        self._test_function_call()
        mock_list_and_merge_patch.stop()
        # Check number of calls and args.
        self.assertEqual(mock_get_current_time.call_count, 27)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        self.assertEqual(mock_get_current_timestamp_as_string.call_count, 9)
        self.assertEqual(
            mock_get_current_timestamp_as_string.call_args.args, ("UTC",)
        )
        self.assertEqual(mock_list_and_merge.call_count, 1)
        expected_args = mock_list_and_merge.call_args.args
        expected_kwargs = mock_list_and_merge.call_args.kwargs
        self.assertEqual(len(expected_args), 2)
        self.assertEqual(expected_args[0], "s3://mock_bucket/binance")
        self.assertIsInstance(expected_args[1], s3fs.core.S3FileSystem)
        self.assertDictEqual(expected_kwargs, {"file_name": "data.parquet"})
        # Check directory structure before merge.
        parquet_files_before = expected_args[1].glob(
            f"{expected_args[0]}/**.parquet"
        )
        # pylint: disable=line-too-long
        expected_before = [
            "mock_bucket/binance/currency_pair=ADA_USDT/year=2022/month=2/16e075d8d7bb401c83f610001ecf3833.parquet",
            "mock_bucket/binance/currency_pair=AVAX_USDT/year=2022/month=2/fe58811224d54841bda27c8a778dfdb5.parquet",
            "mock_bucket/binance/currency_pair=BNB_USDT/year=2022/month=2/f6b95fa536cd4497b7785d2405df4cd3.parquet",
            "mock_bucket/binance/currency_pair=BTC_USDT/year=2022/month=2/31dc2332044840fcbb8268e8ff615bd6.parquet",
            "mock_bucket/binance/currency_pair=DOGE_USDT/year=2022/month=2/90d3eb0f02024d80a6b3a5333103eda0.parquet",
            "mock_bucket/binance/currency_pair=EOS_USDT/year=2022/month=2/3926d9815194476c906dc9ef266ed035.parquet",
            "mock_bucket/binance/currency_pair=ETH_USDT/year=2022/month=2/40df9efb3fed4c37a9aba9c07b56c114.parquet",
            "mock_bucket/binance/currency_pair=LINK_USDT/year=2022/month=2/98486ad9e51a4147840ac2753f2807de.parquet",
            "mock_bucket/binance/currency_pair=SOL_USDT/year=2022/month=2/3f3f608fe0cc4b9da260038c8ca65066.parquet",
        ]
        self.assertListEqual(sorted(parquet_files_before), expected_before)
        # Check directory structure after merge.
        imvcdedhda.list_and_merge_pq_files(*expected_args, **expected_kwargs)
        parquet_files_after = expected_args[1].glob(
            f"{expected_args[0]}/**.parquet"
        )
        # pylint: disable=line-too-long
        expected_after = [
            "mock_bucket/binance/currency_pair=ADA_USDT/year=2022/month=2/16e075d8d7bb401c83f610001ecf3833.parquet",
            "mock_bucket/binance/currency_pair=AVAX_USDT/year=2022/month=2/fe58811224d54841bda27c8a778dfdb5.parquet",
            "mock_bucket/binance/currency_pair=BNB_USDT/year=2022/month=2/f6b95fa536cd4497b7785d2405df4cd3.parquet",
            "mock_bucket/binance/currency_pair=BTC_USDT/year=2022/month=2/31dc2332044840fcbb8268e8ff615bd6.parquet",
            "mock_bucket/binance/currency_pair=DOGE_USDT/year=2022/month=2/90d3eb0f02024d80a6b3a5333103eda0.parquet",
            "mock_bucket/binance/currency_pair=EOS_USDT/year=2022/month=2/3926d9815194476c906dc9ef266ed035.parquet",
            "mock_bucket/binance/currency_pair=ETH_USDT/year=2022/month=2/40df9efb3fed4c37a9aba9c07b56c114.parquet",
            "mock_bucket/binance/currency_pair=LINK_USDT/year=2022/month=2/98486ad9e51a4147840ac2753f2807de.parquet",
            "mock_bucket/binance/currency_pair=SOL_USDT/year=2022/month=2/3f3f608fe0cc4b9da260038c8ca65066.parquet",
        ]
        self.assertListEqual(sorted(parquet_files_after), expected_after)

    def _test_function_call(self) -> None:
        """
        Tests directly _run function for coverage increase.
        """
        # Precaution to ensure that we are using mocked botocore.
        # TODO(Nikola): Mock env variables in setup/upper class instead?
        import boto3

        client = boto3.client("s3")
        buckets = client.list_buckets()["Buckets"]
        self.assertEqual(len(buckets), 1)
        self.assertEqual(buckets[0]["Name"], "mock_bucket")
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "2022-02-08",
            "end_timestamp": "2022-02-09",
            "exchange_id": "binance",
            "universe": "v03",
            "sleep_time": 1,
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": "s3://mock_bucket/",
            "log_level": "INFO",
        }
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdedhda._run(args)
