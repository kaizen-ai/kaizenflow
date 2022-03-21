import argparse
import os.path
import unittest.mock as umock

try:
    import moto

    _HAS_MOTO = True
except ImportError:
    _HAS_MOTO = False
import pytest

import helpers.hs3 as hs3
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.download_historical_data as imvcdedhda

if _HAS_MOTO:

    @pytest.mark.skip("Enable after CMTask1292 is resolved.")
    class TestDownloadHistoricalData1(hunitest.TestCase):
        # Mocked AWS credentials.
        mock_aws_credentials_patch = umock.patch.dict(
            hs3.os.environ,
            {
                "AWS_ACCESS_KEY_ID": "mock_key_id",
                "AWS_SECRET_ACCESS_KEY": "mock_secret_access_key",
            },
        )
        mock_aws_credentials = None
        # Mocked bucket.
        mock_s3 = None
        bucket_name = "mock_bucket"
        moto_client = None
        # Secret needed for getting historical data.
        binance_secret = None

        def setUp(self) -> None:
            super().setUp()
            # It is very important that boto3 is imported after moto.
            # If not, boto3 will access real AWS.
            import boto3

            # Getting necessary secret before boto3 is mocked.
            if self.binance_secret is None:
                import helpers.hsecrets as hsecret

                self.binance_secret = hsecret.get_secret("binance")
            # Start boto3 mock.
            if self.mock_s3 is None:
                self.mock_s3 = moto.mock_s3()
            self.mock_s3.start()
            # Start aws credentials mock. Must be started after moto mock,
            # or it will be overridden by moto with `foobar` values.
            self.mock_aws_credentials = self.mock_aws_credentials_patch.start()
            # Initialize boto client and create bucket for testing.
            self.moto_client = boto3.client("s3")
            self.moto_client.create_bucket(Bucket=self.bucket_name)

        def tearDown(self) -> None:
            super().tearDown()
            # Stop boto3 mock.
            self.mock_s3.stop()
            # Stop mock aws credentials.
            self.mock_aws_credentials_patch.stop()

        @pytest.mark.slow
        @umock.patch.object(imvcdedhda.imvcdeexcl.hsecret, "get_secret")
        @umock.patch.object(imvcdedhda.hdateti, "get_current_time")
        def test_function_call1(
            self,
            mock_get_current_time: umock.MagicMock,
            mock_get_secret: umock.MagicMock,
        ) -> None:
            """
            Test function call with specific arguments that are mimicking
            command line arguments and comparing function output with
            predefined directory structure and file contents.
            """
            mock_get_current_time.return_value = (
                "2022-02-08 00:00:01.000000+00:00"
            )
            mock_get_secret.return_value = self.binance_secret
            mock_list_and_merge_patch = umock.patch.object(
                imvcdedhda.hparque, "list_and_merge_pq_files"
            )
            mock_list_and_merge = mock_list_and_merge_patch.start()
            self._test_function_call()
            # Stop mock so real function can be called for before/after comparison.
            mock_list_and_merge_patch.stop()
            # Check number of calls and args for current time.
            self.assertEqual(mock_get_current_time.call_count, 36)
            self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
            # Check calls to mocked `list_and_merge_pq_files`.
            self.assertEqual(mock_list_and_merge.call_count, 1)
            # Check args/kwargs that were used for function call.
            expected_args = mock_list_and_merge.call_args.args
            expected_kwargs = mock_list_and_merge.call_args.kwargs
            self.assertEqual(len(expected_args), 1)
            # Check first argument, `root_dir`.
            self.assertEqual(expected_args[0], "s3://mock_bucket/binance")
            # Check keyword arguments. In this case only `aws_profile`.
            self.assertDictEqual(expected_kwargs, {"aws_profile": "ck"})
            # TODO(Nikola): Move section below to `helpers/test/test_hparquet.py`, if possible.
            # Check bucket content before merge.
            parquet_meta_list_before = self.moto_client.list_objects(
                Bucket=self.bucket_name
            )["Contents"]
            parquet_path_list_before = [
                meta["Key"] for meta in parquet_meta_list_before
            ]
            self.assertEqual(len(parquet_path_list_before), 9)
            # Add extra parquet files.
            for path in parquet_path_list_before[::2]:
                copy_source = {"Bucket": self.bucket_name, "Key": path}
                self.moto_client.copy_object(
                    CopySource=copy_source,
                    Bucket=self.bucket_name,
                    Key=path.replace(".parquet", "_new.parquet"),
                )
            # Create single `data.parquet` file.
            for path in parquet_path_list_before[1:4:2]:
                copy_source = {"Bucket": self.bucket_name, "Key": path}
                self.moto_client.copy_object(
                    CopySource=copy_source,
                    Bucket=self.bucket_name,
                    Key=os.path.join(*path.split("/")[:-1], "data.parquet"),
                )
                self.moto_client.delete_object(Bucket=self.bucket_name, Key=path)
            # Check if edits are in place.
            updated_parquet_meta_list = self.moto_client.list_objects(
                Bucket=self.bucket_name
            )["Contents"]
            updated_parquet_path_list = [
                meta["Key"] for meta in updated_parquet_meta_list
            ]
            data_parquet_path_list = [
                path
                for path in updated_parquet_path_list
                if path.endswith("/data.parquet")
            ]
            self.assertEqual(len(updated_parquet_path_list), 14)
            self.assertEqual(len(data_parquet_path_list), 2)
            # Check bucket content after merge.
            imvcdedhda.hparque.list_and_merge_pq_files(
                *expected_args, **expected_kwargs
            )
            parquet_meta_list_after = self.moto_client.list_objects(
                Bucket=self.bucket_name
            )["Contents"]
            parquet_path_list_after = [
                meta["Key"] for meta in parquet_meta_list_after
            ]
            parquet_path_list_after.sort()
            expected_list = [
                "binance/currency_pair=ADA_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=AVAX_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=BNB_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=BTC_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=DOGE_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=EOS_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=ETH_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=LINK_USDT/year=2022/month=2/data.parquet",
                "binance/currency_pair=SOL_USDT/year=2022/month=2/data.parquet",
            ]
            self.assertListEqual(parquet_path_list_after, expected_list)

        def test_parser(self) -> None:
            """
            Tests arg parser for predefined args in the script.

            Mostly for coverage and to detect argument changes.
            """
            parser = imvcdedhda._parse()
            cmd = []
            cmd.extend(["--start_timestamp", "2022-02-08"])
            cmd.extend(["--end_timestamp", "2022-02-09"])
            cmd.extend(["--exchange_id", "binance"])
            cmd.extend(["--universe", "v03"])
            cmd.extend(["--sleep_time", "5"])
            cmd.extend(["--aws_profile", "ck"])
            cmd.extend(["--s3_path", "s3://cryptokaizen-data/realtime/"])
            args = parser.parse_args(cmd)
            actual = vars(args)
            expected = {
                "start_timestamp": "2022-02-08",
                "end_timestamp": "2022-02-09",
                "exchange_id": "binance",
                "universe": "v03",
                "sleep_time": 5,
                "incremental": False,
                "aws_profile": "ck",
                "s3_path": "s3://cryptokaizen-data/realtime/",
                "log_level": "INFO",
            }
            self.assertDictEqual(actual, expected)

        def _test_function_call(self) -> None:
            """
            Tests directly _run function for coverage increase.
            """
            # Precaution to ensure that we are using mocked botocore.
            import boto3

            # Initialize boto client and check buckets.
            client = boto3.client("s3")
            buckets = client.list_buckets()["Buckets"]
            self.assertEqual(len(buckets), 1)
            self.assertEqual(buckets[0]["Name"], self.bucket_name)
            # Prepare inputs.
            kwargs = {
                "start_timestamp": "2022-02-08",
                "end_timestamp": "2022-02-09",
                "exchange_id": "binance",
                "universe": "v03",
                "sleep_time": 1,
                "incremental": False,
                "aws_profile": "ck",
                "s3_path": f"s3://{self.bucket_name}/",
                "log_level": "INFO",
            }
            # Run.
            args = argparse.Namespace(**kwargs)
            imvcdedhda._run(args)
