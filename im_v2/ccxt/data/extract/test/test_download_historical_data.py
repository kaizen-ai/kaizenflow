import argparse
import unittest.mock as umock

import pytest

import helpers.hmoto as hmoto
import helpers.hgit as hgit
import im_v2.ccxt.data.extract.download_historical_data as imvcdedhda


@pytest.mark.skipif(
    not hgit.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available")
class TestDownloadHistoricalData1(hmoto.S3Mock_TestCase):
    # Secret needed for getting historical data.
    binance_secret = None

    def setUp(self) -> None:
        # Getting necessary secret before boto3 is mocked.
        if self.binance_secret is None:
            import helpers.hsecrets as hsecret

            self.binance_secret = hsecret.get_secret("binance")
        super().setUp()

    @pytest.mark.slow("Around 15s")
    @umock.patch.object(imvcdedhda.hparque, "list_and_merge_pq_files")
    @umock.patch.object(imvcdedhda.imvcdeexcl.hsecret, "get_secret")
    @umock.patch.object(imvcdedhda.hdateti, "get_current_time")
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
        mock_get_current_time.return_value = "2022-02-08 00:00:01.000000+00:00"
        mock_get_secret.return_value = self.binance_secret
        # TODO(Nikola): Remove comments below and use it in docs, CMTask #1349.
        self._test_function_call()
        # Check number of calls and args for current time.
        self.assertEqual(mock_get_current_time.call_count, 18)
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
        parquet_meta_list = self.moto_client.list_objects(
            Bucket=self.bucket_name
        )["Contents"]
        parquet_path_list = [
            # Remove uuid names.
            "/".join(meta["Key"].split("/")[:-1])
            for meta in parquet_meta_list
        ]
        parquet_path_list.sort()
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

    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvcdedhda._parse()
        cmd = []
        cmd.extend(["--start_timestamp", "2022-02-08"])
        cmd.extend(["--end_timestamp", "2022-02-09"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--universe", "v3"])
        cmd.extend(["--sleep_time", "5"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/realtime/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "2022-02-08",
            "end_timestamp": "2022-02-09",
            "exchange_id": "binance",
            "universe": "v3",
            "sleep_time": 5,
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/realtime/",
            "log_level": "INFO",
        }
        self.assertDictEqual(actual, expected)

    def _test_function_call(self) -> None:
        """
        Test directly _run function for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "2021-12-31 23:00:00",
            "end_timestamp": "2022-01-01 01:00:00",
            "exchange_id": "binance",
            "universe": "v3",
            "sleep_time": 1,
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": f"s3://{self.bucket_name}/",
            "log_level": "INFO",
        }
        # Run.
        args = argparse.Namespace(**kwargs)
        imvcdedhda._run(args)
