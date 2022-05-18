import argparse
import unittest.mock as umock

import pytest

import helpers.hgit as hgit
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.download_realtime_for_one_exchange as imvcdedrfoe
import im_v2.ccxt.data.extract.extractor as imvcdeex
import im_v2.common.data.extract.extract_utils as imvcdeexut


@pytest.mark.skipif(
    not hgit.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available")
class TestDownloadRealtimeForOneExchange1(hunitest.TestCase):
  
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 1000
      
    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvcdedrfoe._parse()
        cmd = []
        cmd.extend(["--start_timestamp", "20211110-101100"])
        cmd.extend(["--end_timestamp", "20211110-101200"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--universe", "v3"])
        cmd.extend(["--db_stage", "dev"])
        cmd.extend(["--db_table", "ccxt_ohlcv"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/realtime/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "20211110-101100",
            "end_timestamp": "20211110-101200",
            "exchange_id": "binance",
            "universe": "v3",
            "db_stage": "dev",
            "db_table": "ccxt_ohlcv",
            "incremental": False,
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/realtime/",
            "file_format": "parquet"
        }
        self.assertDictEqual(actual, expected)

    @umock.patch.object(imvcdeexut, "download_realtime_for_one_exchange")
    def test_main(self, mock_download_realtime: umock.MagicMock) -> None:
        """
        Smoke test to directly run `_main` function for coverage increase.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        kwargs = {
            "start_timestamp": "20211110-101100",
            "end_timestamp": "20211110-101200",
            "exchange_id": "binance",
            "universe": "v3",
            "db_stage": "local",
            "db_table": "ccxt_ohlcv",
            "incremental": False,
            "log_level": "INFO",
            "aws_profile": "ck",
            "s3_path": "s3://mock_bucket",
        }
        namespace = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = namespace
        # Run.
        imvcdedrfoe._main(mock_argument_parser)
        # Check call.
        self.assertEqual(len(mock_download_realtime.call_args), 2)
        self.assertEqual(mock_download_realtime.call_args.args[0], namespace)
        self.assertEqual(
            mock_download_realtime.call_args.args[1], imvcdeex.CcxtExtractor
        )
