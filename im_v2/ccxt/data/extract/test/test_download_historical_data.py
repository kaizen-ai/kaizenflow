import argparse
import unittest.mock as umock

import pytest

import helpers.hgit as hgit
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.download_historical_data as imvcdedhda
import im_v2.ccxt.data.extract.extractor as imvcdeex
import im_v2.common.data.extract.extract_utils as imvcdeexut


@pytest.mark.skipif(
    not hgit.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadHistoricalData1(hunitest.TestCase):
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
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/realtime/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "2022-02-08",
            "end_timestamp": "2022-02-09",
            "exchange_id": "binance",
            "universe": "v3",
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/realtime/",
            "log_level": "INFO",
            "file_format": "parquet"
        }
        self.assertDictEqual(actual, expected)

    @umock.patch.object(imvcdeexut, "download_historical_data")
    def test_main(self, mock_download_realtime: umock.MagicMock) -> None:
        """
        Smoke test to directly run `_main` function for coverage increase.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        kwargs = {
            "data_type": "ohlcv",
            "start_timestamp": "2021-12-31 23:00:00",
            "end_timestamp": "2022-01-01 01:00:00",
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
        imvcdedhda._main(mock_argument_parser)
        # Check call.
        self.assertEqual(len(mock_download_realtime.call_args), 2)
        print(mock_download_realtime.call_args.args[0])
        self.assertEqual(
            mock_download_realtime.call_args.args[1].exchange_id, "binance"
        )
