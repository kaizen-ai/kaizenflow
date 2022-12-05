import argparse
import unittest.mock as umock

import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.talos.data.extract.download_historical_data as imvtdedhda


#@pytest.mark.skipif(
#    not henv.execute_repo_config_code("is_CK_S3_available()"),
#    reason="Run only if CK S3 is available",
#)
@pytest.mark.skip(reason="Talos as a vendor is deprecated.")
class TestDownloadHistoricalData1(hunitest.TestCase):
    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvtdedhda._parse()
        cmd = []
        cmd.extend(["--api_stage", "sandbox"])
        cmd.extend(["--data_type", "ohlcv"])
        cmd.extend(["--contract_type", "spot"])
        cmd.extend(["--start_timestamp", "2022-02-08"])
        cmd.extend(["--end_timestamp", "2022-02-09"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--universe", "v3"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/historical.manual.pq/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "api_stage": "sandbox",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "start_timestamp": "2022-02-08",
            "end_timestamp": "2022-02-09",
            "exchange_id": "binance",
            "universe": "v3",
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/historical.manual.pq/",
            "log_level": "INFO",
            "file_format": "parquet",
            "bid_ask_depth": None,
        }
        self.assertDictEqual(actual, expected)

    @umock.patch.object(imvcdeexut, "download_historical_data")
    def test_main(self, mock_download_historical: umock.MagicMock) -> None:
        """
        Smoke test to directly run `_main` function for coverage increase.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        kwargs = {
            "api_stage": "sandbox",
            "data_type": "ohlcv",
            "start_timestamp": "2021-12-31 23:00:00",
            "end_timestamp": "2022-01-01 01:00:00",
            "universe": "v1",
            "exchange_id": "binance",
            "file_format": "parquet",
            "incremental": False,
            "log_level": "INFO",
            "s3_path": "s3://mock_bucket",
            "aws_profile": "ck",
        }
        namespace = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = namespace
        # Run.
        imvtdedhda._main(mock_argument_parser)
        # Check call.
        self.assertEqual(len(mock_download_historical.call_args), 2)
        self.assertEqual(
            mock_download_historical.call_args.args[1].vendor, "talos"
        )