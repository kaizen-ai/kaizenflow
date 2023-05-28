import argparse
import unittest.mock as umock

import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import im_v2.common.data.extract.download_bulk as imvcdedobu
import im_v2.common.data.extract.extract_utils as imvcdeexut


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadBulkData1(hunitest.TestCase):
    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvcdedobu._parse()
        cmd = []
        cmd.extend(["--download_mode", "periodic_daily"])
        cmd.extend(["--downloading_entity", "manual"])
        cmd.extend(["--action_tag", "downloaded_1min"])
        cmd.extend(["--data_type", "ohlcv"])
        cmd.extend(["--contract_type", "spot"])
        cmd.extend(["--vendor", "ccxt"])
        cmd.extend(["--start_timestamp", "2022-02-08"])
        cmd.extend(["--end_timestamp", "2022-02-09"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--universe", "v3"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data-test/"])
        cmd.extend(["--data_format", "parquet"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "download_mode": "periodic_daily",
            "downloading_entity": "manual",
            "action_tag": "downloaded_1min",
            "data_type": "ohlcv",
            "vendor": "ccxt",
            "start_timestamp": "2022-02-08",
            "end_timestamp": "2022-02-09",
            "exchange_id": "binance",
            "contract_type": "spot",
            "universe": "v3",
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data-test/",
            "log_level": "INFO",
            "data_format": "parquet",
            "bid_ask_depth": None,
            "universe_part": None,
            "assert_on_missing_data": False,
        }
        self.assertDictEqual(actual, expected)

    @umock.patch.object(
        imvcdedobu.imvcdexex, "CcxtExtractor", autospec=True, spec_set=True
    )
    @umock.patch.object(
        imvcdeexut, "download_historical_data", autospec=True, spec_set=True
    )
    def test_main(
        self,
        download_historical_mock: umock.MagicMock,
        ccxt_extractor_mock: umock.MagicMock,
    ) -> None:
        """
        Smoke test to directly run `_main` function for coverage increase.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        kwargs = {
            "download_mode": "bulk",
            "downloading_entity": "manual",
            "action_tag": "downloaded_1min",
            "data_type": "ohlcv",
            "vendor": "ccxt",
            "start_timestamp": "2021-12-31 23:00:00",
            "end_timestamp": "2022-01-01 01:00:00",
            "universe": "v3",
            "exchange_id": "binance",
            "contract_type": "spot",
            "data_format": "parquet",
            "incremental": False,
            "log_level": "INFO",
            "s3_path": "s3://mock_bucket",
            "aws_profile": "ck",
        }
        namespace = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = namespace
        # Run.
        imvcdedobu._main(mock_argument_parser)
        # Check args.
        self.assertEqual(len(download_historical_mock.call_args), 2)
        actual_args = download_historical_mock.call_args.args
        self.assertDictEqual(actual_args[0], {**kwargs, **{"unit": "ms"}})
        # Verify that `CcxtExtractor` instance is passed.
        self.assertEqual(actual_args[1]._extract_mock_name(), "CcxtExtractor()")
        # Verify that `CcxtExtractor` instance creation is properly called.
        self.assertEqual(ccxt_extractor_mock.call_count, 1)
        actual_args = tuple(ccxt_extractor_mock.call_args)
        expected_args = (("binance", "spot"), {})
        self.assertEqual(actual_args, expected_args)
