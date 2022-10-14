import argparse
import unittest.mock as umock

import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.crypto_chassis.data.extract.download_historical_data as imvccdedhd


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadHistoricalData1(hunitest.TestCase):
    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = imvccdedhd._parse()
        cmd = []
        cmd.extend(["--data_type", "ohlcv"])
        cmd.extend(["--start_timestamp", "2022-02-08"])
        cmd.extend(["--end_timestamp", "2022-02-09"])
        cmd.extend(["--exchange_id", "binance"])
        cmd.extend(["--contract_type", "spot"])
        cmd.extend(["--universe", "v3"])
        cmd.extend(["--aws_profile", "ck"])
        cmd.extend(["--s3_path", "s3://cryptokaizen-data/historical.manual.pq/"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "data_type": "ohlcv",
            "start_timestamp": "2022-02-08",
            "end_timestamp": "2022-02-09",
            "exchange_id": "binance",
            "contract_type": "spot",
            "universe": "v3",
            "incremental": False,
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data/historical.manual.pq/",
            "log_level": "INFO",
            "file_format": "parquet",
            "bid_ask_depth": None,
        }
        self.assertDictEqual(actual, expected)


    @umock.patch.object(
        imvccdedhd.imvccdexex, "CryptoChassisExtractor", autospec=True, spec_set=True
    )
    @umock.patch.object(imvcdeexut, "download_historical_data")
    def test_main(
        self, mock_download_historical: umock.MagicMock, chassis_extractor_mock: umock.MagicMock) -> None:
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
            "universe": "v1",
            "exchange_id": "binance",
            "contract_type": "spot",
            "file_format": "parquet",
            "incremental": False,
            "log_level": "INFO",
            "s3_path": "s3://mock_bucket",
            "aws_profile": "ck",
        }
        namespace = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = namespace
        # Run.
        imvccdedhd._main(mock_argument_parser)
        # Check call.
        self.assertEqual(len(mock_download_historical.call_args), 2)
        actual_args = mock_download_historical.call_args.args
        self.assertDictEqual(actual_args[0], {**kwargs, **{"unit": "s"}})
        # Verify that `CryptoChassisExtractor` instance is passed.
        self.assertEqual(actual_args[1]._extract_mock_name(), "CryptoChassisExtractor()")
        # Verify that `CryptoChassisExtractor` instance creation is properly called.
        self.assertEqual(chassis_extractor_mock.call_count, 1)
        actual_args = tuple(chassis_extractor_mock.call_args)
        expected_args = (("spot",), {})
        self.assertEqual(actual_args, expected_args)