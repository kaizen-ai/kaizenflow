import argparse
import unittest.mock as umock

import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import surrentum_infra_sandbox.download_bulk_manual_downloaded_1min_csv_ohlcv_spot_binance_v_1_0_0 as sisdbmohlcv


class TestDownloadHistoricalOHLCV(hunitest.TestCase):
    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = sisdbmohlcv._parse()
        cmd = []
        cmd.extend(["--start_timestamp", "2022-10-20 10:00:00-04:00"])
        cmd.extend(["--end_timestamp", "2022-10-21 15:30:00-04:00"])
        cmd.extend(["--output_file", "test1.csv"])
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "2022-10-20 10:00:00-04:00",
            "end_timestamp": "2022-10-21 15:30:00-04:00",
            "output_file": "test1.csv"
        }
        self.assertDictEqual(actual, expected)

    @umock.patch("requests.request", return_value={})
    @umock.patch("pandas.DataFrame.to_csv")
    def test_main(
        self,
        dumb_request: umock.MagicMock,
        dumb_to_csv: umock.MagicMock,
    ) -> None:
        """
        Smoke test to directly run `_main` function for coverage increase.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        kwargs = {
            "start_timestamp": "2022-10-20 10:00:00-04:00",
            "end_timestamp": "2022-10-20 11:00:00-04:00",
            "output_file": "test1.csv"
        }
        namespace = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = namespace
        # Run.
        sisdbmohlcv._main(mock_argument_parser)
        # Check args.
        self.assertEqual(len(download_historical_mock.call_args), 3)
        actual_args = download_historical_mock.call_args.args
