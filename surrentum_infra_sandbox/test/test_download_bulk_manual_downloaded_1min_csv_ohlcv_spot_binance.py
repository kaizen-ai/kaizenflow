import argparse
from typing import List
import unittest.mock as umock

import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import surrentum_infra_sandbox.download_bulk_manual_downloaded_1min_csv_ohlcv_spot_binance_v_1_0_0 as sisdbmohlcv


def _fake_binance_response() -> List[list]:
    """
    Dumb list of the same fake records as a binance response

    :return: Fake list of binance response
    """
    return [
        [
            1499040000000,      ## Open time
            "0.01634790",       ## Open
            "0.80000000",       ## High
            "0.01575800",       ## Low
            "0.01577100",       ## Close
            "148976.11427815",  ## Volume
            1499644799999,      ## Close time
            "2434.19055334",    ## Quote asset volume
            308,                ## Number of trades
            "1756.87402397",    ## Taker buy base asset volume
            "28.46694368",      ## Taker buy quote asset volume
            "17928899.62484339" ## Ignore
        ]
        for line in range(100)
    ]


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

    @umock.patch("pandas.DataFrame.to_csv")
    def test_main(
        self,
        dumb_to_csv: umock.Mock,
    ) -> None:
        mock_requests = umock.MagicMock()
        mock_request = umock.MagicMock()
        mock_response = umock.MagicMock()
        mock_response.return_value = 200
        mock_requests.request = mock_request
        sisdbmohlcv.requests = mock_requests
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
