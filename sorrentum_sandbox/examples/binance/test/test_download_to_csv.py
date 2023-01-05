import argparse
import unittest.mock as umock
import pytest
from typing import Any, List

import helpers.hunit_test as hunitest
import sorrentum_sandbox.examples.binance.download_to_csv as sosebidotc


def _fake_binance_response() -> List[List[Any]]:
    """
    Build fake records as a Binance response.
    """
    return [
        [
            # Open time.
            1499040000000,
            # Open.
            "0.01634790",
            # High.
            "0.80000000",
            # Low.
            "0.01575800",
            # Close.
            "0.01577100",
            # Volume.
            "148976.11427815",
            # Close time.
            1499644799999,
            # Quote asset volume.
            "2434.19055334",
            # Number of trades.
            308,
            # Taker buy base asset volume.
            "1756.87402397",
            # Taker buy quote asset volume.
            "28.46694368",
            # Ignore.
            "17928899.62484339",
        ]
        for _ in range(100)
    ]

# TODO(Juraj): this needs to be adapted after refactoring the code but most likely
#  we don't even need it.
#class TestDownloadHistoricalOhlcv(hunitest.TestCase):
#    def test_parser(self) -> None:
#        """
#        Test arg parser for predefined args in the script.
#        """
#        parser = sosebidotc._parse()
#        cmd = []
#        cmd.extend(["--start_timestamp", "2022-10-20 10:00:00-04:00"])
#        cmd.extend(["--end_timestamp", "2022-10-21 15:30:00-04:00"])
#        cmd.extend(["--output_file", "test1.csv"])
#        args = parser.parse_args(cmd)
#        actual = vars(args)
#        expected = {
#            "start_timestamp": "2022-10-20 10:00:00-04:00",
#            "end_timestamp": "2022-10-21 15:30:00-04:00",
#            "output_file": "test1.csv",
#        }
#        self.assertDictEqual(actual, expected)
#
#    @umock.patch.object(sisebido.pd.DataFrame, "to_csv")
#    def test_main(self, mock_to_csv) -> None:
#        # Prepare inputs.
#        mock_argument_parser = umock.create_autospec(
#            argparse.ArgumentParser, spec_set=True
#        )
#        kwargs = {
#            "start_timestamp": "2022-10-20 10:00:00-04:00",
#            "end_timestamp": "2022-10-20 11:00:00-04:00",
#            "output_file": "test1.csv",
#        }
#        namespace = argparse.Namespace(**kwargs)
#        mock_argument_parser.parse_args.return_value = namespace
#        # Run.
#        mock_response = umock.MagicMock()
#        mock_response.status_code = 200
#        mock_response.json = umock.MagicMock(
#            return_value=_fake_binance_response()
#        )
#        with umock.patch.object(
#            sisebido.requests, "request", return_value=mock_response
#        ) as mock_request:
#            sisebido._main(mock_argument_parser)
#            mock_request.assert_called()
#        mock_to_csv.assert_called_with(
#            f"{kwargs['output_file']}.gz", index=False, compression="gzip"
#        )
#