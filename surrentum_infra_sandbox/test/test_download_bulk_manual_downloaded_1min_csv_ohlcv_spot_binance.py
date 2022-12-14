import argparse
from typing import List
import unittest.mock as umock

import faker
import pytest

import helpers.henv as henv
import helpers.hunit_test as hunitest
import surrentum_infra_sandbox.download_bulk_manual_downloaded_1min_csv_ohlcv_spot_binance_v_1_0_0 as sisdbmohlcv

f = faker.Faker()

def _fake_binance_response() -> List[list]:
    """
    list of the random fake records as a binance response

    :return: Fake list of binance response
    """
    return [
        [
            ## Open time
            f.pyint(min_value=1499040000000, max_value=1499644799999),   
            ## Open
            f.pyfloat(min_value=0.00001, max_value=0.9),                 
            ## High
            f.pyfloat(min_value=0.00001, max_value=0.9),                 
            ## Low
            f.pyfloat(min_value=0.00001, max_value=0.9),                 
            ## Close
            f.pyfloat(min_value=0.00001, max_value=0.9),                 
            ## Volume
            f.pyfloat(min_value=1.00001, max_value=10000.0),             
            ## Close time
            f.pyint(min_value=1499040000000, max_value=1499644799999),   
            ## Quote asset volume
            f.pyfloat(min_value=1.00001, max_value=10000.0),             
            ## Number of trades
            f.pyfloat(min_value=1, max_value=500),                       
            ## Taker buy base asset volume
            f.pyfloat(min_value=1.00001, max_value=10000.0),             
            ## Taker buy quote asset volume
            f.pyfloat(min_value=1.00001, max_value=10000.0),             
            ## Ignore
            "17928899.62484339"                                          
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

    @umock.patch.object(sisdbmohlcv.pd.DataFrame, "to_csv")
    @umock.patch("surrentum_infra_sandbox.download_bulk_manual_downloaded_1min_csv_ohlcv_spot_binance_v_1_0_0._THROTTLE_DELAY_IN_SECS", 0.0)
    def test_main(self, mock_to_csv) -> None:
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
        mock_response = umock.MagicMock()
        mock_response.status_code = 200
        mock_response.json = umock.MagicMock(return_value = _fake_binance_response())
        with umock.patch.object(
            sisdbmohlcv.requests,
             "request",
              return_value=mock_response
        ) as mock_request:
            sisdbmohlcv._main(mock_argument_parser)
            mock_request.assert_called()
        mock_to_csv.assert_called_with(
            f"{kwargs['output_file']}.gz",
            index=False, compression='gzip'
        )
