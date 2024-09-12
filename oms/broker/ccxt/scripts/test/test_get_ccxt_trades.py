import argparse
import unittest.mock as umock

import dataflow.universe as dtfuniver
import helpers.hdatetime as hdateti
import helpers.hunit_test as hunitest
import oms.broker.ccxt.ccxt_broker_instances as obccbrin
import oms.broker.ccxt.ccxt_utils as obccccut
import oms.broker.ccxt.scripts.get_ccxt_trades as obcsgcctr


class TestGetCcxtTrades1(hunitest.TestCase):
    """
    Unit tests for the `get_ccxt_trades.py` script.
    """

    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = obcsgcctr._parse()
        cmd = [
            "--start_timestamp",
            "2024-06-04",
            "--end_timestamp",
            "2024-06-05",
            "--log_dir",
            "tmp",
            "--secret_id",
            "4",
            "--universe",
            "v8.1",
            "--exchange",
            "binance",
            "--contract_type",
            "futures",
            "--stage",
            "preprod",
        ]
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "start_timestamp": "2024-06-04",
            "end_timestamp": "2024-06-05",
            "log_dir": "tmp",
            "secret_id": 4,
            "universe": "v8.1",
            "exchange": "binance",
            "contract_type": "futures",
            "stage": "preprod",
            "log_level": "INFO",
        }
        self.assertDictEqual(actual, expected)

    def test_get_ccxt_trades(
        self,
    ) -> None:
        """
        Smoke test to check execution of `get_ccxt_trades.py` script.
        """
        # Prepare inputs.
        start_timestamp = "2024-06-04"
        end_timestamp = "2024-06-05"
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        # Assign args to call the function.
        log_dir = self.get_scratch_space()
        kwargs = {
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "log_dir": log_dir,
            "universe": "v8.1",
            "secret_id": 4,
            "exchange": "binance",
            "contract_type": "futures",
            "stage": "preprod",
            "log_level": "DEBUG",
        }
        mock_parse = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = mock_parse
        with umock.patch.object(
            obccbrin.icdcl, "get_CcxtSqlRealTimeImClient_example1"
        ) as mock_im_client, umock.patch.object(
            obccbrin.imvcdcimrdc,
            "get_bid_ask_realtime_raw_data_reader",
        ) as mock_raw_data_reader, umock.patch.object(
            obccccut, "ccxt", spec=obccccut.ccxt
        ) as mock_ccxt:
            obcsgcctr._main(mock_argument_parser)
        # Check symbols.
        arg_list = mock_ccxt.binance().fetchMyTrades.call_args_list
        universe = dtfuniver.get_universe("ccxt_v8_1-all")
        expected_symbols = [univ.split("::")[1] for univ in universe]
        actual_symbols = [
            cell[1]["symbol"].split(":")[0].replace("/", "_") for cell in arg_list
        ]
        self.assertEqual(actual_symbols, expected_symbols)
        # Check start timestamp.
        expected_start_timestamp = hdateti.str_to_timestamp(
            start_timestamp, "UTC"
        )
        actual_start_timestamp = hdateti.convert_unix_epoch_to_timestamp(
            arg_list[0][1]["since"]
        )
        self.assertEqual(actual_start_timestamp, expected_start_timestamp)
        # Check end timestamp.
        expected_end_timestamp = hdateti.str_to_timestamp(end_timestamp, "UTC")
        actual_end_timestamp = hdateti.convert_unix_epoch_to_timestamp(
            arg_list[0][1]["params"]["endTime"]
        )
        self.assertEqual(actual_end_timestamp, expected_end_timestamp)
