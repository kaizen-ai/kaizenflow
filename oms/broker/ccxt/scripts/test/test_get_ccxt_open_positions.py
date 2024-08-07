import argparse
import unittest.mock as umock

import helpers.hunit_test as hunitest
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.common.db.db_utils as imvcddbut
import im_v2.common.universe as ivcu
import oms.broker.ccxt.ccxt_utils as obccccut
import oms.broker.ccxt.scripts.get_ccxt_open_positions as obcsgcopo


class TestGetCcxtOpenPositions(hunitest.TestCase):
    """
    Unit tests for the `get_ccxt_open_positions.py` script.
    """

    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = obcsgcopo._parse()
        cmd = [
            "--exchange",
            "binance",
            "--contract_type",
            "futures",
            "--stage",
            "preprod",
            "--secret_id",
            "4",
            "--universe",
            "v8.1",
            "--log_dir",
            "tmp",
        ]
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "exchange": "binance",
            "contract_type": "futures",
            "stage": "preprod",
            "secret_id": 4,
            "log_dir": "tmp",
            "universe": "v8.1",
            "log_level": "INFO",
        }
        self.assertDictEqual(actual, expected)

    def test_get_ccxt_open_positions(
        self,
    ) -> None:
        """
        Smoke test to check execution of `get_ccxt_open_positions.py` script.
        """
        # Prepare inputs.
        mock_argument_parser = umock.create_autospec(
            argparse.ArgumentParser, spec_set=True
        )
        # Assign args to call the function.
        log_dir = self.get_scratch_space()
        kwargs = {
            "exchange": "binance",
            "contract_type": "futures",
            "stage": "preprod",
            "secret_id": 4,
            "log_dir": log_dir,
            "universe": "v8.1",
            "log_level": "INFO",
        }
        mock_parse = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = mock_parse
        with umock.patch.object(
            obccccut, "ccxt", spec=obccccut.ccxt
        ) as mock_ccxt, umock.patch.object(
            imvcddbut.DbConnectionManager,
            "get_connection",
        ) as mock_db_connection, umock.patch.object(
            imvcdccccl.CcxtSqlRealTimeImClient,
            "get_universe",
        ) as mock_universe:
            mock_universe.return_value = ivcu.get_vendor_universe(
                "ccxt",
                "trade",
                version="v8.1",
                as_full_symbol=True,
            )
            obcsgcopo._main(mock_argument_parser)
        # Check `fetch_positions()` was called.
        self.assertEqual(mock_ccxt.binance().fetch_positions.call_count, 1)
