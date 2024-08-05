import argparse
import unittest.mock as umock

import numpy as np

import helpers.hunit_test as hunitest
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.common.db.db_utils as imvcddbut
import im_v2.common.universe as ivcu
import oms.broker.ccxt.ccxt_utils as obccccut
import oms.broker.ccxt.scripts.flatten_ccxt_account as obcsfccac


class TestFlattenCcxtAccount(hunitest.TestCase):
    """
    Unit tests for the `flatten_ccxt_account.py` script.
    """

    def test_parser(self) -> None:
        """
        Test arg parser for predefined args in the script.

        Mostly for coverage and to detect argument changes.
        """
        parser = obcsfccac._parse()
        cmd = [
            "--assert_on_non_zero_positions",
            "--exchange",
            "binance",
            "--contract_type",
            "futures",
            "--stage",
            "preprod",
            "--secret_id",
            "3",
            "--universe",
            "v7.4",
            "--log_dir",
            "tmp",
        ]
        args = parser.parse_args(cmd)
        actual = vars(args)
        expected = {
            "assert_on_non_zero_positions": True,
            "exchange": "binance",
            "contract_type": "futures",
            "stage": "preprod",
            "secret_id": 3,
            "universe": "v7.4",
            "log_dir": "tmp",
            "log_level": "INFO",
        }
        self.assertDictEqual(actual, expected)

    def test_flatten_ccxt_account(
        self,
    ) -> None:
        """
        Smoke test to check execution of `flatten_ccxt_account.py` script.
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
            "secret_id": 3,
            "log_dir": log_dir,
            "universe": "v8.1",
            "log_level": "INFO",
            "assert_on_non_zero_positions": False,
        }
        mock_parse = argparse.Namespace(**kwargs)
        mock_argument_parser.parse_args.return_value = mock_parse
        with umock.patch.object(
            obccccut, "ccxt", spec=obccccut.ccxt
        ) as mock_ccxt, umock.patch.object(
            imvcddbut.DbConnectionManager,
            "get_connection",
        ) as _, umock.patch.object(
            imvcdccccl.CcxtSqlRealTimeImClient,
            "get_universe",
        ) as mock_universe, umock.patch.object(
            obccccut, "ccxtpro", spec=obccccut.ccxtpro
        ) as mock_ccxtpro:
            # We set random initial order ID, set seed
            # to ensure reproducible test outcome.
            np.random.seed(123)
            order_id = 884741
            mock_universe.return_value = ivcu.get_vendor_universe(
                "ccxt",
                "trade",
                version="v8.1",
                as_full_symbol=True,
            )
            mock_ccxt.binance().fetchBalance.return_value = {"total": {}}
            mock_ccxt.binance().fetch_positions.return_value = [
                {
                    "info": {
                        "positionAmt": "-0.200",
                    },
                    "symbol": "API3/USDT:USDT",
                }
            ]
            mock_create_reduce_only_order = umock.AsyncMock()
            mock_create_reduce_only_order.return_value = {
                "id": order_id,
                "status": "closed",
                "amount": 0.200,
                "side": "buy",
            }
            mock_ccxtpro.binance().createReduceOnlyOrder = (
                mock_create_reduce_only_order
            )
            obcsfccac._main(mock_argument_parser)
        # Check `createReduceOnlyOrder()` was called with parameters.
        expected = {
            "symbol": "API3/USDT:USDT",
            "type": "market",
            "side": "buy",
            "amount": 0.2,
            "params": {
                "portfolio_id": "ccxt_portfolio_1",
                "client_oid": order_id,
            },
            "price": 0,
        }
        actual = mock_ccxtpro.binance().createReduceOnlyOrder.call_args_list[0][1]
        self.assertDictEqual(actual, expected)
