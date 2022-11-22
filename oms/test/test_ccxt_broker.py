import asyncio
import logging
import os
import pprint
import re
import unittest.mock as umock
from typing import List

import pandas as pd

import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import market_data as mdata
import oms
import oms.ccxt_broker as occxbrok
import oms.hsecrets.secret_identifier as ohsseide
import oms.order as omorder

_LOG = logging.getLogger(__name__)


class TestCcxtBroker1(hunitest.TestCase):
    # Mock calls to external providers.
    get_secret_patch = umock.patch.object(occxbrok.hsecret, "get_secret")
    ccxt_patch = umock.patch.object(occxbrok, "ccxt", spec=occxbrok.ccxt)

    @staticmethod
    def get_test_orders() -> List[omorder.Order]:
        """
        Build toy orders for tests.
        """
        # Prepare test data.
        order_str = "Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
        asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
        end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121\
        tz=America/New_York"
        # Get orders.
        orders = omorder.orders_from_string(order_str)
        return orders

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's `start()` method.
        self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
        self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
        # Set dummy credentials for all tests.
        self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}
        # Reset static variables on each test run.
        oms.Broker._submitted_order_id = 1
        oms.Fill._fill_id = 1

    def tearDown(self) -> None:
        self.get_secret_patch.stop()
        self.ccxt_patch.stop()
        # Deallocate in reverse order to avoid race conditions.
        super().tearDown()

    def get_test_broker(
        self, stage: str, contract_type: str, account_type: str
    ) -> occxbrok.CcxtBroker:
        """
        Build `CcxtBroker` for tests.
        """
        exchange_id = "binance"
        universe_version = "v5"
        portfolio_id = "ccxt_portfolio_mock"
        secret_id = ohsseide.SecretIdentifier(exchange_id, stage, account_type, 1)
        broker = occxbrok.CcxtBroker(
            exchange_id,
            universe_version,
            stage,
            account_type,
            portfolio_id,
            contract_type,
            secret_id,
            strategy_id="dummy_strategy_id",
            market_data=umock.create_autospec(
                spec=mdata.MarketData, instance=True
            ),
        )
        return broker

    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        broker = self.get_test_broker(stage, contract_type, account_type)
        # Check broker state and remove dynamic mock ids.
        broker_state_with_mock_ids = pprint.pformat(vars(broker))
        broker_state = re.sub(
            r" id='(.*?)'>", " id='***'>", broker_state_with_mock_ids
        )
        self.check_string(broker_state)

    def test_log_into_exchange1(self) -> None:
        """
        Verify that login is done correctly with `spot` contract type.
        """
        stage = "preprod"
        account_type = "trading"
        exchange_mock = self.ccxt_mock.binance
        _ = self.get_test_broker(stage, "spot", account_type)
        actual_args = pprint.pformat(tuple(exchange_mock.call_args))
        expected_args = (
            "(({'apiKey': 'test', 'rateLimit': True, 'secret': 'test'},), {})"
        )
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)

    def test_log_into_exchange2(self) -> None:
        """
        Verify that login is done correctly with `futures` contract type.
        """
        stage = "preprod"
        account_type = "trading"
        exchange_mock = self.ccxt_mock.binance
        _ = self.get_test_broker(stage, "futures", account_type)
        actual_args = pprint.pformat(tuple(exchange_mock.call_args))
        expected_args = r"""
            (({'apiKey': 'test',
               'options': {'defaultType': 'future'},
               'rateLimit': True,
               'secret': 'test'},),
             {})
        """
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)

    def test_log_into_exchange3(self) -> None:
        """
        Verify constructed secret for obtaining credentials from AWS secrets.
        """
        stage = "preprod"
        contract_type = "futures"
        account_type = "trading"
        _ = self.get_test_broker(stage, contract_type, account_type)
        self.assertEqual(self.get_secret_mock.call_count, 1)
        actual_args = tuple(self.get_secret_mock.call_args)
        expected_args = (("***REMOVED***",), {})
        self.assertEqual(actual_args, expected_args)

    def test_log_into_exchange4(self) -> None:
        """
        Verify that `sandox` mode is set.
        """
        stage = "preprod"
        contract_type = "spot"
        account_type = "sandbox"
        broker = self.get_test_broker(stage, contract_type, account_type)
        actual_method_calls = str(broker._exchange.method_calls)
        expected_method_call = "call.set_sandbox_mode(True),"
        self.assertIn(expected_method_call, actual_method_calls)

    def test_submit_orders(
        self,
    ) -> None:
        """
        Verify that orders are properly submitted via mocked exchange.
        """
        orders = self.get_test_orders()
        # Define broker parameters.
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        # Initialize class.
        broker = self.get_test_broker(stage, contract_type, account_type)
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            create_order_mock.side_effect = [{"id": 0}]
            # Run.
            receipt, order_df = asyncio.run(
                broker._submit_orders(orders, "dummy_timestamp", dry_run=False)
            )
        # Check the count of calls.
        self.assertEqual(create_order_mock.call_count, 1)
        # Check the args.
        actual_args = pprint.pformat(tuple(create_order_mock.call_args))
        expected_args = r"""
            ((),
             {'amount': 0.121,
              'params': {'client_oid': 0, 'portfolio_id': 'ccxt_portfolio_mock'},
              'side': 'buy',
              'symbol': 'ETH/USDT',
              'type': 'market'})
        """
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)
        # Check the receipt.
        self.assert_equal(receipt, "filename_1.txt")
        # Check the order Dataframe.
        act = hpandas.convert_df_to_json_string(order_df, n_tail=None)
        exp = r"""
            original shape=(1, 9)
            Head:
            {
                "0":{
                    "order_id":0,
                    "creation_timestamp":"2022-08-05T14:36:44Z",
                    "asset_id":1464553467,
                    "type_":"price@twap",
                    "start_timestamp":"2022-08-05T14:36:44Z",
                    "end_timestamp":"2022-08-05T14:38:44Z",
                    "curr_num_shares":0.0,
                    "diff_num_shares":0.121,
                    "tz":"America\/New_York"
                }
            }
            Tail:
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @umock.patch.object(occxbrok.time, "sleep")
    def test_submit_orders_errors1(self, sleep_mock: umock.MagicMock) -> None:
        """
        Verify that Binance API error is raised correctly.
        """
        orders = self.get_test_orders()
        # Define broker parameters.
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        # Initialize class.
        broker = self.get_test_broker(stage, contract_type, account_type)
        broker._submitted_order_id = 1
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            # Check the Binance API error.
            from ccxt.base.errors import ExchangeNotAvailable

            create_order_mock.side_effect = [
                ExchangeNotAvailable(umock.Mock(), ""),
                Exception("MockException"),
            ]
            with umock.patch.object(
                self.ccxt_mock, "ExchangeNotAvailable", ExchangeNotAvailable
            ):
                # Run.
                with self.assertRaises(Exception) as cm:
                    asyncio.run(
                        broker._submit_orders(
                            orders, "dummy_timestamp", dry_run=False
                        )
                    )
        self.assert_equal(str(cm.exception), "MockException")
        # Check the count of calls.
        self.assertEqual(sleep_mock.call_count, 1)

    def test_submit_orders_errors2(self) -> None:
        """
        Verify that the order is not submitted in case of a liquidity error.
        """
        orders = self.get_test_orders()
        # Define broker parameters.
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        # Initialize class.
        broker = self.get_test_broker(stage, contract_type, account_type)
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            # Check that the order is not submitted if the error is connected to liquidity.
            from ccxt.base.errors import ExchangeNotAvailable, OrderNotFillable

            create_order_mock.side_effect = [
                OrderNotFillable(umock.Mock(status=-4131), '"code":-4131,')
            ]
            with umock.patch.object(
                self.ccxt_mock, "ExchangeNotAvailable", ExchangeNotAvailable
            ):
                # Run.
                _, order_df = asyncio.run(
                    broker._submit_orders(
                        orders, "dummy_timestamp", dry_run=False
                    )
                )
        # Order df should be empty.
        self.assertEqual(order_df.empty, True)

    def test_get_fills_for_time_period(self) -> None:
        """
        Verify that fills for conducted trades are requested properly.
        """
        # Define broker parameters.
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        # Initialize class.
        broker = self.get_test_broker(stage, contract_type, account_type)
        with umock.patch.object(
            broker._exchange, "fetchMyTrades", create=True
        ) as fetch_trades_mock:
            # Load an example of a return value.
            # Command to generate:
            #  oms/get_ccxt_fills.py \
            #  --start_timestamp 2022-09-01T00:00:00.000Z \
            #  --end_timestamp 2022-09-01T00:10:00.000Z \
            #  --dst_dir oms/test/outcomes/TestCcxtBroker1.test_get_fills_for_time_period/input/ \
            #  --exchange_id binance \
            #  --contract_type futures \
            #  --stage preprod \
            #  --account_type trading \
            #  --secrets_id 1 \
            #  --universe v5
            # Note: the return value is generated via the script rather than
            #  the mocked CCXT function, so the test covers only the format of
            #  the data, and not the content.
            return_value_path = os.path.join(self.get_input_dir(), "trades.json")
            fetch_trades_mock.return_value = hio.from_json(return_value_path)
            start_timestamp = pd.Timestamp("2022-09-01T00:00:00.000Z")
            end_timestamp = pd.Timestamp("2022-09-01T00:10:00.000Z")
            fills = broker.get_fills_for_time_period(
                start_timestamp, end_timestamp
            )
            # Verify that the format of the data output is correct.
            self.assertEqual(len(fills), 1548)
            self.assertIsInstance(fills, list)
            self.assertIsInstance(fills[0], dict)
            # Verify that trades are fetched for each asset in the universe.
            self.assertEqual(fetch_trades_mock.call_count, 9)
            # Verify that the arguments are called correctly.
            expected_calls = [
                umock.call(
                    symbol="ADA/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="AVAX/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="BNB/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="BTC/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="DOGE/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="EOS/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="ETH/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="LINK/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
                umock.call(
                    symbol="SOL/USDT",
                    since=1661990400000,
                    params={"endTime": 1661991000000},
                ),
            ]
            fetch_trades_mock.assert_has_calls(expected_calls)

    def test_get_fills(self) -> None:
        """
        Verify that orders are filled properly via mocked exchange.
        """
        orders = self.get_test_orders()
        # Define broker parameters.
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        # Initialize class.
        broker = self.get_test_broker(stage, contract_type, account_type)
        broker._submitted_order_id = 1
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            create_order_mock.side_effect = [{"id": 0}]
            # Run.
            asyncio.run(
                broker._submit_orders(orders, "dummy_timestamp", dry_run=False)
            )
        # Mock last order execution time.
        broker.last_order_execution_ts = pd.to_datetime(1662242460466, unit="ms")
        with umock.patch.object(
            broker._exchange, "fetch_orders", create=True
        ) as fetch_orders_mock:
            fetch_orders_mock.return_value = [
                {
                    "info": {
                        "time": "1662242460466",
                        "updateTime": "1662242460466",
                    },
                    "id": 0,
                    "timestamp": 1662242460466,
                    "side": "sell",
                    "price": 19749.8,
                    "filled": 0.004,
                    "status": "closed",
                }
            ]
            # Run.
            fills = broker.get_fills()
        fill = fills[0]
        # Check the count of calls.
        self.assertEqual(fetch_orders_mock.call_count, 1)
        # Check the args.
        actual_args = pprint.pformat(tuple(fetch_orders_mock.call_args))
        expected_args = r"""
            ((), {'since': 1662242460466, 'symbol': 'ETH/USDT'})
        """
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)
        # Check fill.
        self.assertEqual(fill.price, 19749.8)
        self.assertEqual(fill.num_shares, -0.004)
        self.assertEqual(fill._fill_id, 1)
        # Convert timestamps to string.
        actual_time = fill.timestamp.strftime("%Y-%m-%d %X")
        expected_time = pd.to_datetime(1662242460466, unit="ms").strftime(
            "%Y-%m-%d %X"
        )
        # Check timestamps.
        self.assertEqual(actual_time, expected_time)


# @pytest.mark.skip(reason="Run manually.")
class TestSaveMarketInfo(hunitest.TestCase):
    """
    Capture market info data from a CCXT broker so that it can be reused in
    other tests and code.
    """

    def get_test_broker(
        self,
        universe_version: str,
        stage: str,
        contract_type: str,
        account_type: str,
    ) -> occxbrok.CcxtBroker:
        """
        Build `CcxtBroker` for tests.
        """
        exchange_id = "binance"
        portfolio_id = "ccxt_portfolio_mock"
        secret_id = ohsseide.SecretIdentifier(exchange_id, stage, account_type, 1)
        broker = occxbrok.CcxtBroker(
            exchange_id,
            universe_version,
            stage,
            account_type,
            portfolio_id,
            contract_type,
            secret_id,
            strategy_id="dummy_strategy_id",
            market_data=umock.create_autospec(
                spec=mdata.MarketData, instance=True
            ),
        )
        return broker

    def test1(self) -> None:
        """
        Save minimal order limits on s3 for simulated broker run.
        """
        # Initialize broker.
        universe_version = "v7.1"
        stage = "preprod"
        contract_type = "futures"
        account_type = "trading"
        broker = self.get_test_broker(
            universe_version, stage, contract_type, account_type
        )
        # Get market information from the exchange.
        market_info = broker.market_info
        _LOG.debug("asset_market_info dict '%s' ...", market_info)
        # Build file path.
        dst_dir = self.get_input_dir(use_only_test_class=True)
        file_name = "binance.market_info.json"
        file_path = os.path.join(dst_dir, file_name)
        # Save data.
        _LOG.info("Saving data in '%s' ...", file_path)
        hio.to_json(file_path, market_info)
        _LOG.info("Saving in '%s' done", file_path)
