import asyncio
import pprint
import re
import unittest.mock as umock

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import oms.secrets.secret_identifier as oseseide
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.order as omorder


class TestCcxtBroker1(hunitest.TestCase):
    # Mock calls to external providers.
    get_secret_patch = umock.patch.object(occxbrok.hsecret, "get_secret")
    ccxt_patch = umock.patch.object(occxbrok, "ccxt", spec=occxbrok.ccxt)

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's `start()` method.
        self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
        self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
        # Set dummy credentials for all tests.
        self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}

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
        secret_id = oseseide.SecretIdentifier(
            exchange_id, stage, account_type, 1
        )
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
        # Prepare test data.
        order_str = "Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00\
        asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00\
        end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121\
        tz=America/New_York"
        orders = omorder.orders_from_string(order_str)
        #
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

