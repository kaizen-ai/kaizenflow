import asyncio
import pprint
import unittest.mock as umock

import helpers.hunit_test as hunitest
import market_data as mdata
import oms.ccxt_broker as occxbrok
import oms.order as omorder
import im_v2.common.secrets.secret_identifier as imvcsseid



TEST_ORDERS = """Order: order_id=0 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=1464553467 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.121 tz=America/New_York
Order: order_id=1 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=1467591036 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=0.011 tz=America/New_York
Order: order_id=2 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=2061507978 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=169.063 tz=America/New_York
Order: order_id=3 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=2237530510 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=2.828 tz=America/New_York
Order: order_id=4 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=2601760471 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=-33.958 tz=America/New_York
Order: order_id=5 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=3065029174 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=6052.094 tz=America/New_York
Order: order_id=6 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=3303714233 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=-0.07 tz=America/New_York
Order: order_id=7 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=8717633868 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=3.885 tz=America/New_York
Order: order_id=8 creation_timestamp=2022-08-05 10:36:44.976104-04:00 asset_id=8968126878 type_=price@twap start_timestamp=2022-08-05 10:36:44.976104-04:00 end_timestamp=2022-08-05 10:38:44.976104-04:00 curr_num_shares=0.0 diff_num_shares=1.384 tz=America/New_York"""

class TestCcxtBroker1(hunitest.TestCase):
    # Mock calls to external providers.
    get_secret_patch = umock.patch.object(occxbrok.hsecret, "get_secret")
    ccxt_patch = umock.patch.object(occxbrok, "ccxt", spec=occxbrok.ccxt)
    #
    asset_id_to_symbol_map_patch = umock.patch.object(
        occxbrok.CcxtBroker,
        "_build_asset_id_to_symbol_mapping",
        spec=occxbrok.CcxtBroker._build_asset_id_to_symbol_mapping,
    )

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's start() method.
        self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
        self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
        self.asset_id_to_symbol_map_mock: umock.MagicMock = (
            self.asset_id_to_symbol_map_patch.start()
        )
        # Set dummy credentials for all tests.
        self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}
        # Set asset ids to symbol map for all tests.
        self.asset_id_to_symbol_map_mock.return_value = {
            1464553467: "ETH/USDT",
            1467591036: "BTC/USDT",
            2061507978: "EOS/USDT",
            2237530510: "SOL/USDT",
            2601760471: "LINK/USDT",
            3065029174: "DOGE/USDT",
            3303714233: "ADA/USDT",
            8717633868: "AVAX/USDT",
            8968126878: "BNB/USDT",
        }

    def tearDown(self) -> None:
        self.get_secret_patch.stop()
        self.ccxt_patch.stop()
        self.asset_id_to_symbol_map_patch.stop()
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
        secret_id = imvcsseid.SecretIdentifier(exchange_id, stage, account_type, 1)
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
        self.assertEqual(broker._exchange_id, "binance")
        self.assertEqual(broker._universe_version, "v5")
        self.assertEqual(broker._stage, "preprod")
        self.assertEqual(broker._account_type, "trading")
        self.assertEqual(broker._portfolio_id, "ccxt_portfolio_mock")
        self.assertEqual(broker._contract_type, "spot")
        self.assertEqual(str(broker._secret_id), "***REMOVED***")
        self.assertEqual(broker.strategy_id, "dummy_strategy_id")
        self.assertEqual(broker.max_order_submit_retries, 3)
        # Verify if maps are correct.
        self.assertEqual(
            sorted(broker._asset_id_to_symbol_mapping.keys()),
            sorted(broker._symbol_to_asset_id_mapping.values()),
        )
        self.assertEqual(
            sorted(broker._asset_id_to_symbol_mapping.values()),
            sorted(broker._symbol_to_asset_id_mapping.keys()),
        )
        self.assertEqual(
            sorted(broker._asset_id_to_symbol_mapping.keys()),
            sorted(broker._minimal_order_limits.keys()),
        )
        # Check if `exchange_class._exchange` was created from `ccxt.binance()` call.
        # Mock memorizes calls that lead to creation of it.
        self.assertEqual(
            broker._exchange._extract_mock_name(), "ccxt.binance()"
        )
        actual_method_calls = str(broker._exchange.method_calls)
        # Check calls against `exchange_class._exchange`.
        expected_method_calls = (
            "[call.checkRequiredCredentials(), call.load_markets()]"
        )
        self.assertEqual(actual_method_calls, expected_method_calls)

    def test_log_into_exchange(self) -> None:
        """
        Verify that login is done correctly based on the contract type.
        """
        stage = "preprod"
        account_type = "trading"
        exchange_mock = self.ccxt_mock.binance
        # Verify with `spot` contract type.
        _ = self.get_test_broker(stage, "spot", account_type)
        actual_args = pprint.pformat(tuple(exchange_mock.call_args))
        expected_args = "(({'apiKey': 'test', 'rateLimit': True, 'secret': 'test'},), {})"
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)
        # Verify with `futures` contract type.
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
        # Verify constructed secret.
        broker_class = self.get_test_broker(stage, "futures", "sandbox")
        self.assertEqual(self.get_secret_mock.call_count, 3)
        actual_args = tuple(self.get_secret_mock.call_args)
        expected_args = (("binance.preprod.sandbox.1",), {})
        self.assertEqual(actual_args, expected_args)
        # Verify `sandbox` mode.
        actual_method_calls = str(broker_class._exchange.method_calls)
        expected_method_call = "call.set_sandbox_mode(True),"
        self.assertIn(expected_method_call, actual_method_calls)
        # Check overall exchange initialization.
        self.assertEqual(exchange_mock.call_count, 3)

    def test_submit_orders(self):
        """
        Verify that orders are properly submitted.
        """
        # TODO(Nikola): Only one order is enough to test initial flow.
        # Prepare test data.
        orders = omorder.orders_from_string(TEST_ORDERS)
        #
        stage = "preprod"
        contract_type = "spot"
        account_type = "trading"
        # Initialize class.
        broker = self.get_test_broker(stage, contract_type, account_type)
        # TODO(Nikola): When possible, directly change vars instead mocking.
        broker._asset_id_to_symbol_mapping = {}
        broker._symbol_to_asset_id_mapping = {}
        broker._minimal_order_limits = {}
        # Patch main external source.
        with umock.patch.object(
            broker._exchange, "createOrder", create=True
        ) as create_order_mock:
            create_order_mock.side_effect = []
            # Run.
            receipt, order_df = asyncio.run(broker._submit_orders(orders, "dummy_timestamp", dry_run=False))
        # TODO(Nikola): Finish test.
