import unittest.mock as umock

import helpers.hunit_test as hunitest
import market_data as mdata
import oms.ccxt_broker as occxbrok


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
        broker = occxbrok.CcxtBroker(
            exchange_id,
            universe_version,
            stage,
            account_type,
            portfolio_id,
            contract_type,
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
        broker_class = self.get_test_broker(stage, contract_type, account_type)
        self.assertEqual(broker_class._exchange_id, "binance")
        self.assertEqual(broker_class._universe_version, "v5")
        self.assertEqual(broker_class._stage, "preprod")
        self.assertEqual(broker_class._account_type, "trading")
        self.assertEqual(broker_class._portfolio_id, "ccxt_portfolio_mock")
        self.assertEqual(broker_class._contract_type, "spot")
        self.assertEqual(broker_class._secret_id, 1)
        self.assertEqual(broker_class.strategy_id, "dummy_strategy_id")
        self.assertEqual(broker_class.max_order_submit_retries, 3)
        # Verify if maps are correct.
        self.assertEqual(
            sorted(broker_class._asset_id_to_symbol_mapping.keys()),
            sorted(broker_class._symbol_to_asset_id_mapping.values()),
        )
        self.assertEqual(
            sorted(broker_class._asset_id_to_symbol_mapping.values()),
            sorted(broker_class._symbol_to_asset_id_mapping.keys()),
        )
        self.assertEqual(
            sorted(broker_class._asset_id_to_symbol_mapping.keys()),
            sorted(broker_class._minimal_order_limits.keys()),
        )
        # Check if `exchange_class._exchange` was created from `ccxt.binance()` call.
        # Mock memorizes calls that lead to creation of it.
        self.assertEqual(
            broker_class._exchange._extract_mock_name(), "ccxt.binance()"
        )
        actual_method_calls = str(broker_class._exchange.method_calls)
        # Check calls against `exchange_class._exchange`.
        expected_method_calls = (
            "[call.checkRequiredCredentials(), call.load_markets()]"
        )
        self.assertEqual(actual_method_calls, expected_method_calls)
        # Wrong stage.
        with self.assertRaises(AssertionError) as fail:
            self.get_test_broker("dummy", contract_type, account_type)
        actual = str(fail.exception)
        expected = "Failed assertion *\n'dummy' in '['local', 'preprod']'"
        self.assertIn(expected, actual)
        # Wrong contract type.
        with self.assertRaises(AssertionError) as fail:
            self.get_test_broker(stage, "dummy", account_type)
        actual = str(fail.exception)
        expected = "Failed assertion *\n'dummy' in '['spot', 'futures']'"
        self.assertIn(expected, actual)
        # Wrong account type.
        with self.assertRaises(AssertionError) as fail:
            self.get_test_broker(stage, contract_type, "dummy")
        actual = str(fail.exception)
        expected = "Failed assertion *\n'dummy' in '['trading', 'sandbox']'"
        self.assertIn(expected, actual)

    def test_log_into_exchange(self) -> None:
        """
        Verify that login is done correctly based on the contract type.
        """
        stage = "preprod"
        account_type = "trading"
        exchange_mock = self.ccxt_mock.binance
        # Verify with `spot` contract type.
        _ = self.get_test_broker(stage, "spot", account_type)
        actual_args = tuple(exchange_mock.call_args)
        expected_args = (
            ({"apiKey": "test", "rateLimit": True, "secret": "test"},),
            {},
        )
        self.assertEqual(actual_args, expected_args)
        # Verify with `futures` contract type.
        _ = self.get_test_broker(stage, "futures", account_type)
        actual_args = tuple(exchange_mock.call_args)
        expected_args = (
            (
                {
                    "apiKey": "test",
                    "options": {"defaultType": "future"},
                    "rateLimit": True,
                    "secret": "test",
                },
            ),
            {},
        )
        self.assertEqual(actual_args, expected_args)
        # Verify constructed secret.
        broker_class = self.get_test_broker(stage, "futures", "sandbox")
        self.assertEqual(self.get_secret_mock.call_count, 3)
        actual_args = tuple(self.get_secret_mock.call_args)
        expected_args = (("binance.preprod.sandbox.1",), {})
        self.assertEqual(actual_args, expected_args)
        # Verify `sandbox` mode.
        actual_method_calls = str(broker_class._exchange.method_calls)
        expected_method_calls = r"""
            [call.checkRequiredCredentials(),
             call.load_markets(),
             call.checkRequiredCredentials(),
             call.load_markets(),
             call.set_sandbox_mode(True),
             call.checkRequiredCredentials(),
             call.load_markets()]
        """
        self.assert_equal(
            actual_method_calls, expected_method_calls, fuzzy_match=True
        )
        # Check overall exchange initialization.
        self.assertEqual(exchange_mock.call_count, 3)
