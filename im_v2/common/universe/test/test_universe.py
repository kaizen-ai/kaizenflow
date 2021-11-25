import helpers.unit_test as hunitest
import im_v2.common.universe.universe as imvcounun


class TestFilterVendorUniverseAsTuples(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that filtering works properly.
        """
        exchange_ids = ["binance", "kucoin"]
        currency_pairs = ["BTC/USDT", "ETH/USDT", "FIL/USDT"]
        ccxt_universe = imvcounun.get_vendor_universe_as_tuples("v03", "CCXT")
        actual = imvcounun.filter_vendor_universe_as_tuples(
            ccxt_universe, exchange_ids, currency_pairs
        )
        expected = [
            imvcounun.ExchangeCurrencyTuple("binance", "BTC/USDT"),
            imvcounun.ExchangeCurrencyTuple("binance", "ETH/USDT"),
            imvcounun.ExchangeCurrencyTuple("kucoin", "BTC/USDT"),
            imvcounun.ExchangeCurrencyTuple("kucoin", "ETH/USDT"),
            imvcounun.ExchangeCurrencyTuple("kucoin", "FIL/USDT"),
        ]
        self.assert_equal(str(actual), str(expected))


class TestGetUniverse(hunitest.TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        _ = imvcounun.get_trade_universe()

    def test_get_universe2(self) -> None:
        """
        Verify that incorrect universe name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imvcounun.get_trade_universe("non-existent")


class TestGetVendorUniverse(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that universe is received correctly.
        """
        universe_as_full_symbols = imvcounun.get_vendor_universe(version="small")
        self.assertEqual(len(universe_as_full_symbols), 2)
        self.assert_equal(universe_as_full_symbols[0], "gateio::XRP_USDT")
        self.assert_equal(universe_as_full_symbols[1], "kucoin::SOL_USDT")
