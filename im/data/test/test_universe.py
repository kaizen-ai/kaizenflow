import helpers.unit_test as huntes
import im.data.universe as imdauni


class TestFilterVendorUniverseAsTuples(huntes.TestCase):
    def test1(self) -> None:
        """
        Test that filtering works properly.
        """
        exchange_ids = ["binance", "kucoin"]
        currency_pairs = ["BTC/USDT", "ETH/USDT", "FIL/USDT"]
        ccxt_universe = imdauni.get_vendor_universe_as_tuples("v03", "CCXT")
        actual = imdauni.filter_vendor_universe_as_tuples(
            ccxt_universe, exchange_ids, currency_pairs
        )
        expected = [
            imdauni.ExchangeCurrencyTuple("binance", "BTC/USDT"),
            imdauni.ExchangeCurrencyTuple("binance", "ETH/USDT"),
            imdauni.ExchangeCurrencyTuple("kucoin", "BTC/USDT"),
            imdauni.ExchangeCurrencyTuple("kucoin", "ETH/USDT"),
            imdauni.ExchangeCurrencyTuple("kucoin", "FIL/USDT"),
        ]
        self.assert_equal(str(actual), str(expected))


class TestGetUniverse(huntes.TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        _ = imdauni.get_trade_universe()

    def test_get_universe2(self) -> None:
        """
        Verify that incorrect universe name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imdauni.get_trade_universe("non-existent")
