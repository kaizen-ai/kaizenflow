import helpers.unit_test as hunitest
import im_v2.data.universe as imtwodauni


class TestFilterVendorUniverseAsTuples(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that filtering works properly.
        """
        exchange_ids = ["binance", "kucoin"]
        currency_pairs = ["BTC/USDT", "ETH/USDT", "FIL/USDT"]
        ccxt_universe = imtwodauni.get_vendor_universe_as_tuples("v03", "CCXT")
        actual = imtwodauni.filter_vendor_universe_as_tuples(
            ccxt_universe, exchange_ids, currency_pairs
        )
        expected = [
            imtwodauni.ExchangeCurrencyTuple("binance", "BTC/USDT"),
            imtwodauni.ExchangeCurrencyTuple("binance", "ETH/USDT"),
            imtwodauni.ExchangeCurrencyTuple("kucoin", "BTC/USDT"),
            imtwodauni.ExchangeCurrencyTuple("kucoin", "ETH/USDT"),
            imtwodauni.ExchangeCurrencyTuple("kucoin", "FIL/USDT"),
        ]
        self.assert_equal(str(actual), str(expected))


class TestGetUniverse(hunitest.TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        _ = imtwodauni.get_trade_universe()

    def test_get_universe2(self) -> None:
        """
        Verify that incorrect universe name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imtwodauni.get_trade_universe("non-existent")
