import helpers.unit_test as hunitest
import im_v2.data.universe as imv2dauni


class TestFilterVendorUniverseAsTuples(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that filtering works properly.
        """
        exchange_ids = ["binance", "kucoin"]
        currency_pairs = ["BTC/USDT", "ETH/USDT", "FIL/USDT"]
        ccxt_universe = imv2dauni.get_vendor_universe_as_tuples("v03", "CCXT")
        actual = imv2dauni.filter_vendor_universe_as_tuples(
            ccxt_universe, exchange_ids, currency_pairs
        )
        expected = [
            imv2dauni.ExchangeCurrencyTuple("binance", "BTC/USDT"),
            imv2dauni.ExchangeCurrencyTuple("binance", "ETH/USDT"),
            imv2dauni.ExchangeCurrencyTuple("kucoin", "BTC/USDT"),
            imv2dauni.ExchangeCurrencyTuple("kucoin", "ETH/USDT"),
            imv2dauni.ExchangeCurrencyTuple("kucoin", "FIL/USDT"),
        ]
        self.assert_equal(str(actual), str(expected))


class TestGetUniverse(hunitest.TestCase):
    def test_get_universe1(self) -> None:
        """
        A smoke test to verify that universe loads correctly.
        """
        _ = imv2dauni.get_trade_universe()

    def test_get_universe2(self) -> None:
        """
        Verify that incorrect universe name is recognized.
        """
        with self.assertRaises(AssertionError):
            _ = imv2dauni.get_trade_universe("non-existent")
