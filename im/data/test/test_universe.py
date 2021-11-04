import helpers.unit_test as hut
import im.data.universe as imdauni


class TestFilterVendorUniverseAsTuples(hut.TestCase):
    def test1(self) -> None:
        """
        Test that filtering works properly.
        """
        exchange_ids = ["binance", "kucoin"]
        currency_pairs = ["BTC/USDT", "ETH/USDT", "FIL/USDT"]
        ccxt_universe = imdauni.get_vendor_universe_as_tuples("v0_3")["CCXT"]
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
        self.assert_equal(actual, expected)
