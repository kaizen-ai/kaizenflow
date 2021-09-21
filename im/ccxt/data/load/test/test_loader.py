import helpers.unit_test as hut
import im.ccxt.data.load.loader as icdloloa


class TestGetFileName(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange, currencies.
        """
        exchange = "binance"
        currency = "ETH/USDT"
        actual = icdloloa._get_file_name(exchange, currency)
        expected = "binance_ETH_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test supported exchange, currencies.
        """
        exchange = "kucoin"
        currency = "ADA/USDT"
        actual = icdloloa._get_file_name(exchange, currency)
        expected = "kucoin_ADA_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test unsupported exchange.
        """
        exchange = "unsupported exchange"
        currency = "ADA/USDT"
        with self.assertRaises(AssertionError):
            icdloloa._get_file_name(exchange, currency)

    def test4(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange = "binance"
        currency = "unsupported_currency"
        with self.assertRaises(AssertionError):
            icdloloa._get_file_name(exchange, currency)


# TODO(Grisha): add tests for CcxtLoader.read_data() once aws is fixed #28.
