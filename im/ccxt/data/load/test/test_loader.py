import im.ccxt.data.load.loader as icdloloa
import helpers.unit_test as hut


class TestGetFileName(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange = "binance"
        currency = "ETH/USDT"
        actual = icdloloa.get_file_name(exchange, currency)
        expected = "binance_ETH_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange = "kucoin"
        currency = "ADA/USDT"
        actual = icdloloa.get_file_name(exchange, currency)
        expected = "kucoin_ADA_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        with self.assertRaises(AssertionError):
            icdloloa.get_file_name(exchange, currency)

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        with self.assertRaises(AssertionError):
            icdloloa.get_file_name(exchange, currency)


# TODO(Grisha): add tests for CcxtLoader.read_data() once aws is fixed #28.
