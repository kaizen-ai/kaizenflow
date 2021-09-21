import im.ccxt.data.load.loader as icdloloa
import helpers.unit_test as hut


class TestGetFileName(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange, currencies.
        """
        exchange = "binance"
        currency = "ETH/USDT"
        actual = icdloloa.get_file_name(exchange, currency)
        expected = "binance_ETH_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test supported exchange, currencies.
        """
        exchange = "kucoin"
        currency = "ADA/USDT"
        actual = icdloloa.get_file_name(exchange, currency)
        expected = "kucoin_ADA_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test unsupported exchange, currencies.
        """
        exchange = "exchange"
        currency = "currency"
        actual = icdloloa.get_file_name(exchange, currency)
        expected = "exchange_currency.csv.gz"
        self.assert_equal(actual, expected)


# TODO(Grisha): add tests for CcxtLoader.read_data() once aws is fixed #28.
