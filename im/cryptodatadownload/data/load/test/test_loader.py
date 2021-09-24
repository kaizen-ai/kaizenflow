import helpers.unit_test as hut
import im.cryptodatadownload.data.load.loader as crdall


class TestGetFileName(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "ETH/USDT"
        actual = crdall._get_file_name(exchange_id, currency_pair)
        expected = "Binance_ETHUSDT_minute.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "kucoin"
        currency_pair = "ADA/USDT"
        actual = crdall._get_file_name(exchange_id, currency_pair)
        expected = "Kucoin_ADAUSDT_minute.csv.gz"
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        with self.assertRaises(AssertionError):
            crdall._get_file_name(exchange_id, currency_pair)

    def test4(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        with self.assertRaises(AssertionError):
            crdall._get_file_name(exchange_id, currency_pair)


# TODO(Dan): add tests for CddLoader.read_data() once aws is fixed #28.
