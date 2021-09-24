import helpers.unit_test as hut
# TODO(Dan): return to code after CmTask43 is fixed.
# import im.cryptodatadownload.data.load.loader as crdall

import pytest


@pytest.mark.skip()
class TestGetFileName(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id, currency pair, and timeframe.
        """
        exchange_id = "binance"
        currency_pair = "ETH/USDT"
        timeframe = "minute"
        actual = crdall._get_file_name(exchange_id, currency_pair, timeframe)
        expected = "Binance_ETHUSDT_minute.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test supported exchange id, currency pair, and timeframe.
        """
        exchange_id = "kucoin"
        currency_pair = "ADA/USDT"
        timeframe = "minute"
        actual = crdall._get_file_name(exchange_id, currency_pair, timeframe)
        expected = "Kucoin_ADAUSDT_minute.csv.gz"
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        timeframe = "minute"
        with self.assertRaises(AssertionError):
            crdall._get_file_name(exchange_id, currency_pair, timeframe)

    def test4(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        timeframe = "minute"
        with self.assertRaises(AssertionError):
            crdall._get_file_name(exchange_id, currency_pair, timeframe)

    def test5(self) -> None:
        """
        Test unsupported timeframe.
        """
        exchange_id = "binance"
        currency_pair = "ADA/USDT"
        timeframe = "unsupported_timeframe"
        with self.assertRaises(AssertionError):
            crdall._get_file_name(exchange_id, currency_pair, timeframe)


# TODO(Dan): add tests for CddLoader.read_data() once aws is fixed #28.
