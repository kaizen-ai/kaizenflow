import helpers.unit_test as hut
import im.ccxt.data.load.loader as cdlloa


class TestGetFileName(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "ETH/USDT"
        actual = cdlloa._get_file_path(
            cdlloa._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        expected = "ccxt/20210924/binance/ETH_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        with self.assertRaises(AssertionError):
            cdlloa._get_file_path(
                cdlloa._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        with self.assertRaises(AssertionError):
            cdlloa._get_file_path(
                cdlloa._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


# TODO(Grisha): add tests for CcxtLoader.read_data() once aws is fixed #28.
