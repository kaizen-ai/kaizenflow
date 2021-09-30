import pytest

import helpers.unit_test as hut
import im.cryptodatadownload.data.load.loader as crdall


class TestGetFilePath(hut.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "ETH/USDT"
        actual = crdall._get_file_path(
            crdall._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        expected = "cryptodatadownload/20210924/binance/ETH_USDT.csv.gz"
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        with self.assertRaises(AssertionError):
            crdall._get_file_path(
                crdall._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        with self.assertRaises(AssertionError):
            crdall._get_file_path(
                crdall._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


@pytest.mark.skip()
class TestCddLoader(hut.TestCase):
    def test1(self) -> None:
        """
        Test that locally stored files are being read correctly.
        """
        cdd_loader = crdall.CddLoader("/app/im")
        actual = cdd_loader.read_data("binance", "BTC/USDT", "OHLCV")
        # Check the output values.
        actual_string = hut.convert_df_to_json_string(actual)
        self.check_string(actual_string)


# TODO(Dan): add tests for CddLoader.read_data() once aws is fixed #28.
