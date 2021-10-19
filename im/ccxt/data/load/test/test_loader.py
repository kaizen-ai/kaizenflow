import os

import helpers.s3 as hs3
import helpers.unit_test as hut
import im.ccxt.data.load.loader as cdlloa


_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestGetFilePath(hut.TestCase):
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
        # TODO(gp): We should throw a different exception, like
        # `UnsupportedExchane`.
        # TODO(gp): Same change also for CDD test_loader.py
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


class TestCcxtLoader(hut.TestCase):

    def test1(self) -> None:
        """
        Test files on S3 are being read correctly.
        """
        # TODO(gp): We could factor out these 2 lines in a `def _get_loader()`.
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_data("binance", "BTC/USDT", "OHLCV")
        # Check the output values.
        actual_string = hut.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("unsupported_exchange_id", "BTC/USDT", "OHLCV")

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("binance", "unsupported_currency_pair", "OHLCV")

    def test4(self) -> None:
        """
        Test unsupported data type.
        """
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data("binance", "BTC/USDT", "unsupported_data_type")
