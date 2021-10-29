import im.ccxt.data.load.loader as icdloloa
import helpers.unit_test as hut


_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestGetFilePath(hut.TestCase):
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
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        # TODO(gp): We should throw a different exception, like
        # `UnsupportedExchane`.
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            icdloloa.get_file_name(exchange, currency)

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            icdloloa.get_file_name(exchange, currency)


# TODO(*): Consider to factor out the class calling in a `def _get_loader()`.
class TestCcxtLoader(hut.TestCase):
    @pytest.mark.slow
    def test1(self) -> None:
        """
        Test files on S3 are being read correctly.
        """
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_data_from_filesystem(
            "binance", "BTC/USDT", "OHLCV"
        )
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
            ccxt_loader.read_data_from_filesystem(
                "unsupported_exchange_id", "BTC/USDT", "OHLCV"
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data_from_filesystem(
                "binance", "unsupported_currency_pair", "OHLCV"
            )

    def test4(self) -> None:
        """
        Test unsupported data type.
        """
        ccxt_loader = cdlloa.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data_from_filesystem(
                "binance", "BTC/USDT", "unsupported_data_type"
            )
