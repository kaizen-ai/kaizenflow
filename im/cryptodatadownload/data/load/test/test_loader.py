import os

import pytest

import helpers.s3 as hs3
import helpers.unit_test as hunitest
import im.cryptodatadownload.data.load.loader as imcdalolo


# TODO(gp): Move to im.cryptodatadownloader.data.load.cdd_
def _get_ccd_loader_example1():
    _AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")
    data_type = "OHLCV"
    root_dir = _AM_S3_ROOT_DIR
    cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
    return cdd_loader


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        data_type = "OHLCV"
        root_dir = _AM_S3_ROOT_DIR
        cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
        #
        exchange_id = "binance"
        currency_pair = "ETH_USDT"
        actual = cdd_loader._get_file_path(
            imcdalolo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        expected = os.path.join(
            hs3.get_path(),
            "data/cryptodatadownload/20210924/binance/ETH_USDT.csv.gz",
        )
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        data_type = "OHLCV"
        root_dir = _AM_S3_ROOT_DIR
        cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
        #
        exchange_id = "unsupported exchange"
        currency_pair = "ADA_USDT"
        with self.assertRaises(AssertionError):
            cdd_loader._get_file_path(
                imcdalolo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        data_type = "OHLCV"
        root_dir = _AM_S3_ROOT_DIR
        cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
        with self.assertRaises(AssertionError):
            cdd_loader._get_file_path(
                imcdalolo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


class TestCddLoader(hunitest.TestCase):
    @pytest.mark.skip("CmapAmp413: 18s. Too slow")
    def test1(self) -> None:
        """
        Test files on S3 are being read correctly.
        """
        data_type = "OHLCV"
        root_dir = _AM_S3_ROOT_DIR
        cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
        actual = cdd_loader.read_data("binance::BTC_USDT")
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        data_type = "OHLCV"
        root_dir = _AM_S3_ROOT_DIR
        cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
        with self.assertRaises(AssertionError):
            cdd_loader.read_data("unsupported_exchange_id::BTC_USDT")

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        data_type = "OHLCV"
        root_dir = _AM_S3_ROOT_DIR
        cdd_loader = imcdalolo.CddLoader(data_type, root_dir, aws_profile="am")
        with self.assertRaises(AssertionError):
            cdd_loader.read_data("binance::unsupported_currency_pair")

    def test4(self) -> None:
        """
        Test unsupported data type.
        """
        root_dir = _AM_S3_ROOT_DIR
        data_type = "unsupported_data_type"
        with self.assertRaises(AssertionError):
            cdd_loader = imcdalolo.CddLoader(
                data_type, root_dir, aws_profile="am"
            )
