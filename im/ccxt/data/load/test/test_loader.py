import os
from typing import List

import pandas as pd
import pytest

import helpers.s3 as hs3
import helpers.unit_test as hunitest
import im.ccxt.data.load.loader as imcdalolo
import im_v2.data.universe as imv2dauni

_AM_S3_ROOT_DIR = os.path.join(hs3.get_path(), "data")


class TestGetFilePath(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test supported exchange id and currency pair.
        """
        exchange_id = "binance"
        currency_pair = "ETH/USDT"
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader._get_file_path(
            imcdalolo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
        )
        # TODO(gp): CmampTask413: Use get_bucket()
        expected = (
            "s3://alphamatic-data/data/ccxt/20210924/binance/ETH_USDT.csv.gz"
        )
        self.assert_equal(actual, expected)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        exchange_id = "unsupported exchange"
        currency_pair = "ADA/USDT"
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        # TODO(gp): We should throw a different exception, like
        # `UnsupportedExchange`.
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imcdalolo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )

    def test3(self) -> None:
        """
        Test unsupported currency pair.
        """
        exchange_id = "binance"
        currency_pair = "unsupported_currency"
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        # TODO(gp): Same change also for CDD test_loader.py
        with self.assertRaises(AssertionError):
            ccxt_loader._get_file_path(
                imcdalolo._LATEST_DATA_SNAPSHOT, exchange_id, currency_pair
            )


class TestReadUniverseDataFromFilesystem(hunitest.TestCase):
    def test1(self) -> None:
        """
        Test that all files from universe version are being read correctly.
        """
        # Initialize loader and get actual result.
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_universe_data_from_filesystem(
            universe="small", data_type="OHLCV"
        )
        actual_json = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_json)

    @pytest.mark.slow("18 seconds.")
    def test2(self) -> None:
        """
        Test that data for provided list of tuples is being read correctly.
        """
        # Set input universe.
        input_universe = [
            imv2dauni.ExchangeCurrencyTuple("kucoin", "BTC/USDT"),
            imv2dauni.ExchangeCurrencyTuple("kucoin", "ETH/USDT"),
        ]
        # Initialize loader and get actual result.
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_universe_data_from_filesystem(
            universe=input_universe, data_type="OHLCV"
        )
        actual_json = hunitest.convert_df_to_json_string(actual)
        # Check output.
        self.check_string(actual_json)

    def test3(self) -> None:
        """
        Test that all files from small test universe are being read correctly.
        """
        # Initialize loader and get actual result.
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_universe_data_from_filesystem(
            universe="small", data_type="OHLCV"
        )
        # Check output.
        expected_length = 190046
        expected_exchange_ids = ["gateio", "kucoin"]
        expected_currency_pairs = ["SOL/USDT", "XRP/USDT"]
        self._check_output(
            actual,
            expected_length,
            expected_exchange_ids,
            expected_currency_pairs,
        )

    def _check_output(
        self,
        actual: pd.DataFrame,
        expected_length: int,
        expected_exchange_ids: List[str],
        expected_currency_pairs: List[str],
    ) -> None:
        """
        Verify that actual outcome dataframe matches the expected one.

        :param actual: actual outcome dataframe
        :param expected_length: expected outcome dataframe length
        :param expected_exchange_ids: list of expected exchange ids
        :param expected_currency_pairs: list of expected currency pairs
        """
        # Check output df length.
        self.assert_equal(str(expected_length), str(actual.shape[0]))
        # Check unique exchange ids in the output df.
        actual_exchange_ids = sorted(list(actual["exchange_id"].unique()))
        self.assert_equal(str(actual_exchange_ids), str(expected_exchange_ids))
        # Check unique currency pairs in the output df.
        actual_currency_pairs = sorted(list(actual["currency_pair"].unique()))
        self.assert_equal(
            str(actual_currency_pairs), str(expected_currency_pairs)
        )
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)


# TODO(*): Consider to factor out the class calling in a `def _get_loader()`.
class TestReadDataFromFilesystem(hunitest.TestCase):
    @pytest.mark.slow
    def test1(self) -> None:
        """
        Test that files on S3 are being read correctly.
        """
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        actual = ccxt_loader.read_data_from_filesystem(
            "binance", "BTC/USDT", "OHLCV"
        )
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual_string)

    def test2(self) -> None:
        """
        Test unsupported exchange id.
        """
        ccxt_loader = imcdalolo.CcxtLoader(
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
        ccxt_loader = imcdalolo.CcxtLoader(
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
        ccxt_loader = imcdalolo.CcxtLoader(
            root_dir=_AM_S3_ROOT_DIR, aws_profile="am"
        )
        with self.assertRaises(AssertionError):
            ccxt_loader.read_data_from_filesystem(
                "binance", "BTC/USDT", "unsupported_data_type"
            )
