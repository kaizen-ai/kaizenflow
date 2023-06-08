import logging
import unittest.mock as umock

import ccxt
import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.extractor as imvcdexex

_LOG = logging.getLogger(__name__)


class TestCcxtVersion1(hunitest.TestCase):
    @umock.patch.object(imvcdexex.hdateti, "get_current_time")
    def test_download_ohlcv_with_current_ccxt_version1(
        self, mock_get_current_time: umock.MagicMock
    ) -> None:
        """
        Run with the current ccxt version to verify that data semantics don't
        change.
        """
        current_time = "2022-10-21 00:00:00.000000+00:00"
        mock_get_current_time.return_value = current_time
        # Prepare expected output.
        # Expected output: This is downloaded data with the CCXT version 2.1.80.
        expected_output = r"""
        timestamp    open    high    low    close    volume    end_download_timestamp
        0    1666224060000    19120.48    19123.30    19114.05    19114.05    0.483268    2022-10-21 00:00:00.000000+00:00
        1    1666224120000    19117.81    19128.66    19117.81    19128.66    0.813596    2022-10-21 00:00:00.000000+00:00
        2    1666224180000    19128.27    19128.27    19115.57    19122.26    1.800264    2022-10-21 00:00:00.000000+00:00
        3    1666224240000    19118.08    19122.31    19115.23    19115.23    0.482966    2022-10-21 00:00:00.000000+00:00
        4    1666224300000    19120.87    19120.87    19113.28    19113.28    0.434573    2022-10-21 00:00:00.000000+00:00
        """
        # Initialize class.
        # Using Binanceus API because Binance API is not accessible.
        exchange_class = imvcdexex.CcxtExtractor("binanceus", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
        start_timestamp = pd.Timestamp("2022-10-20T00:01:00Z")
        end_timestamp = pd.Timestamp("2022-10-20T00:05:00Z")
        # Run.
        ccxt_data = exchange_class._download_ohlcv(
            exchange_id="binanceus",
            currency_pair="BTC/USDT",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            bar_per_iteration=500,
        )
        _LOG.info("\n==> Current CCXT version=%s <==", ccxt.__version__)
        # Check output.
        actual_output = hpandas.df_to_str(ccxt_data)
        self.assert_equal(actual_output, expected_output, fuzzy_match=True)
