import logging
from typing import Any, List, Optional

import pandas as pd
import pytest

import helpers.dbg as hdbg
import helpers.unit_test as hunitest

# TODO(Dan): return to code after CmTask43 is fixed.
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl

_LOG = logging.getLogger(__name__)


#@pytest.mark.skip()
class Test_CcxtExchange(hunitest.TestCase):
    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        _ = imvcdeexcl.CcxtExchange("binance")

    def test_unsupported_exchange(self) -> None:
        """
        Test that initializing the class with unsupported exchange
        string raises AssertionErrors
        """
        raise NotImplementedError

    def test_get_exchange_currencies(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Extract a list of currencies.
        exchange_class = imvcdeexcl.CcxtExchange("binance")
        curr_list = exchange_class.get_exchange_currencies()
        # Verify that the output is a non-empty list with only string values.
        hdbg.dassert_container_type(curr_list, list, str)
        self.assertGreater(len(curr_list), 0)

    def test_fetch_ohlcv(self) -> None:
        """
        Test that a single iteration of loading historical data works correctly:
        - timestamps are not outside the specified interval
        - returned dataframe has the expected row count (most likely only if start date is far enough
          in the past)
        - returned dataframe has the expected column count and column names
        """
        raise NotImplementedError

    def test_fetch_ohlcv_invalid_params(self):
        """
        Test that a single iteration of loading historical data returns empty result if:
        - step is <= 0
        - the provided start date (since) is in the future
        - timeframe argument has an invalid format
        - currency pair is invalid
        """
        raise NotImplementedError

    def test_download_ohlcv_data(self) -> None:
        """
        Test that historical data is being loaded correctly.
        TODO(Juraj): add test to check all timestamps are within requested interval
        """
        # Initiate class and set date parameters.
        exchange_class = imvcdeexcl.CcxtExchange("binance")
        start_date = "2021-09-09T00:00:00Z"
        end_date = "2021-09-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            curr_symbol="BTC_USDT",
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
        )
        # Verify that the output is a dataframe and verify its length.
        hdbg.dassert_isinstance(actual, pd.DataFrame)
        self.assertEqual(1500, actual.shape[0])
        # Verify column names.
        exp_col_names = ["timestamp", "open", "high", "close", "volume"]
        self.assertEqual(exp_col_names, actual.columns.to_list())
        # Verify types inside each column.
        col_types = [col_type.name for col_type in actual.dtypes]
        exp_col_types = ["int64"] + ["float64"] * 5
        self.assertEqual(exp_col_types, col_types)
        # Verify corner datetimes if output is not empty.
        first_date = int(actual["timestamp"].iloc[0])
        last_date = int(actual["timestamp"].iloc[-1])
        self.assertEqual(1631145600000, first_date)
        self.assertEqual(1631235540000, last_date)
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual, n_tail=None)
        self.check_string(actual_string)

    def test_download_ohlcv_data_invalid_params(self):
        """
        Test that the download_ohlcv_data method returns empty result if:
        - step is <= 0
        - the provided start date (since) is in the future
        - the end date is before the start date
        - currency pair is in invalid format
        - timeframe argument has an invalid format
        """
        raise NotImplementedError

    def test_download_order_book(self):
        """
        Verify that order book is downloaded correctly.
        """
        exchange_class = imvcdeexcl.CcxtExchange("gateio")
        order_book = exchange_class.download_order_book("BTC_USDT")
        order_book_keys = [
            "symbol",
            "bids",
            "asks",
            "timestamp",
            "datetime",
            "nonce",
        ]
        self.assertListEqual(order_book_keys, list(order_book.keys()))
        # TODO(Juraj): assert that passed currency pair matches the value of symbol key
