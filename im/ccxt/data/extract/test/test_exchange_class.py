import logging
from typing import Any, List, Optional

import pandas as pd
import pytest

import helpers.unit_test as hut
# TODO(Dan): return to code after CmTask43 is fixed.
import im.ccxt.data.extract.exchange_class as imcdaexexccla
import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


# TODO(gp): CmampTask413: Why skip this guy?
@pytest.mark.skip()
class Test_CcxtExchange(hut.TestCase):
    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        _ = imcdaexexccla.CcxtExchange("binance")

    def test_get_exchange_currencies(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Extract a list of currencies.
        exchange_class = imcdaexexccla.CcxtExchange("binance")
        curr_list = exchange_class.get_exchange_currencies()
        # Verify that the output is a non-empty list with only string values.
        hdbg.dassert_container_type(curr_list, list, str)
        self.assertGreater(len(curr_list), 0)

    def test_download_ohlcv_data(self) -> None:
        """
        Test that historical data is being loaded correctly.
        """
        # Initiate class and set date parameters.
        exchange_class = imcdaexexccla.CcxtExchange("binance")
        start_date = "2021-09-09T00:00:00Z"
        end_date = "2021-09-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            curr_symbol="BTC/USDT",
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
        )
        # Verify that the output is a dataframe and verify its length.
        dbg.dassert_isinstance(actual, pd.DataFrame)
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
        actual_string = hut.convert_df_to_json_string(actual, n_tail=None)
        self.check_string(actual_string)

    def test_download_order_book(self):
        """
        Verify that order book is downloaded correctly.
        """
        exchange_class = imcdaexexccla.CcxtExchange("gateio")
        order_book = exchange_class.download_order_book("BTC/USDT")
        order_book_keys = ["symbol", "bids", "asks", "timestamp", "datetime", "nonce"]
        self.assertListEqual(order_book_keys, list(order_book.keys()))
