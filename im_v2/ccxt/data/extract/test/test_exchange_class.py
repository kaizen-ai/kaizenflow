import logging

import pandas as pd
import pytest

import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest

# TODO(Dan): return to code after CmTask43 is fixed.
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl

_LOG = logging.getLogger(__name__)


@pytest.mark.skip(reason="CMTask961")
class Test_CcxtExchange(hunitest.TestCase):
    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        _ = imvcdeexcl.CcxtExchange("binance")

    def test_get_exchange_currencies(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Extract a list of currencies.
        exchange_class = imvcdeexcl.CcxtExchange("binance")
        curr_list = exchange_class.get_exchange_currency_pairs()
        # Verify that the output is a non-empty list with only string values.
        hdbg.dassert_container_type(curr_list, list, str)
        self.assertGreater(len(curr_list), 0)

    @pytest.mark.slow()
    def test_download_ohlcv_data(self) -> None:
        """
        Test that historical data is being loaded correctly.
        """
        # Initiate class and set date parameters.
        exchange_class = imvcdeexcl.CcxtExchange("binance")
        start_date = "2021-09-09T00:00:00Z"
        end_date = "2021-09-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            currency_pair="BTC/USDT",
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
        )
        # Verify that the output is a dataframe and verify its length.
        hdbg.dassert_isinstance(actual, pd.DataFrame)
        self.assertEqual(1500, actual.shape[0])
        # Verify column names.
        exp_col_names = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "created_at",
        ]
        self.assertEqual(exp_col_names, actual.columns.to_list())
        # Verify types inside each column.
        col_types = [col_type.name for col_type in actual.dtypes]
        exp_col_types = [
            "int64",
            "float64",
            "float64",
            "float64",
            "float64",
            "float64",
            "object",
        ]
        self.assertEqual(exp_col_types, col_types)
        # Verify corner datetimes if output is not empty.
        first_date = int(actual["timestamp"].iloc[0])
        last_date = int(actual["timestamp"].iloc[-1])
        self.assertEqual(1631145600000, first_date)
        self.assertEqual(1631235540000, last_date)
        # Check the output values.
        actual_string = hunitest.convert_df_to_json_string(actual, n_tail=None)
        self.check_string(actual_string)

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
