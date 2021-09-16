import logging
from typing import Any, List, Optional

import pandas as pd
import pytest

import helpers.unit_test as hut
# TODO(Dan): return to code after CmTask43 is fixed.
# import im.ccxt.exchange_class as iccexcl

_LOG = logging.getLogger(__name__)


class Test_CCXTExchange(hut.TestCase):
    @pytest.mark.skip()
    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        _ = iccexcl.CCXTExchange("binance")

    @pytest.mark.skip()
    def test_get_exchange_currencies(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Extract a list of currencies.
        exchange_class = iccexcl.CCXTExchange("binance")
        curr_list = exchange_class.get_exchange_currencies()
        # Verify that the output is a non-empty list with only string values.
        dbg.dassert_isinstance(curr_list, list)
        dbg.dassert_lt(0, len(curr_list))
        dbg.dassert_eq(all(isinstance(curr, str) for curr in curr_list)

    @ pytest.mark.skip()
    def test_download_ohlcv_data(self) -> None:
        """
        Test that historical data is being loaded correctly.
        """
        # Initiate class and set date parameters.
        exchange_class = iccexcl.CCXTExchange("binance")
        start_date = "2021-09-09T00:00:00Z"
        end_date = "2021-09-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
            curr_symbol="BTC/USDT",
        )
        # Verify that the output is a dataframe and verify its length.
        dbg.dassert_isinstance(actual, pd.DataFrame)
        dbg.dassert_eq(1500, actual.shape[0])
        # Verify column names.
        exp_col_names = ["timestamp", "open", "high", "close", "volume"]
        dbg.dassert_eq(list(actual.columns), exp_col_names)
        # Verify types inside each column.
        col_types = [col_type.name for col_type in actual.dtypes]
        exp_col_types = ["int64"] + ["float64"] * 5
        dbg.dassert_eq(exp_col_types, col_types)
        # Verify corner datetimes if output is not empty.
        first_date = int(actual["timestamp"].iloc[0])
        last_date = int(actual["timestamp"].iloc[-1])
        dbg.dassert_eq(1631145600000, first_date)
        dbg.dassert_eq(1631235540000, last_date)
        # Check the output values.
        actual_string = hut.convert_df_to_json_string(actual, n_tail=None)
        self.check_string(actual_string)
