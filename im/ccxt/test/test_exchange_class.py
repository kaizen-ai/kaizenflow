import logging
from typing import Any, List, Optional

import pandas as pd

import helpers.unit_test as hut
import im.ccxt.exchange_class as iccexcl

_LOG = logging.getLogger(__name__)


class Test_CCXTExchange(hut.TestCase):
    @staticmethod
    def _conduct_asserts(
        actual: Any,
        exp_n_rows: int,
        exchange_class: Optional[iccexcl.CCXTExchange] = None,
        exp_first_date: Optional[str] = None,
        exp_last_date: Optional[str] = None,
    ) -> None:
        # Assert that the output is a dataframe and assert its size.
        dbg.dassert_isinstance(actual, pd.DataFrame)
        dbg.dassert_eq(tuple([exp_n_rows, 6]), actual.shape)
        # Assert column names.
        exp_col_names = ["timestamp", "open", "high", "close", "volume"]
        dbg.dassert_eq(list(actual.columns), exp_col_names)
        # Assert corner datetimes if output is not empty.
        if not actual.empty:
            exchange = exchange_class._exchange
            first_date = exchange.iso8601(int(actual["timestamp"].iloc[0]))
            last_date = exchange.iso8601(int(actual["timestamp"].iloc[-1]))
            dbg.dassert_eq(exp_first_date, first_date)
            dbg.dassert_eq(exp_last_date, last_date)


    def test_get_exchange_currencies(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Set a list of exchange ids to test and iterate over them.
        exchange_ids = ["binance", "kucoin"]
        for exchange_id in exchange_ids:
            # Extract a list of currencies.
            exchange_class = iccexcl.CCXTExchange(exchange_id)
            curr_list = exchange_class.get_exchange_currencies()
            # Assert that the output is a non-empty list.
            dbg.dassert_isinstance(curr_list, List)
            dbg.dassert_lt(0, len(curr_list))


    def test_download_ohlcv_data_binance1(self) -> None:
        """
        Test that Binance data is loaded correctly.
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
        # Conduct asserts.
        # Last date is going beyond end date due to step value.
        self._conduct_asserts(
            actual, 1500, exchange_class, start_date, "2021-09-10T00:59:00Z",
        )
        # Check the output values.
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test_download_ohlcv_data_binance2(self) -> None:
        """
        Test that Binance data is loaded correctly with non-default step.
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
            step=1000,
        )
        # Conduct asserts.
        # Last date is going beyond end date due to step value.
        self._conduct_asserts(
            actual, 2000, exchange_class, start_date, "2021-09-10T09:19:00Z",
        )
        # Check the output values.
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test_download_ohlcv_data_binance3(self) -> None:
        """
        Test that empty dataframe is loaded for period with no data.
        """
        # Initiate class.
        exchange_class = iccexcl.CCXTExchange("binance")
        # Set dates of a period with no data.
        start_date = "2011-01-09T00:00:00Z"
        end_date = "2011-01-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
            curr_symbol="BTC/USDT",
        )
        # Conduct asserts.
        # Last date is going beyond end date due to step value.
        self._conduct_asserts(actual, 0)

    def test_download_ohlcv_data_kucoin1(self) -> None:
        """
        Test that Kucoin data is loaded correctly.
        """
        # Initiate class and set date parameters.
        exchange_class = iccexcl.CCXTExchange("kucoin")
        start_date = "2021-09-09T00:00:00Z"
        end_date = "2021-09-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
            curr_symbol="BTC/USDT",
        )
        # Conduct asserts.
        # Last date is going beyond end date due to step value.
        self._conduct_asserts(
            actual, 1500, exchange_class, start_date, "2021-09-10T00:59:00Z",
        )
        # Check the output values.
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test_download_ohlcv_data_kucoin2(self) -> None:
        """
        Test that Kucoin data is loaded correctly with non-default step.
        """
        # Initiate class and set date parameters.
        exchange_class = iccexcl.CCXTExchange("kucoin")
        start_date = "2021-09-09T00:00:00Z"
        end_date = "2021-09-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
            curr_symbol="BTC/USDT",
            step=1000,
        )
        # Conduct asserts.
        # Last date is going beyond end date due to step value.
        self._conduct_asserts(
            actual, 2000, exchange_class, start_date, "2021-09-10T09:19:00Z",
        )
        # Check the output values.
        actual_string = hut.convert_df_to_string(actual, index=True)
        self.check_string(actual_string)

    def test_download_ohlcv_data_kucoin3(self) -> None:
        """
        Test that empty dataframe is loaded for period with no data.
        """
        # Initiate class.
        exchange_class = iccexcl.CCXTExchange("kucoin")
        # Set dates of a period with no data.
        start_date = "2011-01-09T00:00:00Z"
        end_date = "2011-01-10T00:00:00Z"
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            start_datetime=pd.Timestamp(start_date),
            end_datetime=pd.Timestamp(end_date),
            curr_symbol="BTC/USDT",
        )
        # Conduct asserts.
        self._conduct_asserts(actual, 0)
