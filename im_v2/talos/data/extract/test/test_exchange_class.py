import logging
import unittest.mock as umock
from typing import Type, Union

import pandas as pd
import pytest

import helpers.hdbg as hdbg
import helpers.hunit_test as hunitest
import im_v2.talos.data.extract.exchange_class as imvtdeexcl

_LOG = logging.getLogger(__name__)


@pytest.mark.skip("Enable after CMTask1292 is resolved.")
class TestTalosExchange1(hunitest.TestCase):

    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        _ = imvtdeexcl.TalosExchange("sandbox")

    @pytest.mark.slow()
    @umock.patch.object(imvtdeexcl.hdateti, "get_current_time")
    def test_download_ohlcv_data1(
        self, mock_get_current_time: umock.MagicMock
    ) -> None:
        """
        Test download for historical data.
        """
        mock_get_current_time.return_value = "2021-09-09 00:00:00.000000+00:00"
        start_timestamp = pd.Timestamp("2021-09-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2021-09-10T00:00:00", tz="UTC")
        actual = self.download_ohlcv_data(start_timestamp, end_timestamp)
        # Verify dataframe length.
        self.assertEqual(1440, actual.shape[0])
        # Check number of calls and args for current time.
        self.assertEqual(mock_get_current_time.call_count, 2)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        # Verify corner datetime if output is not empty.
        first_date = int(actual["timestamp"].iloc[0])
        last_date = int(actual["timestamp"].iloc[-1])
        self.assertEqual(1631145600000, first_date)
        # Talos considers [a, b) time interval so last minute is missing.
        self.assertEqual(1631231940000, last_date)
        # Check the output values.
        actual = actual.reset_index(drop=True)
        actual = hunitest.convert_df_to_json_string(actual)
        self.check_string(actual)

    def test_download_ohlcv_data_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        cur_pair = "BTC_USDT"
        exchange = "binance"
        # End is before start -> invalid.
        start_timestamp = "invalid"
        end_timestamp = pd.Timestamp("2021-09-10T00:00:00", tz="UTC")
        expected = (
            "'invalid' is '<class 'str'>' instead of "
            "'<class 'pandas._libs.tslibs.timestamps.Timestamp'"
        )
        self.download_ohlcv_data_invalid_input_param_helper(
            cur_pair,
            exchange,
            start_timestamp,
            end_timestamp,
            expected,
            AssertionError,
        )

    def test_download_ohlcv_data_invalid_input2(self) -> None:
        """
        Run with invalid end timestamp.
        """
        cur_pair = "BTC_USDT"
        exchange = "binance"
        # End is before start -> invalid.
        start_timestamp = pd.Timestamp("2021-09-10T00:00:00", tz="UTC")
        end_timestamp = "invalid"
        expected = (
            "'invalid' is '<class 'str'>' instead of "
            "'<class 'pandas._libs.tslibs.timestamps.Timestamp'"
        )
        self.download_ohlcv_data_invalid_input_param_helper(
            cur_pair,
            exchange,
            start_timestamp,
            end_timestamp,
            expected,
            AssertionError,
        )

    def test_download_ohlcv_data_invalid_input3(self) -> None:
        """
        Run with invalid range.

        Start greater than the end.
        """
        cur_pair = "BTC_USDT"
        exchange = "binance"
        # End is before start -> invalid.
        start_timestamp = pd.Timestamp("2021-09-10T00:00:00")
        end_timestamp = pd.Timestamp("2021-09-09T00:00:00")
        expected = "2021-09-10 00:00:00 <= 2021-09-09 00:00:00"
        self.download_ohlcv_data_invalid_input_param_helper(
            cur_pair,
            exchange,
            start_timestamp,
            end_timestamp,
            expected,
            AssertionError,
        )

    def test_download_ohlcv_data_invalid_input4(self) -> None:
        """
        Run with invalid currency pair.
        """
        cur_pair = "invalid_pair"
        exchange = "binance"
        start_timestamp = pd.Timestamp("2021-09-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2021-09-10T00:00:00", tz="UTC")
        expected = "Finished with code: 400"
        self.download_ohlcv_data_invalid_input_param_helper(
            cur_pair,
            exchange,
            start_timestamp,
            end_timestamp,
            expected,
            ValueError,
        )

    def test_download_ohlcv_data_invalid_input5(self) -> None:
        """
        Run with invalid exchange.
        """
        cur_pair = "BTC_USDT"
        exchange = "unknown_exchange"
        start_timestamp = pd.Timestamp("2021-09-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2021-09-10T00:00:00", tz="UTC")
        expected = "Finished with code: 400"
        self.download_ohlcv_data_invalid_input_param_helper(
            cur_pair,
            exchange,
            start_timestamp,
            end_timestamp,
            expected,
            ValueError,
        )

    # Allow params to be also str, so we can test raising
    # of assertion errors.
    def download_ohlcv_data_invalid_input_param_helper(
        self,
        currency_pair: str,
        exchange: str,
        start_timestamp: Union[pd.Timestamp, str],
        end_timestamp: Union[pd.Timestamp, str],
        expected: str,
        raises: Union[Type[AssertionError], Type[ValueError]],
    ) -> None:
        """
        Make creation of tests with invalid input easier.
        """
        # Initialize class.
        exchange_class = imvtdeexcl.TalosExchange("sandbox")
        # Run with invalid input.
        with pytest.raises(raises) as fail:
            exchange_class.download_ohlcv_data(
                currency_pair=currency_pair,
                exchange=exchange,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        # Check output for error.
        actual = str(fail.value)
        self.assertIn(expected, actual)

    def download_ohlcv_data(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        Test that data is being loaded correctly.

        Data is returned for further checking in different tests.
        """
        # Initiate class and set date parameters.
        exchange_class = imvtdeexcl.TalosExchange("sandbox")
        # Extract data.
        actual = exchange_class.download_ohlcv_data(
            currency_pair="BTC_USDT",
            exchange="binance",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            bar_per_iteration=1000,
        )
        # Verify that the output is a dataframe.
        hdbg.dassert_isinstance(actual, pd.DataFrame)
        # Verify column names.
        exp_col_names = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ticks",
            "end_download_timestamp",
        ]
        self.assertEqual(exp_col_names, actual.columns.to_list())
        # Verify types inside each column.
        # col_types = [col_type.name for col_type in actual.dtypes]
        # exp_col_types = [
        #     "int64",
        #     "float64",
        #     "float64",
        #     "float64",
        #     "float64",
        #     "float64",
        #     "object",
        # ]
        # self.assertListEqual(exp_col_types, col_types)
        return actual
