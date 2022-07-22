import logging
import unittest.mock as umock
from typing import Optional

import pandas as pd
import pytest

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.extractor as ivcdexex

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestCcxtExtractor1(hunitest.TestCase):
    @umock.patch.object(ivcdexex.hsecret, "get_secret")
    @umock.patch.object(ivcdexex, "ccxt", spec=ivcdexex.ccxt)
    def test_initialize_class(self, _, __) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        self.assertEqual(exchange_class.exchange_id, "binance")
        self.assertEqual(exchange_class.contract_type, "spot")
        self.assertEqual(exchange_class.vendor, "CCXT")
        self.assertEqual(exchange_class._exchange._extract_mock_name(), "ccxt.binance()")
        actual_method_calls = str(exchange_class._exchange.method_calls)
        expected_method_calls = "[call.checkRequiredCredentials(), call.load_markets()]"
        self.assertEqual(actual_method_calls, expected_method_calls)


    @umock.patch.object(ivcdexex.hsecret, "get_secret")
    @umock.patch.object(ivcdexex, "ccxt", spec=ivcdexex.ccxt)
    def test_log_into_exchange(self, ccxt_mock: umock.MagicMock, get_secret_mock: umock.MagicMock) -> None:
        """
        TODO(Nikola): Docs, invalid contract type ... list of markets, etc.
        """
        get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}
        # Verify with `spot` contract type.
        _ = ivcdexex.CcxtExtractor("binance", "spot")
        exchange_mock = ccxt_mock.binance
        actual_args = exchange_mock.call_args.args
        actual_kwargs = exchange_mock.call_args.kwargs
        expected_args = ({'apiKey': 'test', 'rateLimit': True, 'secret': 'test'},)
        expected_kwargs = {}
        self.assertEqual(actual_args, expected_args)
        self.assertEqual(actual_kwargs, expected_kwargs)
        # Verify with `futures` contract type.
        _ = ivcdexex.CcxtExtractor("binance", "futures")
        actual_args = exchange_mock.call_args.args
        actual_kwargs = exchange_mock.call_args.kwargs
        expected_args = ({'apiKey': 'test', 'options': {'defaultType': 'future'}, 'rateLimit': True, 'secret': 'test'},)
        expected_kwargs = {}
        self.assertEqual(actual_args, expected_args)
        self.assertEqual(actual_kwargs, expected_kwargs)
        # Check overall exchange initialization.
        self.assertEqual(exchange_mock.call_count, 2)

    @umock.patch.object(ivcdexex.hsecret, "get_secret")
    @umock.patch.object(ivcdexex, "ccxt", spec=ivcdexex.ccxt)
    @umock.patch.object(ivcdexex.CcxtExtractor, "_fetch_ohlcv", spec=ivcdexex.CcxtExtractor._fetch_ohlcv)
    def test__download_ohlcv(self, fetch_ohlcv_mock: umock.MagicMock,  _, __):
        """
        TODO(Nikola): ... We will just gonna check what was passed to `fetch_ohlcv_mock`...
            and check if is properly converted to dataframe.

            Same pattern for testing `_fetch_ohlcv` from `ccxt` and `download_order_book`.
        """
        fetch_ohlcv_mock.return_value = [
            [1645660800000, 37250.02, 37267.8, 37205.4, 37218.81, 59.1615],
            [1645660860000, 37218.8, 37234.26, 37213.2, 37214.46, 23.41537],
            [1645660920000, 37214.47, 37224.2, 37138.58, 37138.58, 48.11884],
            [1645660980000, 37138.59, 37216.5, 37100.17, 37216.49, 53.65817],
            [1645661040000, 37216.49, 37302.46, 37213.66, 37270.45, 36.44746]
        ]
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        pass


    def test_get_exchange_currency_pairs(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Extract a list of currencies.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        curr_list = exchange_class.get_exchange_currency_pairs()
        # Verify that the output is a non-empty list with only string values.
        hdbg.dassert_container_type(curr_list, list, str)
        self.assertGreater(len(curr_list), 0)

    @pytest.mark.skip(reason="CMTask2089")
    @umock.patch.object(ivcdexex.hdateti, "get_current_time")
    def test_download_ohlcv1(
        self, mock_get_current_time: umock.MagicMock
    ) -> None:
        """
        Test download for historical data.
        """
        mock_get_current_time.return_value = "2021-09-09 00:00:00.000000+00:00"
        start_timestamp = pd.Timestamp("2021-09-09T00:00:00Z")
        end_timestamp = pd.Timestamp("2021-09-10T00:00:00Z")
        actual = self._download_ohlcv(start_timestamp, end_timestamp)
        # Verify dataframe length.
        self.assertEqual(1500, actual.shape[0])
        # Check number of calls and args for current time.
        self.assertEqual(mock_get_current_time.call_count, 3)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        # Verify corner datetime if output is not empty.
        first_date = int(actual["timestamp"].iloc[0])
        last_date = int(actual["timestamp"].iloc[-1])
        self.assertEqual(1631145600000, first_date)
        self.assertEqual(1631235540000, last_date)
        # Check the output values.
        actual = hpandas.convert_df_to_json_string(actual, n_tail=None)
        self.check_string(actual)

    def test_download_ohlcv2(self) -> None:
        """
        Test download for latest bars when no timestamps are provided.
        """
        actual = self._download_ohlcv(None, None)
        # Verify dataframe length. Only one bar is obtained.
        self.assertEqual(500, actual.shape[0])

    def test_download_ohlcv_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Run with invalid input.
        start_timestamp = "invalid"
        end_timestamp = pd.Timestamp("2021-09-10T00:00:00Z")
        with pytest.raises(AssertionError) as fail:
            exchange_class._download_ohlcv(
                exchange_id="binance",
                currency_pair="BTC/USDT",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        # Check output for error.
        actual = str(fail.value)
        expected = (
            "'invalid' is '<class 'str'>' instead of "
            "'<class 'pandas._libs.tslibs.timestamps.Timestamp'"
        )
        self.assertIn(expected, actual)

    @pytest.mark.skip(reason="CMTask2089")
    def test_download_ohlcv_invalid_input2(self) -> None:
        """
        Run with invalid end timestamp.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Run with invalid input.
        start_timestamp = pd.Timestamp("2021-09-09T00:00:00Z")
        end_timestamp = "invalid"
        with pytest.raises(AssertionError) as fail:
            exchange_class._download_ohlcv(
                exchange_id="binance",
                currency_pair="BTC/USDT",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        # Check output for error.
        actual = str(fail.value)
        expected = (
            "'invalid' is '<class 'str'>' instead of "
            "'<class 'pandas._libs.tslibs.timestamps.Timestamp'"
        )
        self.assertIn(expected, actual)

    def test_download_ohlcv_invalid_input3(self) -> None:
        """
        Run with invalid range.

        Start greater than the end.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Run with invalid input.
        start_timestamp = pd.Timestamp("2021-09-10T00:00:00Z")
        end_timestamp = pd.Timestamp("2021-09-09T00:00:00Z")
        with pytest.raises(AssertionError) as fail:
            exchange_class._download_ohlcv(
                exchange_id="binance",
                currency_pair="BTC/USDT",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        # Check output for error.
        actual = str(fail.value)
        expected = "2021-09-10 00:00:00+00:00 <= 2021-09-09 00:00:00+00:00"
        self.assertIn(expected, actual)

    def test_download_ohlcv_invalid_input4(self) -> None:
        """
        Run with invalid currency pair.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Run with invalid input.
        with pytest.raises(AssertionError) as fail:
            exchange_class._download_ohlcv(
                exchange_id="binance",
                currency_pair="invalid_currency_pair",
                start_timestamp=None,
                end_timestamp=None,
            )
        # Check output for error.
        actual = str(fail.value)
        expected = "Currency pair is not present in exchange"
        self.assertIn(expected, actual)

    def test_download_order_book(self) -> None:
        """
        Verify that order book is downloaded correctly.
        """
        exchange_class = ivcdexex.CcxtExtractor("gateio", "spot")
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

    @pytest.mark.skip(reason="CMTask2089")
    def test_download_order_book_invalid_input1(self) -> None:
        """
        Run with invalid currency pair.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Run with invalid input.
        with pytest.raises(AssertionError) as fail:
            exchange_class.download_order_book("invalid_currency_pair")
        # Check output for error.
        actual = str(fail.value)
        expected = "Currency pair is not present in exchange"
        self.assertIn(expected, actual)

    def _download_ohlcv(
        self,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Test that data is being loaded correctly.

        Data is returned for further checking in different tests.
        """
        # Initiate class and set date parameters.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Extract data.
        actual = exchange_class._download_ohlcv(
            currency_pair="BTC/USDT",
            exchange_id="binance",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
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
            "end_download_timestamp",
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
        self.assertListEqual(exp_col_types, col_types)
        return actual
