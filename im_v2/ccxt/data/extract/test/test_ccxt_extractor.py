import logging
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hserver as hserver
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.extractor as ivcdexex

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    not hserver.is_CK_S3_available(),
    reason="Run only if CK S3 is available",
)
class TestCcxtExtractor1(hunitest.TestCase):
    # Mock calls to external providers.
    # TODO(Nikola): Although this code belongs to `setUp`, when this code is
    #   moved there patch is created for each test separately. We want to avoid
    #   that and only start/stop same patch for each test.
    get_secret_patch = umock.patch.object(ivcdexex.hsecret, "get_secret")
    ccxt_patch = umock.patch.object(ivcdexex, "ccxt", spec=ivcdexex.ccxt)
    get_secret_mock = None
    ccxt_mock = None

    def setUp(self) -> None:
        super().setUp()
        #
        self.get_secret_mock: umock.MagicMock = self.get_secret_patch.start()
        self.ccxt_mock: umock.MagicMock = self.ccxt_patch.start()
        # Set dummy credentials for all tests.
        self.get_secret_mock.return_value = {"apiKey": "test", "secret": "test"}

    def tearDown(self) -> None:
        # We need to deallocate in reverse order to avoid race conditions.
        super().tearDown()
        #
        self.get_secret_patch.stop()
        self.ccxt_patch.stop()

    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        self.assertEqual(exchange_class.exchange_id, "binance")
        self.assertEqual(exchange_class.contract_type, "spot")
        self.assertEqual(exchange_class.vendor, "CCXT")
        self.assertEqual(
            exchange_class._exchange._extract_mock_name(), "ccxt.binance()"
        )
        actual_method_calls = str(exchange_class._exchange.method_calls)
        expected_method_calls = (
            "[call.checkRequiredCredentials(), call.load_markets()]"
        )
        self.assertEqual(actual_method_calls, expected_method_calls)
        # Wrong contract type.
        with pytest.raises(AssertionError) as fail:
            ivcdexex.CcxtExtractor("binance", "dummy")
        actual = str(fail.value)
        expected = (
            "Failed assertion *\n'dummy' in '['futures', 'spot']'\n"
            "Supported contract types: spot, futures"
        )
        self.assertIn(expected, actual)

    def test_log_into_exchange(self) -> None:
        """
        Verify that login to `ccxt` is done with proper params for different
        contracts.
        """
        exchange_mock = self.ccxt_mock.binance
        # Verify with `spot` contract type.
        _ = ivcdexex.CcxtExtractor("binance", "spot")
        actual_args = tuple(exchange_mock.call_args)
        expected_args = (
            ({"apiKey": "test", "rateLimit": True, "secret": "test"},),
            {},
        )
        self.assertEqual(actual_args, expected_args)
        # Verify with `futures` contract type.
        _ = ivcdexex.CcxtExtractor("binance", "futures")
        actual_args = tuple(exchange_mock.call_args)
        expected_args = (
            (
                {
                    "apiKey": "test",
                    "options": {"defaultType": "future"},
                    "rateLimit": True,
                    "secret": "test",
                },
            ),
            {},
        )
        self.assertEqual(actual_args, expected_args)
        # Check overall exchange initialization.
        self.assertEqual(exchange_mock.call_count, 2)

    @umock.patch.object(
        ivcdexex.CcxtExtractor,
        "_fetch_ohlcv",
        spec=ivcdexex.CcxtExtractor._fetch_ohlcv,
    )
    def test_download_ohlcv1(self, fetch_ohlcv_mock: umock.MagicMock):
        """
        Verify that wrapper around `ccxt.binance` download is properly called.
        """
        fetch_ohlcv_mock.return_value = pd.DataFrame(["dummy"], columns=["dummy"])
        # Prepare data and initialize class before run.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
        start_timestamp = pd.Timestamp("2022-02-24T00:00:00Z")
        end_timestamp = pd.Timestamp("2022-02-25T00:00:00Z")
        # As this is mock without spec `create=True` is used to create function that does not exist.
        # At the end of `with` statement change will be reverted.
        with umock.patch.object(
            exchange_class._exchange, "parse_timeframe", create=True
        ) as parse_timeframe_mock, umock.patch.object(
            ivcdexex.time, "sleep"
        ) as sleep_mock:
            parse_timeframe_mock.return_value = 60
            # Run.
            ohlcv_data = exchange_class._download_ohlcv(
                exchange_id="binance",
                currency_pair="BTC/USDT",
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            #
            self.assertEqual(parse_timeframe_mock.call_count, 1)
            actual_args = tuple(parse_timeframe_mock.call_args)
            expected_args = (("1m",), {})
            self.assertEqual(actual_args, expected_args)
            #
            self.assertEqual(sleep_mock.call_count, 3)
        # Check output.
        self.assertEqual(fetch_ohlcv_mock.call_count, 3)
        actual_args = str(fetch_ohlcv_mock.call_args_list)
        expected_args = r"""
        [call('BTC/USDT', since=1645660800000, bar_per_iteration=500),
         call('BTC/USDT', since=1645690800000, bar_per_iteration=500),
         call('BTC/USDT', since=1645720800000, bar_per_iteration=500)]
        """
        self.assert_equal(actual_args, expected_args, fuzzy_match=True)
        actual_ohlcv = hpandas.df_to_str(ohlcv_data)
        expected_ohlcv = r"""dummy
        0  dummy
        0  dummy
        0  dummy
        """
        self.assert_equal(actual_ohlcv, expected_ohlcv, fuzzy_match=True)

    @umock.patch.object(ivcdexex.hdateti, "get_current_time")
    def test_fetch_ohlcv1(self, mock_get_current_time: umock.MagicMock):
        """
        Verify if download from `ccxt.binance` is properly requested and parsed
        upon retrieval.
        """
        # Prepare test data.
        current_time = "2022-02-24 00:00:00.000000+00:00"
        mock_get_current_time.return_value = current_time
        bars = [
            [1645660800000, 37250.02, 37267.8, 37205.4, 37218.81, 59.1615],
            [1645660860000, 37218.8, 37234.26, 37213.2, 37214.46, 23.41537],
            [1645660920000, 37214.47, 37224.2, 37138.58, 37138.58, 48.11884],
            [1645660980000, 37138.59, 37216.5, 37100.17, 37216.49, 53.65817],
            [1645661040000, 37216.49, 37302.46, 37213.66, 37270.45, 36.44746],
        ]
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        expected_df = pd.DataFrame(data=bars, columns=columns)
        expected_df["end_download_timestamp"] = current_time
        expected_df = hpandas.df_to_str(expected_df)
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
        # As this is mock without spec `create=True` is used to create function that does not exist.
        # At the end of `with` statement change will be reverted.
        with umock.patch.object(
            exchange_class._exchange, "fetch_ohlcv", create=True
        ) as fetch_ohlcv_mock:
            fetch_ohlcv_mock.return_value = bars
            # Run.
            ohlcv_df = exchange_class._fetch_ohlcv(
                currency_pair="BTC_USDT",
                since=1,
                bar_per_iteration=2,
            )
            ohlcv_df = hpandas.df_to_str(ohlcv_df)
            self.assert_equal(ohlcv_df, expected_df)
            #
            self.assertEqual(fetch_ohlcv_mock.call_count, 1)
            actual_args = tuple(fetch_ohlcv_mock.call_args)
            expected_args = (
                ("BTC/USDT",),
                {"limit": 2, "since": 1, "timeframe": "1m"},
            )
            self.assertEqual(actual_args, expected_args)
            #
            actual_args = tuple(mock_get_current_time.call_args)
            expected_args = (("UTC",), {})
            self.assertEqual(actual_args, expected_args)
            self.assertEqual(mock_get_current_time.call_count, 1)

    def test_get_exchange_currency_pairs(self) -> None:
        """
        Test that a non-empty list of exchange currencies is loaded.
        """
        # Extract a list of currencies.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        with umock.patch.object(
            exchange_class._exchange, "load_markets", create=True
        ) as load_markets_mock:
            load_markets_mock.return_value = {"BTC/USDT": {}}
            # Run.
            actual = exchange_class.get_exchange_currency_pairs()
            #
            self.assertEqual(load_markets_mock.call_count, 1)
        # Verify that the output is a non-empty list with only string values.
        expected = ["BTC/USDT"]
        self.assertEqual(actual, expected)

    def test_download_ohlcv_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
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

    def test_download_ohlcv_invalid_input2(self) -> None:
        """
        Run with invalid end timestamp.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
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
        exchange_class.currency_pairs = ["BTC/USDT"]
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

    def test_download_order_book1(self) -> None:
        """
        Verify that order book is downloaded correctly.
        """
        exchange_class = ivcdexex.CcxtExtractor("gateio", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
        self.assertEqual(
            exchange_class._exchange._extract_mock_name(), "ccxt.gateio()"
        )
        #
        with umock.patch.object(
            exchange_class._exchange, "fetch_order_book", create=True
        ) as fetch_order_book_mock:
            fetch_order_book_mock.return_value = {"dummy": "dummy"}
            # Run.
            order_book = exchange_class.download_order_book("BTC_USDT")
            #
            self.assertEqual(fetch_order_book_mock.call_count, 1)
            actual_args = tuple(fetch_order_book_mock.call_args)
            expected_args = (("BTC/USDT",), {})
            self.assertEqual(actual_args, expected_args)
            # Check output.
            self.assertDictEqual(order_book, {"dummy": "dummy"})

    def test_download_order_book_invalid_input1(self) -> None:
        """
        Run with invalid currency pair.
        """
        # Initialize class.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        exchange_class.currency_pairs = ["BTC/USDT"]
        # Run with invalid input.
        with pytest.raises(AssertionError) as fail:
            exchange_class.download_order_book("invalid_currency_pair")
        # Check output for error.
        actual = str(fail.value)
        expected = "Currency pair is not present in exchange"
        self.assertIn(expected, actual)


class TestCcxtAPI1(hunitest.TestCase):
    def test_ccxt_api_call(self) -> None:
        """
        Verify that our api call to `ccxt` is not broken.
        """
        # Initiate class and set date parameters.
        exchange_class = ivcdexex.CcxtExtractor("binance", "spot")
        # Extract data.
        actual = exchange_class._download_ohlcv(
            currency_pair="BTC/USDT",
            exchange_id="binance",
            start_timestamp=None,
            end_timestamp=None,
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
        # Verify dataframe length. Only one bar is obtained.
        self.assertEqual(500, actual.shape[0])
