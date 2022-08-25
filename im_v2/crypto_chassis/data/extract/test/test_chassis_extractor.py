import logging
import unittest.mock as umock

import pandas as pd

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.extract.extractor as imvccdexex

_LOG = logging.getLogger(__name__)


class TestCryptoChassisExtractor1(hunitest.TestCase):
    pandas_read_csv_patch = umock.patch.object(
        imvccdexex.pd, "read_csv", spec=imvccdexex.pd.read_csv
    )
    coerce_to_numeric_patch = umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "coerce_to_numeric",
        spec=imvccdexex.CryptoChassisExtractor.coerce_to_numeric,
    )
    requests_patch = umock.patch.object(
        imvccdexex, "requests", spec=imvccdexex.requests
    )
    build_query_url_patch = umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_query_url",
        spec=imvccdexex.CryptoChassisExtractor._build_query_url,
    )
    build_base_url_patch = umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_base_url",
        spec=imvccdexex.CryptoChassisExtractor._build_base_url,
    )
    convert_currency_pair_patch = umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "convert_currency_pair",
        spec=imvccdexex.CryptoChassisExtractor.convert_currency_pair,
    )

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's start() method.
        self.pandas_read_csv_mock: umock.MagicMock = (
            self.pandas_read_csv_patch.start()
        )
        self.coerce_to_numeric_mock: umock.MagicMock = (
            self.coerce_to_numeric_patch.start()
        )
        self.requests_mock: umock.MagicMock = self.requests_patch.start()
        self.build_query_url_mock: umock.MagicMock = (
            self.build_query_url_patch.start()
        )
        self.build_base_url_mock: umock.MagicMock = (
            self.build_base_url_patch.start()
        )
        self.convert_currency_pair_mock: umock.MagicMock = (
            self.convert_currency_pair_patch.start()
        )

    def tearDown(self) -> None:
        self.convert_currency_pair_patch.stop()
        self.build_base_url_patch.stop()
        self.build_query_url_patch.stop()
        self.requests_patch.stop()
        self.coerce_to_numeric_patch.stop()
        self.pandas_read_csv_patch.stop()
        # Deallocate in reverse order to avoid race conditions.
        super().tearDown()

    def test_initialize_class(self) -> None:
        """
        Smoke test that the class is being initialized correctly.
        """
        exchange_class = imvccdexex.CryptoChassisExtractor("spot")
        self.assertEqual(exchange_class.contract_type, "spot")
        self.assertEqual(exchange_class.vendor, "crypto_chassis")
        self.assertEqual(
            exchange_class._endpoint, "https://api.cryptochassis.com/v1"
        )
        # Wrong contract type.
        with self.assertRaises(AssertionError) as fail:
            imvccdexex.CryptoChassisExtractor("dummy")
        actual = str(fail.exception)
        expected = "Failed assertion *\n'dummy' in '['spot', 'futures']'\n"
        self.assertIn(expected, actual)

    def test_download_bid_ask_spot(
        self,
    ) -> None:
        """
        Verify that `_download_bid_ask` is called properly in `spot` mode.
        """
        #
        start_timestamp = pd.Timestamp("2022-08-18T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-18T23:59:00", tz="UTC")
        exchange_id = "binance"
        currency_pair = "btc/usdt"
        contract_type = "spot"
        # Mock the returns of the functions.
        self.convert_currency_pair_mock.return_value = "btc-usdt"
        self.build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/market-depth/binance/btc-usdt"
        )
        self.build_query_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/market-depth/binance/"
            "btc-usdt?startTime=1660766400&endTime=1660852740"
        )
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {"urls": [{"url": "https://mock-url.com"}]}
        self.requests_mock.get.return_value = response_mock
        self.pandas_read_csv_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price_bid_size": ["23341.25_0.003455"],
                "ask_price_ask_size": ["23344.58_0.052201"],
            }
        )
        self.coerce_to_numeric_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price": [23341.25],
                "bid_size": [0.003455],
                "ask_price": [23344.58],
                "ask_size": [0.052201],
            }
        )
        # Get the data.
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        bidask_data = client._download_bid_ask(
            exchange_id, currency_pair, start_timestamp, end_timestamp
        )
        # Check calls against `convert_currency`.
        self.assertEqual(self.convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(self.convert_currency_pair_mock.call_args)
        expected_args = (("btc/usdt",), {})
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_base_url`.
        self.assertEqual(self.build_base_url_mock.call_count, 1)
        actual_args = tuple(self.build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "market-depth",
                "exchange": "binance",
                "currency_pair": "btc-usdt",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_query_url`.
        self.assertEqual(self.build_query_url_mock.call_count, 1)
        actual_args = tuple(self.build_query_url_mock.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/market-depth/binance/btc-usdt",),
            {"depth": "1", "startTime": "2022-08-18T00:00:00Z"},
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `requests.get`.
        self.assertEqual(self.requests_mock.get.call_count, 1)
        actual_args = tuple(self.requests_mock.get.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/market-depth/binance/btc-usdt?"\
                "startTime=1660766400&endTime=1660852740",), {}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `pandas.read_csv`.
        self.assertEqual(self.pandas_read_csv_mock.call_count, 1)
        actual_args = tuple(self.pandas_read_csv_mock.call_args)
        expected_args = (
            ("https://mock-url.com",), {"compression": "gzip"}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `coerce_to_numeric`.
        self.assertEqual(self.coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(self.coerce_to_numeric_mock.call_args)
        # Reproduce the structure of the arguments.
        exp_arg_df = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price": ["23341.25"],
                "bid_size": ["0.003455"],
                "ask_price": ["23344.58"],
                "ask_size": ["0.052201"],
            }
        )
        expected_args = (
            (exp_arg_df,),
            {"float_columns": ["bid_price", "bid_size", "ask_price", "ask_size"]},
        )
        # Convert Dataframes to string.
        expected_df_str = hpandas.df_to_str(expected_args[0][0])
        actual_df_str = hpandas.df_to_str(actual_args[0][0])
        # Compare Dataframes.
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)
        # Compare `float_columns` argument.
        self.assertEqual(actual_args[1], expected_args[1])
        # Check final `bid-ask` data.
        bidask_expected = pd.DataFrame(
            {
                "timestamp": [1660780800],
                "bid_price": [23341.25],
                "bid_size": [0.003455],
                "ask_price": [23344.58],
                "ask_size": [0.052201],
            }
        )
        expected_df_str = hpandas.df_to_str(bidask_expected)
        actual_df_str = hpandas.df_to_str(bidask_data)
        self.assertEqual(actual_df_str, expected_df_str)
        # Run with invalid exchange name.
        exchange = "bibance"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        response_mock.json = lambda: {
            "message": "Unsupported exchange = bibance."
        }
        self.requests_mock.get.return_value = response_mock
        df = client._download_bid_ask(
            exchange, currency_pair, start_timestamp, end_timestamp
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)
        # Run with invalid currency pair.
        exchange = "binance"
        currency_pair = "btc/busdt"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        response_mock.json = lambda: {"message": "Unsupported pair = btc-busdt."}
        self.requests_mock.get.return_value = response_mock
        df = client._download_bid_ask(
            exchange, currency_pair, start_timestamp, end_timestamp
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_bid_ask_futures(
        self,
    ) -> None:
        """
        Verify that `_download_bid_ask` is called properly in `futures` mode.
        """
        #
        start_timestamp = pd.Timestamp("2022-08-18T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-18T23:59:00", tz="UTC")
        exchange_id = "binance"
        currency_pair = "btc/usd"
        contract_type = "futures"
        # Mock the returns of the functions.
        self.convert_currency_pair_mock.return_value = "btcusd"
        self.build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/market-depth/"
            "binance-coin-futures/btcusd_perp"
        )
        self.build_query_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/market-depth/"
            "binance-coin-futures/btcusd_perp?startTime=1660766400&endTime=1660852740"
        )
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {"urls": [{"url": "https://mock-url.com"}]}
        self.requests_mock.get.return_value = response_mock
        self.pandas_read_csv_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price_bid_size": ["23341.25_0.003455"],
                "ask_price_ask_size": ["23344.58_0.052201"],
            }
        )
        self.coerce_to_numeric_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price": [23341.25],
                "bid_size": [0.003455],
                "ask_price": [23344.58],
                "ask_size": [0.052201],
            }
        )
        #
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        bidask_data = client._download_bid_ask(
            exchange_id, currency_pair, start_timestamp, end_timestamp
        )
        # Check calls against `convert_currency`.
        self.assertEqual(self.convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(self.convert_currency_pair_mock.call_args)
        expected_args = (("btc/usd",), {})
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_base_url`.
        self.assertEqual(self.build_base_url_mock.call_count, 1)
        actual_args = tuple(self.build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "market-depth",
                "exchange": "binance-coin-futures",
                "currency_pair": "btcusd_perp",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_query_url`.
        self.assertEqual(self.build_query_url_mock.call_count, 1)
        actual_args = tuple(self.build_query_url_mock.call_args)
        expected_args = (
            (
                "https://api.cryptochassis.com/v1/market-depth/binance-coin-futures/btcusd_perp",
            ),
            {"depth": "1", "startTime": "2022-08-18T00:00:00Z"},
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `requests.get`.
        self.assertEqual(self.requests_mock.get.call_count, 1)
        actual_args = tuple(self.requests_mock.get.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/market-depth/binance-coin-futures/btcusd_perp?"\
                "startTime=1660766400&endTime=1660852740",), {}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `pandas.read_csv`.
        self.assertEqual(self.pandas_read_csv_mock.call_count, 1)
        actual_args = tuple(self.pandas_read_csv_mock.call_args)
        expected_args = (
            ("https://mock-url.com",), {"compression": "gzip"}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `coerce_to_numeric`.
        self.assertEqual(self.coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(self.coerce_to_numeric_mock.call_args)
        # Reproduce the structure of the arguments.
        exp_arg_df = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price": ["23341.25"],
                "bid_size": ["0.003455"],
                "ask_price": ["23344.58"],
                "ask_size": ["0.052201"],
            }
        )
        expected_args = (
            (exp_arg_df,),
            {"float_columns": ["bid_price", "bid_size", "ask_price", "ask_size"]},
        )
        # Convert Dataframes to string.
        expected_df_str = hpandas.df_to_str(expected_args[0][0])
        actual_df_str = hpandas.df_to_str(actual_args[0][0])
        # Compare Dataframes.
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)
        # Compare `float_columns` argument.
        self.assertEqual(actual_args[1], expected_args[1])
        # Check final `bid-ask` data.
        bidask_expected = pd.DataFrame(
            {
                "timestamp": [1660780800],
                "bid_price": [23341.25],
                "bid_size": [0.003455],
                "ask_price": [23344.58],
                "ask_size": [0.052201],
            }
        )
        expected_df_str = hpandas.df_to_str(bidask_expected)
        actual_df_str = hpandas.df_to_str(bidask_data)
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)

    def test_download_bid_ask_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        exchange = "binance"
        currency_pair = "btc/usdt"
        start_timestamp = "invalid"
        end_timestamp = pd.Timestamp("2022-01-09T23:59:00", tz="UTC")
        contract_type = "spot"
        expected = """
* Failed assertion *
Instance of 'invalid' is '<class 'str'>' instead of '<class 'pandas._libs.tslibs.timestamps.Timestamp'>'
"""
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        with self.assertRaises(AssertionError) as cm:
            client._download_bid_ask(
                exchange, currency_pair, start_timestamp, end_timestamp
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assertIn(expected, actual)

    def test_download_bid_ask_invalid_input2(self) -> None:
        """
        Run with invalid timestamp period.
        """
        exchange = "binance"
        currency_pair = "btc/busd"
        # End is before start -> invalid.
        end_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        start_timestamp = pd.Timestamp("2022-01-09T23:59:00", tz="UTC")
        contract_type = "spot"
        expected = """
* Failed assertion *
2022-01-09 23:59:00+00:00 <= 2022-01-09 00:00:00+00:00
"""
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        with self.assertRaises(AssertionError) as cm:
            client._download_bid_ask(
                exchange, currency_pair, start_timestamp, end_timestamp
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assertIn(expected, actual)

    def test_download_ohlcv_spot(self) -> None:
        """
        Verify that `_download_ohlcv` is called properly in `spot` mode.
        """
        start_timestamp = pd.Timestamp("2022-08-19T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-19T23:59:00", tz="UTC")
        exchange_id = "coinbase"
        currency_pair = "btc/usd"
        contract_type = "spot"
        # Mock the returns of the functions.
        self.convert_currency_pair_mock.return_value = "btc-usd"
        self.build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/ohlc/coinbase/btc-usd"
        )
        self.build_query_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/ohlc/coinbase/"
            "btc-usd?startTime=1660852800&endTime=1660939140"
        )
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {
            "recent": {
                "fields": "time_seconds, open, high, low, close, volume, vwap, number_of_trades, twap",
                "data": [
                    [
                        1660922520,
                        "21347.98",
                        "21350.43",
                        "21333.03",
                        "21340.22",
                        "18.51337353",
                        "21340.4743",
                        572,
                        "21341.1172",
                    ]
                ],
            }
        }
        self.requests_mock.get.return_value = response_mock
        self.coerce_to_numeric_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660922520],
                "open": [21347.98],
                "high": [21350.43],
                "low": [21333.03],
                "close": [21340.22],
                "volume": [18.51337353],
                "vwap": [21340.4743],
                "number_of_trades": [572],
                "twap": [21341.1172],
            }
        )
        #
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        ohlcv_data = client._download_ohlcv(
            exchange_id, currency_pair, start_timestamp, end_timestamp
        )
        # Check calls against `convert_currency`.
        self.assertEqual(self.convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(self.convert_currency_pair_mock.call_args)
        expected_args = (("btc/usd",), {})
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_base_url`.
        self.assertEqual(self.build_base_url_mock.call_count, 1)
        actual_args = tuple(self.build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "ohlc",
                "exchange": "coinbase",
                "currency_pair": "btc-usd",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_query_url`.
        self.assertEqual(self.build_query_url_mock.call_count, 1)
        actual_args = tuple(self.build_query_url_mock.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/ohlc/coinbase/btc-usd",),
            {
                "endTime": 1660953540,
                "includeRealTime": "1",
                "interval": "1m",
                "startTime": 1660867200,
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `requests.get`.
        self.assertEqual(self.requests_mock.get.call_count, 1)
        actual_args = tuple(self.requests_mock.get.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/ohlc/coinbase/btc-usd?"\
                "startTime=1660852800&endTime=1660939140",), {}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `coerce_to_numeric`.
        self.assertEqual(self.coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(self.coerce_to_numeric_mock.call_args)
        # Reproduce the structure of the arguments.
        exp_arg_df = pd.DataFrame(
            {
                "time_seconds": [1660922520],
                "open": ["21347.98"],
                "high": ["21350.43"],
                "low": ["21333.03"],
                "close": ["21340.22"],
                "volume": ["18.51337353"],
                "vwap": ["21340.4743"],
                "number_of_trades": [572],
                "twap": ["21341.1172"],
            }
        )
        expected_args = (
            (exp_arg_df,),
            {"float_columns": ["open", "high", "low", "close", "volume"]},
        )
        # Convert Dataframes to string.
        expected_df_str = hpandas.df_to_str(expected_args[0][0])
        actual_df_str = hpandas.df_to_str(actual_args[0][0])
        # Compare Dataframes.
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)
        # Compare `float_columns` argument.
        self.assertEqual(actual_args[1], expected_args[1])
        # Check final `ohlcv` data.
        ohlcv_expected = pd.DataFrame(
            {
                "timestamp": [1660922520],
                "open": [21347.98],
                "high": [21350.43],
                "low": [21333.03],
                "close": [21340.22],
                "volume": [18.51337353],
                "vwap": [21340.4743],
                "number_of_trades": [572],
                "twap": [21341.1172],
            }
        )
        expected_df_str = hpandas.df_to_str(ohlcv_expected)
        actual_df_str = hpandas.df_to_str(ohlcv_data)
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)
        # Run with invalid exchange name.
        exchange = "bibance"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        response_mock.json = lambda: {
            "message": "Unsupported exchange = bibance."
        }
        self.requests_mock.get.return_value = response_mock
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        actual = client._download_ohlcv(
            exchange,
            currency_pair,
            start_timestamp,
            end_timestamp,
        )
        actual = hpandas.convert_df_to_json_string(actual)
        self.assert_equal(expected, actual, fuzzy_match=True)
        # Run with invalid currency pair.
        exchange = "binance"
        currency_pair = "btc/busdt"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        response_mock.json = lambda: {"message": "Unsupported pair = btc-busdt."}
        self.requests_mock.get.return_value = response_mock
        actual = client._download_ohlcv(
            exchange,
            currency_pair,
            start_timestamp,
            end_timestamp,
        )
        actual = hpandas.convert_df_to_json_string(actual)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_ohlcv_futures(self) -> None:
        """
        Verify that `_download_ohlcv` is called properly in `spot` mode.
        """
        start_timestamp = pd.Timestamp("2022-08-19T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-19T23:59:00", tz="UTC")
        exchange_id = "binance"
        currency_pair = "btc/usd"
        contract_type = "futures"
        # Mock the returns of the functions.
        self.convert_currency_pair_mock.return_value = "btcusd"
        self.build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/ohlc/"
            "binance-coin-futures/btcusd_perp"
        )
        self.build_query_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/ohlc/"
            "binance-coin-futures/btcusd_perp?startTime=1660852800&endTime=1660939140"
        )
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {
            "recent": {
                "fields": "time_seconds, open, high, low, close, volume, vwap, number_of_trades, twap",
                "data": [
                    [
                        1660922520,
                        "21347.98",
                        "21350.43",
                        "21333.03",
                        "21340.22",
                        "18.51337353",
                        "21340.4743",
                        572,
                        "21341.1172",
                    ]
                ],
            }
        }
        self.requests_mock.get.return_value = response_mock
        self.coerce_to_numeric_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660922520],
                "open": [21347.98],
                "high": [21350.43],
                "low": [21333.03],
                "close": [21340.22],
                "volume": [18.51337353],
                "vwap": [21340.4743],
                "number_of_trades": [572],
                "twap": [21341.1172],
            }
        )
        #
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        ohlcv_data = client._download_ohlcv(
            exchange_id, currency_pair, start_timestamp, end_timestamp
        )
        # Check calls against `convert_currency`.
        self.assertEqual(self.convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(self.convert_currency_pair_mock.call_args)
        expected_args = (("btc/usd",), {})
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_base_url`.
        self.assertEqual(self.build_base_url_mock.call_count, 1)
        actual_args = tuple(self.build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "ohlc",
                "exchange": "binance-coin-futures",
                "currency_pair": "btcusd_perp",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_query_url`.
        self.assertEqual(self.build_query_url_mock.call_count, 1)
        actual_args = tuple(self.build_query_url_mock.call_args)
        expected_args = (
            (
                "https://api.cryptochassis.com/v1/ohlc/binance-coin-futures/btcusd_perp",
            ),
            {
                "endTime": 1660953540,
                "includeRealTime": "1",
                "interval": "1m",
                "startTime": 1660867200,
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `requests.get`.
        self.assertEqual(self.requests_mock.get.call_count, 1)
        actual_args = tuple(self.requests_mock.get.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/ohlc/binance-coin-futures/btcusd_perp?"\
                "startTime=1660852800&endTime=1660939140",), {}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `coerce_to_numeric`.
        self.assertEqual(self.coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(self.coerce_to_numeric_mock.call_args)
        # Reproduce the structure of the arguments.
        exp_arg_df = pd.DataFrame(
            {
                "time_seconds": [1660922520],
                "open": ["21347.98"],
                "high": ["21350.43"],
                "low": ["21333.03"],
                "close": ["21340.22"],
                "volume": ["18.51337353"],
                "vwap": ["21340.4743"],
                "number_of_trades": [572],
                "twap": ["21341.1172"],
            }
        )
        expected_args = (
            (exp_arg_df,),
            {"float_columns": ["open", "high", "low", "close", "volume"]},
        )
        # Convert Dataframes to string.
        expected_df_str = hpandas.df_to_str(expected_args[0][0])
        actual_df_str = hpandas.df_to_str(actual_args[0][0])
        # Compare Dataframes.
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)
        # Compare `float_columns` argument.
        self.assertEqual(actual_args[1], expected_args[1])
        # Check final `ohlcv` data.
        ohlcv_expected = pd.DataFrame(
            {
                "timestamp": [1660922520],
                "open": [21347.98],
                "high": [21350.43],
                "low": [21333.03],
                "close": [21340.22],
                "volume": [18.51337353],
                "vwap": [21340.4743],
                "number_of_trades": [572],
                "twap": [21341.1172],
            }
        )
        expected_df_str = hpandas.df_to_str(ohlcv_expected)
        actual_df_str = hpandas.df_to_str(ohlcv_data)
        self.assert_equal(actual_df_str, expected_df_str, fuzzy_match=True)

    def test_download_ohlcv_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        exchange = "binance"
        currency_pair = "btc/usdt"
        start_timestamp = "invalid"
        end_timestamp = "invalid"
        contract_type = "spot"
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = """
* Failed assertion *
Instance of 'invalid' is '<class 'str'>' instead of '<class 'pandas._libs.tslibs.timestamps.Timestamp'>'
"""
        with self.assertRaises(AssertionError) as cm:
            client._download_ohlcv(
                exchange,
                currency_pair,
                start_timestamp,
                end_timestamp,
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assertIn(expected, actual)

    def test_download_trades_spot(self) -> None:
        """
        Verify that `_download_trades` is called properly in `spot` mode.
        """
        #
        start_timestamp = pd.Timestamp("2022-08-18T00:00:00", tz="UTC")
        exchange_id = "coinbase"
        currency_pair = "btc/usd"
        contract_type = "spot"
        # Mock the returns of the functions.
        self.convert_currency_pair_mock.return_value = "btc-usd"
        self.build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/trade/coinbase/btc-usd"
        )
        self.build_query_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/trade/coinbase/"
            "btc-usd?startTime=1660766400"
        )
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {"urls": [{"url": "https://mock-url.com"}]}
        self.requests_mock.get.return_value = response_mock
        self.pandas_read_csv_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "price": [23344.42],
                "size": [0.0022015],
                "is_buyer_maker": [0],
            }
        )
        #
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        trade_data = client._download_trades(
            exchange_id, currency_pair, start_timestamp=start_timestamp
        )
        # Check calls against `convert_currency`.
        self.assertEqual(self.convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(self.convert_currency_pair_mock.call_args)
        expected_args = (("btc/usd",), {})
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_base_url`.
        self.assertEqual(self.build_base_url_mock.call_count, 1)
        actual_args = tuple(self.build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "trade",
                "exchange": "coinbase",
                "currency_pair": "btc-usd",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_query_url`.
        self.assertEqual(self.build_query_url_mock.call_count, 1)
        actual_args = tuple(self.build_query_url_mock.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/trade/coinbase/btc-usd",),
            {"startTime": "2022-08-18T00:00:00Z"},
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `requests.get`.
        self.assertEqual(self.requests_mock.get.call_count, 1)
        actual_args = tuple(self.requests_mock.get.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/trade/coinbase/btc-usd?"\
                "startTime=1660766400",), {}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `pandas.read_csv`.
        self.assertEqual(self.pandas_read_csv_mock.call_count, 1)
        actual_args = tuple(self.pandas_read_csv_mock.call_args)
        expected_args = (
            ("https://mock-url.com",), {"compression": "gzip"}
        )
        self.assertEqual(actual_args, expected_args)
        # Compare `float_columns` argument.
        self.assertEqual(actual_args[1], expected_args[1])
        # Check final `trade` data.
        trade_expected = pd.DataFrame(
            {
                "timestamp": [1660780800],
                "price": [23344.42],
                "size": [0.0022015],
                "is_buyer_maker": [0],
            }
        )
        expected_df_str = hpandas.df_to_str(trade_expected)
        actual_df_str = hpandas.df_to_str(trade_data)
        self.assertEqual(actual_df_str, expected_df_str)
        # Run with invalid exchange name.
        exchange = "bibance"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        response_mock.json = lambda: {
            "message": "Unsupported exchange = bibance."
        }
        self.requests_mock.get.return_value = response_mock
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        actual = client._download_trades(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        actual = hpandas.convert_df_to_json_string(actual)
        self.assert_equal(expected, actual, fuzzy_match=True)
        # Run with invalid currency pair.
        exchange = "binance"
        currency_pair = "btc/busdt"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        response_mock.json = lambda: {"message": "Unsupported pair = btc-busdt."}
        self.requests_mock.get.return_value = response_mock
        actual = client._download_trades(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        actual = hpandas.convert_df_to_json_string(actual)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_trades_futures(self) -> None:
        """
        Verify that `_download_trades` is called properly in `futures` mode.
        """
        #
        start_timestamp = pd.Timestamp("2022-08-18T00:00:00", tz="UTC")
        exchange_id = "binance"
        currency_pair = "btc/usdt"
        contract_type = "futures"
        # Mock the returns of the functions.
        self.convert_currency_pair_mock.return_value = "btcusdt"
        self.build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/trade/binance-usds-futures/btcusdt"
        )
        self.build_query_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/trade/"
            "binance-usds-futures/btcusdt?startTime=1660766400"
        )
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {"urls": [{"url": "https://mock-url.com"}]}
        self.requests_mock.get.return_value = response_mock
        self.pandas_read_csv_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "price": [23344.42],
                "size": [0.0022015],
                "is_buyer_maker": [0],
            }
        )
        #
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        trade_data = client._download_trades(
            exchange_id, currency_pair, start_timestamp=start_timestamp
        )
        # Check calls against `convert_currency`.
        self.assertEqual(self.convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(self.convert_currency_pair_mock.call_args)
        expected_args = (("btc/usdt",), {})
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_base_url`.
        self.assertEqual(self.build_base_url_mock.call_count, 1)
        actual_args = tuple(self.build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "trade",
                "exchange": "binance-usds-futures",
                "currency_pair": "btcusdt",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `build_query_url`.
        self.assertEqual(self.build_query_url_mock.call_count, 1)
        actual_args = tuple(self.build_query_url_mock.call_args)
        expected_args = (
            (
                "https://api.cryptochassis.com/v1/trade/binance-usds-futures/btcusdt",
            ),
            {"startTime": "2022-08-18T00:00:00Z"},
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `requests.get`.
        self.assertEqual(self.requests_mock.get.call_count, 1)
        actual_args = tuple(self.requests_mock.get.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/trade/binance-usds-futures/btcusdt?"\
                "startTime=1660766400",), {}
        )
        self.assertEqual(actual_args, expected_args)
        # Check calls against `pandas.read_csv`.
        self.assertEqual(self.pandas_read_csv_mock.call_count, 1)
        actual_args = tuple(self.pandas_read_csv_mock.call_args)
        expected_args = (
            ("https://mock-url.com",), {"compression": "gzip"}
        )
        self.assertEqual(actual_args, expected_args)
        # Compare `float_columns` argument.
        self.assertEqual(actual_args[1], expected_args[1])
        # Check final `trade` data.
        trade_expected = pd.DataFrame(
            {
                "timestamp": [1660780800],
                "price": [23344.42],
                "size": [0.0022015],
                "is_buyer_maker": [0],
            }
        )
        expected_df_str = hpandas.df_to_str(trade_expected)
        actual_df_str = hpandas.df_to_str(trade_data)
        self.assertEqual(actual_df_str, expected_df_str)

    def test_download_trade_invalid_input1(self) -> None:
        """
        Run with invalid start timestamp.
        """
        exchange = "binance"
        currency_pair = "btc/usdt"
        start_timestamp = "invalid"
        contract_type = "spot"
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = """
* Failed assertion *
Instance of 'invalid' is '<class 'str'>' instead of '<class 'pandas._libs.tslibs.timestamps.Timestamp'>'
"""
        with self.assertRaises(AssertionError) as cm:
            client._download_trades(
                exchange, currency_pair, start_timestamp=start_timestamp
            )
        # Check output for error.
        actual = str(cm.exception)
        self.assertIn(expected, actual)


class TestCryptoChassisExtractor2(hunitest.TestCase):
    def test_coerce_to_numeric(self) -> None:
        """
        Test if the specified columns are converted to numeric values..
        """
        contract_type = "spot"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        test_df = pd.DataFrame(
            {
                "time_seconds": [1660922520],
                "num": ["21347.98"],
                "num2": ["346"],
                "non_num": ["21350"],
            }
        )
        # Define float `fields`.
        num_fields = ["num", "num2"]
        expected_df = pd.DataFrame(
            {
                "time_seconds": [1660922520],
                "num": [21347.98],
                "num2": [346.0],
                "non_num": [21350],
            }
        )
        actual_df = client.coerce_to_numeric(test_df, num_fields)
        hunitest.compare_df(actual_df, expected_df)

    def test_convert_pair_spot1(self) -> None:
        """
        Test if currency pair is converted according to the contract type.
        """
        contract_type = "spot"
        pair = "btc/usd"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = "btc-usd"
        actual = client.convert_currency_pair(pair)
        self.assertEqual(actual, expected)

    def test_convert_pair_spot2(self) -> None:
        """
        Test if currency pair is converted according to the contract type.
        """
        contract_type = "spot"
        pair = "btc_usd"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = "btc-usd"
        actual = client.convert_currency_pair(pair)
        self.assertEqual(actual, expected)

    def test_convert_pair_futures1(self) -> None:
        """
        Test if currency pair is converted according to the contract type.
        """
        contract_type = "futures"
        pair = "btc/usd"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = "btcusd"
        actual = client.convert_currency_pair(pair)
        self.assertEqual(actual, expected)

    def test_convert_pair_futures1(self) -> None:
        """
        Test if currency pair is converted according to the contract type.
        """
        contract_type = "futures"
        pair = "btc_usd"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = "btcusd"
        actual = client.convert_currency_pair(pair)
        self.assertEqual(actual, expected)

    def test_build_query_url(self) -> None:
        """
        Test if the query URL is built correctly.
        """
        contract_type = "spot"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        base_url = "https://api.cryptochassis.com/v1/trade/coinbase/btc-usd"
        actual = client._build_query_url(
            base_url, startTime=1660852800, endTime=1660939140, emptyArg=None
        )
        expected = (
            "https://api.cryptochassis.com/v1/trade/coinbase/btc-usd?"
            "startTime=1660852800&endTime=1660939140"
        )
        # Check that all valid parameters are used.
        self.assert_equal(actual, expected)

    def test_build_base_url(self) -> None:
        """
        Test if the base URL is built correctly.
        """
        contract_type = "spot"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        expected = "https://api.cryptochassis.com/v1/trade/coinbase/btc-usd"
        actual = client._build_base_url("trade", "coinbase", "btc-usd")
        self.assert_equal(actual, expected)
