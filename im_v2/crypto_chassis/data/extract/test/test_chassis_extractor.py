import logging
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.crypto_chassis.data.extract.extractor as imvccdexex

_LOG = logging.getLogger(__name__)


class TestCryptoChassisExtractor1(hunitest.TestCase):
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

    @umock.patch.object(
        imvccdexex.pd,
        "read_csv",
        spec=imvccdexex.pd.read_csv,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "coerce_to_numeric",
        spec=imvccdexex.CryptoChassisExtractor.coerce_to_numeric,
    )
    @umock.patch.object(imvccdexex, "requests", spec=imvccdexex.requests)
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_query_url",
        spec=imvccdexex.CryptoChassisExtractor._build_query_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_base_url",
        spec=imvccdexex.CryptoChassisExtractor._build_base_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "convert_currency_pair",
        spec=imvccdexex.CryptoChassisExtractor.convert_currency_pair,
    )
    def test_download_bid_ask_spot(
        self,
        convert_currency_pair_mock: umock.MagicMock,
        build_base_url_mock: umock.MagicMock,
        build_query_url_mock: umock.MagicMock,
        requests_mock: umock.MagicMock,
        coerce_to_numeric_mock: umock.MagicMock,
        pandas_read_csv_mock: umock.MagicMock,
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
        convert_currency_pair_mock.return_value = "btc-usd"
        build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/market-depth/binance/btc-usd"
        )
        build_query_url_mock.return_value = "https://api.cryptochassis.com/v1/market-depth/binance/btc-usd?startTime=1660766400&endTime=1660852740"
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {"urls": [{"url": "https://mock-url.com"}]}
        requests_mock.get.return_value = response_mock
        pandas_read_csv_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price_bid_size": ["23341.25_0.003455"],
                "ask_price_ask_size": ["23344.58_0.052201"],
            }
        )
        coerce_to_numeric_mock.return_value = pd.DataFrame(
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
        # Test `convert_currency`.
        self.assertEqual(convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(convert_currency_pair_mock.call_args)
        expected_args = (("btc/usdt",), {})
        self.assertEqual(actual_args, expected_args)
        # Test `build_base_url`.
        self.assertEqual(build_base_url_mock.call_count, 1)
        actual_args = tuple(build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "market-depth",
                "exchange": "binance",
                "currency_pair": "btc-usd",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Test `build_query_url`.
        self.assertEqual(build_query_url_mock.call_count, 1)
        actual_args = tuple(build_query_url_mock.call_args)
        expected_args = (
            ("https://api.cryptochassis.com/v1/market-depth/binance/btc-usd",),
            {"depth": "1", "startTime": "2022-08-18T00:00:00Z"},
        )
        self.assertEqual(actual_args, expected_args)
        # Test `coerce_to_numeric`.
        self.assertEqual(coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(coerce_to_numeric_mock.call_args)
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

    @umock.patch.object(
        imvccdexex.pd,
        "read_csv",
        spec=imvccdexex.pd.read_csv,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "coerce_to_numeric",
        spec=imvccdexex.CryptoChassisExtractor.coerce_to_numeric,
    )
    @umock.patch.object(imvccdexex, "requests", spec=imvccdexex.requests)
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_query_url",
        spec=imvccdexex.CryptoChassisExtractor._build_query_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_base_url",
        spec=imvccdexex.CryptoChassisExtractor._build_base_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "convert_currency_pair",
        spec=imvccdexex.CryptoChassisExtractor.convert_currency_pair,
    )
    def test_download_bid_ask_futures(
        self,
        convert_currency_pair_mock: umock.MagicMock,
        build_base_url_mock: umock.MagicMock,
        build_query_url_mock: umock.MagicMock,
        requests_mock: umock.MagicMock,
        coerce_to_numeric_mock: umock.MagicMock,
        pandas_read_csv_mock: umock.MagicMock,
    ) -> None:
        """
        Verify that `_download_bid_ask` is called properly in `futures` mode.
        """
        #
        start_timestamp = pd.Timestamp("2022-08-18T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-18T23:59:00", tz="UTC")
        exchange_id = "binance"
        currency_pair = "btc/usdt"
        contract_type = "futures"
        # Mock the returns of the functions.
        convert_currency_pair_mock.return_value = "btcusd"
        build_base_url_mock.return_value = "https://api.cryptochassis.com/v1/market-depth/binance-coin-futures/btcusd_perp"
        build_query_url_mock.return_value = "https://api.cryptochassis.com/v1/market-depth/binance-coin-futures/btcusd_perp?startTime=1660766400&endTime=1660852740"
        response_mock = umock.MagicMock()
        response_mock.json = lambda: {"urls": [{"url": "https://mock-url.com"}]}
        requests_mock.get.return_value = response_mock
        pandas_read_csv_mock.return_value = pd.DataFrame(
            {
                "time_seconds": [1660780800],
                "bid_price_bid_size": ["23341.25_0.003455"],
                "ask_price_ask_size": ["23344.58_0.052201"],
            }
        )
        coerce_to_numeric_mock.return_value = pd.DataFrame(
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
        # Test `convert_currency`.
        self.assertEqual(convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(convert_currency_pair_mock.call_args)
        expected_args = (("btc/usdt",), {})
        self.assertEqual(actual_args, expected_args)
        # Test `build_base_url`.
        self.assertEqual(build_base_url_mock.call_count, 1)
        actual_args = tuple(build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "market-depth",
                "exchange": "binance-coin-futures",
                "currency_pair": "btcusd_perp",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Test `build_query_url`.
        self.assertEqual(build_query_url_mock.call_count, 1)
        actual_args = tuple(build_query_url_mock.call_args)
        expected_args = (
            (
                "https://api.cryptochassis.com/v1/market-depth/binance-coin-futures/btcusd_perp",
            ),
            {"depth": "1", "startTime": "2022-08-18T00:00:00Z"},
        )
        self.assertEqual(actual_args, expected_args)
        # Test `coerce_to_numeric`.
        self.assertEqual(coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(coerce_to_numeric_mock.call_args)
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
        Run with invalid exchange name.
        """
        exchange = "bibance"
        currency_pair = "btc/usdt"
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-01-09T23:59:00", tz="UTC")
        contract_type = "spot"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        df = client._download_bid_ask(
            exchange, currency_pair, start_timestamp, end_timestamp
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_bid_ask_invalid_input3(self) -> None:
        """
        Run with invalid currency pair.
        """
        exchange = "binance"
        currency_pair = "btc/busdt"
        # End is before start -> invalid.
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-01-09T23:59:00", tz="UTC")
        contract_type = "spot"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        df = client._download_bid_ask(
            exchange, currency_pair, start_timestamp, end_timestamp
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)

    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "coerce_to_numeric",
        spec=imvccdexex.CryptoChassisExtractor.coerce_to_numeric,
    )
    @umock.patch.object(imvccdexex, "requests", spec=imvccdexex.requests)
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_query_url",
        spec=imvccdexex.CryptoChassisExtractor._build_query_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_base_url",
        spec=imvccdexex.CryptoChassisExtractor._build_base_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "convert_currency_pair",
        spec=imvccdexex.CryptoChassisExtractor.convert_currency_pair,
    )
    def test_download_ohlcv_spot(
        self,
        convert_currency_pair_mock: umock.MagicMock,
        build_base_url_mock: umock.MagicMock,
        build_query_url_mock: umock.MagicMock,
        requests_mock: umock.MagicMock,
        coerce_to_numeric_mock: umock.MagicMock,
    ) -> None:
        """
        Verify that `_download_ohlcv` is called properly in `spot` mode.
        """
        start_timestamp = pd.Timestamp("2022-08-19T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-19T23:59:00", tz="UTC")
        exchange_id = "coinbase"
        currency_pair = "btc/usdt"
        contract_type = "spot"
        # Mock the returns of the functions.
        convert_currency_pair_mock.return_value = "btc-usd"
        build_base_url_mock.return_value = (
            "https://api.cryptochassis.com/v1/ohlc/coinbase/btc-usd"
        )
        build_query_url_mock.return_value = "https://api.cryptochassis.com/v1/ohlc/coinbase/btc-usd?startTime=1660852800&endTime=1660939140"
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
        requests_mock.get.return_value = response_mock
        coerce_to_numeric_mock.return_value = pd.DataFrame(
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
        # Test `convert_currency`.
        self.assertEqual(convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(convert_currency_pair_mock.call_args)
        expected_args = (("btc/usdt",), {})
        self.assertEqual(actual_args, expected_args)
        # Test `build_base_url`.
        self.assertEqual(build_base_url_mock.call_count, 1)
        actual_args = tuple(build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "ohlc",
                "exchange": "coinbase",
                "currency_pair": "btc-usd",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Test `build_query_url`.
        self.assertEqual(build_query_url_mock.call_count, 1)
        actual_args = tuple(build_query_url_mock.call_args)
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
        # Test `coerce_to_numeric`.
        self.assertEqual(coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(coerce_to_numeric_mock.call_args)
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

    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "coerce_to_numeric",
        spec=imvccdexex.CryptoChassisExtractor.coerce_to_numeric,
    )
    @umock.patch.object(imvccdexex, "requests", spec=imvccdexex.requests)
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_query_url",
        spec=imvccdexex.CryptoChassisExtractor._build_query_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_build_base_url",
        spec=imvccdexex.CryptoChassisExtractor._build_base_url,
    )
    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "convert_currency_pair",
        spec=imvccdexex.CryptoChassisExtractor.convert_currency_pair,
    )
    def test_download_ohlcv_futures(
        self,
        convert_currency_pair_mock: umock.MagicMock,
        build_base_url_mock: umock.MagicMock,
        build_query_url_mock: umock.MagicMock,
        requests_mock: umock.MagicMock,
        coerce_to_numeric_mock: umock.MagicMock,
    ) -> None:
        """
        Verify that `_download_ohlcv` is called properly in `spot` mode.
        """
        start_timestamp = pd.Timestamp("2022-08-19T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-08-19T23:59:00", tz="UTC")
        exchange_id = "binance"
        currency_pair = "btc/usdt"
        contract_type = "futures"
        # Mock the returns of the functions.
        convert_currency_pair_mock.return_value = "btcusd"
        build_base_url_mock.return_value = "https://api.cryptochassis.com/v1/ohlc/binance-coin-futures/btcusd_perp"
        build_query_url_mock.return_value = "https://api.cryptochassis.com/v1/ohlc/binance-coin-futures/btcusd_perp?startTime=1660852800&endTime=1660939140"
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
        requests_mock.get.return_value = response_mock
        coerce_to_numeric_mock.return_value = pd.DataFrame(
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
        # Test `convert_currency`.
        self.assertEqual(convert_currency_pair_mock.call_count, 1)
        actual_args = tuple(convert_currency_pair_mock.call_args)
        expected_args = (("btc/usdt",), {})
        self.assertEqual(actual_args, expected_args)
        # Test `build_base_url`.
        self.assertEqual(build_base_url_mock.call_count, 1)
        actual_args = tuple(build_base_url_mock.call_args)
        expected_args = (
            (),
            {
                "data_type": "ohlc",
                "exchange": "binance-coin-futures",
                "currency_pair": "btcusd_perp",
            },
        )
        self.assertEqual(actual_args, expected_args)
        # Test `build_query_url`.
        self.assertEqual(build_query_url_mock.call_count, 1)
        actual_args = tuple(build_query_url_mock.call_args)
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
        # Test `coerce_to_numeric`.
        self.assertEqual(coerce_to_numeric_mock.call_count, 1)
        actual_args = tuple(coerce_to_numeric_mock.call_args)
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

    @pytest.mark.skip(reason="CmTask1997 'Too many request errors'.")
    def test_download_ohlcv_invalid_input1(self) -> None:
        """
        Run with invalid exchange name.
        """
        exchange = "bibance"
        currency_pair = "btc/usdt"
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-03-09T00:00:00", tz="UTC")
        contract_type = "spot"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        df = client._download_ohlcv(
            exchange,
            currency_pair,
            start_timestamp,
            end_timestamp,
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)

    @pytest.mark.skip(reason="CmTask1997 'Too many request errors'.")
    def test_download_ohlcv_invalid_input2(self) -> None:
        """
        Run with invalid currency pair.
        """
        exchange = "binance"
        currency_pair = "btc/busdt"
        # End is before start -> invalid.
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        end_timestamp = pd.Timestamp("2022-03-09T00:00:00", tz="UTC")
        contract_type = "spot"
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        df = client._download_ohlcv(
            exchange,
            currency_pair,
            start_timestamp,
            end_timestamp,
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_ohlcv_invalid_input3(self) -> None:
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

    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_download_trades",
        spec=imvccdexex.CryptoChassisExtractor._download_trades,
    )
    def test_download_trades_spot(
        self,
        download_trades_mock: umock.MagicMock,
    ) -> None:
        """
        Verify that `_download_trade` is called properly in `spot` mode.
        """
        download_trades_mock.return_value = pd.DataFrame(
            ["dummy"], columns=["dummy"]
        )
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        exchange = "coinbase"
        currency_pair = "btc/usdt"
        contract_type = "spot"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        trades_data = client._download_trades(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        self.assertEqual(download_trades_mock.call_count, 1)
        actual_args = tuple(download_trades_mock.call_args)
        expected_args = (
            (("coinbase"), ("btc/usdt")),
            {
                "start_timestamp": pd.Timestamp(
                    "2022-01-09 00:00:00+0000", tz="UTC"
                )
            },
        )
        self.assertEqual(actual_args, expected_args)
        actual_output = hpandas.df_to_str(trades_data)
        expected_output = r"""dummy
            0  dummy
        """
        self.assert_equal(actual_output, expected_output, fuzzy_match=True)

    @umock.patch.object(
        imvccdexex.CryptoChassisExtractor,
        "_download_trades",
        spec=imvccdexex.CryptoChassisExtractor._download_trades,
    )
    def test_download_trade_futures(
        self, download_trades_mock: umock.MagicMock
    ) -> None:
        """
        Verify that `_download_trade` is called properly in `futures` mode.
        """
        download_trades_mock.return_value = pd.DataFrame(
            ["dummy"], columns=["dummy"]
        )
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        exchange = "coinbase"
        currency_pair = "btc/usdt"
        contract_type = "futures"
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        trades_data = client._download_trades(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        self.assertEqual(download_trades_mock.call_count, 1)
        actual_args = tuple(download_trades_mock.call_args)
        expected_args = (
            (("coinbase"), ("btc/usdt")),
            {
                "start_timestamp": pd.Timestamp(
                    "2022-01-09 00:00:00+0000", tz="UTC"
                )
            },
        )
        self.assertEqual(actual_args, expected_args)
        actual_output = hpandas.df_to_str(trades_data)
        expected_output = r"""dummy
            0  dummy
        """
        self.assert_equal(actual_output, expected_output, fuzzy_match=True)

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

    def test_download_trade_invalid_input2(self) -> None:
        """
        Run with invalid exchange name.
        """
        exchange = "bibance"
        currency_pair = "btc/usdt"
        contract_type = "spot"
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        df = client._download_trades(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)

    def test_download_trade_invalid_input3(self) -> None:
        """
        Run with invalid currency pair.
        """
        exchange = "binance"
        currency_pair = "btc/busdt"
        contract_type = "spot"
        # End is before start -> invalid.
        start_timestamp = pd.Timestamp("2022-01-09T00:00:00", tz="UTC")
        # Empty Dataframe is expected.
        expected = hpandas.convert_df_to_json_string(pd.DataFrame())
        client = imvccdexex.CryptoChassisExtractor(contract_type)
        df = client._download_trades(
            exchange, currency_pair, start_timestamp=start_timestamp
        )
        actual = hpandas.convert_df_to_json_string(df)
        self.assert_equal(expected, actual, fuzzy_match=True)
