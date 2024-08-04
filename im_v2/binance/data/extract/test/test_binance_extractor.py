import time
import unittest.mock as umock
from collections import namedtuple
from typing import Any, List

import pandas as pd

import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.binance.data.extract.extractor as imvbdexex
import im_v2.binance.websocket.binance_socket_manager as imvbwbsoma


class TestBinanceExtractor1(hunitest.TestCase):
    def test_fetch_trades(self) -> None:
        """
        Tests the fetch trades method.
        """
        # Mock downloading files and extracting data from them.
        imvbdexex.hio = umock.MagicMock()
        contract_type = "futures"
        data_type = "trades"
        try:
            binance_extractor = imvbdexex.BinanceExtractor(
                contract_type, imvbdexex.BinanceNativeTimePeriod.DAILY, data_type
            )
            binance_extractor._download_binance_files = umock.AsyncMock()
            binance_extractor._extract_data_from_binance_files = umock.MagicMock(
                return_value=self._get_mock_trades()
            )
            # Prepare parameters.
            currency_pair = "BTCUSDT"
            start_date = pd.Timestamp("2020-01-01 00:00:00")
            end_date = pd.Timestamp("2020-01-03 23:59:00")
            # Run.
            actual_df = binance_extractor._fetch_trades(
                currency_pair, start_date, end_date
            )
        except Exception as e:
            raise e
        finally:
            binance_extractor.close()
        # Compare results.
        actual = hpandas.df_to_str(
            actual_df.drop(columns=["end_download_timestamp"])
        )
        expected = """timestamp  price  amount  side
            0  1577836800000    100       1   buy
            1  1577923200000    200       2  sell
            2  1578009600000    300       3   buy"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def _get_mock_trades(self) -> pd.DataFrame:
        """
        Returns a mock trades dataframe.
        """
        return pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
                "time": [
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-01 00:00:00")
                    ),
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-02 00:00:00")
                    ),
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-03 00:00:00")
                    ),
                    hdateti.convert_timestamp_to_unix_epoch(
                        pd.Timestamp("2020-01-04 10:00:00")
                    ),
                ],
                "price": [100, 200, 300, 400],
                "qty": [1, 2, 3, 4],
                "is_buyer_maker": [True, False, True, False],
                "quote_qty": [100, 400, 900, 300],
                "id": [1, 2, 3, 4],
            }
        )


class TestBinanceExtractor2(hunitest.TestCase):
    """
    Test subscription and reconnection on error.
    """

    def test1(self) -> None:
        """
        Test subscription and WebSocketConnectionClosedException.

        The expected behavior is as follows:
            1. We subscribe to the webSocket with the specified currency pairs and begin receiving data.
            2. WebSocket exception is thrown during data reception, we catch it internally.
            3. Upon catching the exception, we proceed to resubscribe to the same currency pairs.
            4. Once resubscribed, we resume receiving data from the WebSocket.
        """
        # Create dummy data.
        bid_ask = namedtuple("bid_ask", ["data"])
        dummy_data = bid_ask(data="dummy")
        side_effect = [
            ["", dummy_data],
            imvbwbsoma.WebSocketConnectionClosedException,
            ["", dummy_data],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
        ]
        exp = r"""['dummy', 'dummy']"""
        self._test_binance_extractor(side_effect, exp)

    def test2(self) -> None:
        """
        Test max attempts for retry subscription.

        The expected behavior is as follows:
            1. Initially attempts to subscribe to the websocket may fail.
            2. Subscription is retried based on the specified maximum attempts count.
            3. Upon successful subscription, data reception starts.
        """
        # Create dummy data.
        bid_ask = namedtuple("bid_ask", ["data"])
        dummy_data = bid_ask(data="dummy")
        max_attempts = 2
        side_effect = [
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
            ["", dummy_data],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
        ]
        exp = r"""['dummy']"""
        self._test_binance_extractor(side_effect, exp, max_attempts=max_attempts)

    def test3(self) -> None:
        """
        Test max attempts for retry subscription.

        The expected behavior is as follows:
            1. Initially attempts to subscribe to the websocket may fail.
            2. Subscription is retried based on the specified maximum attempts count.
            3. Max attempts are exhausted, data reception fails.
        """
        # Create dummy data.
        bid_ask = namedtuple("bid_ask", ["data"])
        dummy_data = bid_ask(data="dummy")
        max_attempts = 2
        side_effect = [
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
            ["", dummy_data],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
        ]
        exp = r"""[]"""
        self._test_binance_extractor(side_effect, exp, max_attempts=max_attempts)

    def test4(self) -> None:
        """
        Test max attempts for retry subscription.

        The expected behavior is as follows:
            1. Initial subscription attempts are successful.
            2. An unexpected close connection signal is received.
            3. Subscription is not retried as max attempts only apply to initial subscription.
        """
        # Create dummy data.
        bid_ask = namedtuple("bid_ask", ["data"])
        dummy_data = bid_ask(data="dummy")
        max_attempts = 2
        side_effect = [
            ["", dummy_data],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
            ["", dummy_data],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
        ]
        exp = r"""['dummy']"""
        self._test_binance_extractor(side_effect, exp, max_attempts=max_attempts)

    def _test_binance_extractor(
        self,
        side_effect: List[Any],
        expected: str,
        *,
        max_attempts: int = 0,
    ) -> None:
        """
        Init `BinanceExtractor` and subscribe.
        """
        self.actual = []
        try:
            with umock.patch.object(
                imvbdexex.BinanceExtractor,
                "_handle_orderbook_message",
                new=self._mock_handle_orderbook_message,
            ), umock.patch.object(
                imvbwbsoma.random,
                "randint",
                # Mock waiting time for resubscription.
                new=self._mock_randint,
            ), umock.patch.object(
                imvbwbsoma, "create_connection"
            ) as mock_ws_connection:
                # Mock WebSocketConnectionClosed error.
                mock_ws_connection_instance = mock_ws_connection.return_value
                mock_ws_connection_instance.recv_data_frame = umock.MagicMock(
                    side_effect=side_effect
                )
                # Init.
                contract_type = "futures"
                data_type = "bid_ask"
                exchange_id = "binance"
                currency_pairs = ["ETH_USDT"]
                bid_ask_depth = 5
                binance_extractor = imvbdexex.BinanceExtractor(
                    contract_type,
                    imvbdexex.BinanceNativeTimePeriod.DAILY,
                    data_type,
                    max_attempts=max_attempts,
                )
                # Subscribe.
                with hasynci.solipsism_context() as event_loop:
                    coroutine = binance_extractor._subscribe_to_websocket_bid_ask_multiple_symbols(
                        exchange_id,
                        currency_pairs,
                        bid_ask_depth=bid_ask_depth,
                    )
                    hasynci.run(coroutine, event_loop=event_loop)
                time.sleep(1)
                # Assert.
                self.assert_equal(str(self.actual), expected)
        except Exception as e:
            raise e
        finally:
            binance_extractor.close()

    def _mock_handle_orderbook_message(self, _, message: str):
        """
        Mock `_handle_orderbook_message`.
        """
        self.actual.append(message)

    def _mock_randint(self, start: int, end: int):
        """
        Mock `random.randint()`
        """
        return 0


class TestBinanceExtractor3(hunitest.TestCase):
    data = namedtuple("data", ["data"])

    def get_mock_test_data(self, mock_data: str) -> List[List]:
        """
        Provide mock test data for testing the functionality of the
        BinanceExtractor class.
        """
        mock_tuple = self.data(data=mock_data)
        side_effect = [
            ["", mock_tuple],
            [imvbwbsoma.ABNF.OPCODE_CLOSE, {}],
        ]
        return side_effect

    def test1(self) -> None:
        """
        Check that the Binance Extractor correctly downloads bid_ask data using
        default parameters.
        """
        # Prepare mock data.
        mock_orderbook_results = """{
            "s" : "BTCUSDT",
            "T" : "2024-05-04 00:00:00",
            "b" : [200,201],
            "a" : [150,151]
        }"""
        ws_data_type = "bid_ask"
        mock_test_data = self.get_mock_test_data(mock_orderbook_results)
        expected = r"""
        {'timestamp': '2024-05-04 00:00:00', 'bids': [200, 201], 'asks': [150, 151], 'symbol': 'BTC_USDT', 'end_download_timestamp': '2024-05-09 12:00:00+0000'}
        """
        # Test method.
        self._test_binance_extractor_with_mock_data(
            ws_data_type, mock_test_data, expected
        )

    def test2(self) -> None:
        """
        Check that the Binance Extractor correctly downloads bid_ask data for
        an empty imput.
        """
        # Prepare mock data.
        mock_orderbook_results = '{"invalid_key" : "invalid_value"}'
        mock_test_data = self.get_mock_test_data(mock_orderbook_results)
        ws_data_type = "bid_ask"
        expected = r"""
        {'timestamp': None, 'bids': [], 'asks': [], 'end_download_timestamp': '2024-05-09 12:00:00+0000', 'symbol': 'BTC_USDT'}
        """
        # Test method.
        self._test_binance_extractor_with_mock_data(
            ws_data_type, mock_test_data, expected
        )

    def test3(self) -> None:
        """
        Check that the Binance Extractor correctly downloads trades data using
        default parameters.
        """
        # Prepare mock data.
        mock_trades_data = """{
            "s" : "BTCUSDT",
            "t" : 1234,
            "m" : false,
            "X" : "MARKET",
            "T" : "2024-05-06 00:00:00",
            "p" : 200,
            "q" : 15
        }"""
        mock_test_data = self.get_mock_test_data(mock_trades_data)
        ws_data_type = "trades"
        expected = r"""
        {'currency_pair': 'BTC_USDT', 'data': [{'id': 1234, 'timestamp': '2024-05-06 00:00:00', 'side': 'sell', 'price': 200, 'amount': 15}], 'end_download_timestamp': '2024-05-09 12:00:00+0000', 'exchange_id': 'binance'}
        """
        # Test method.
        self._test_binance_extractor_with_mock_data(
            ws_data_type, mock_test_data, expected
        )

    def test4(self) -> None:
        """
        Check that the Binance Extractor correctly downloads trades data for an
        empty input.
        """
        # Prepare mock data.
        mock_trades_data = '{"invalid_key" : "invalid_value"}'
        mock_test_data = self.get_mock_test_data(mock_trades_data)
        ws_data_type = "trades"
        expected = "None"
        # Test method.
        self._test_binance_extractor_with_mock_data(
            ws_data_type, mock_test_data, expected
        )

    def test5(self) -> None:
        """
        Check that the Binance Extractor correctly downloads ohlcv data using
        default parameters.
        """
        # Prepare mock data.
        mock_ohlcv_data = """{
            "s" : "BTCUSDT",
            "k": {
                "t": 1648296000,
                "o": 45678.90,
                "h": 45987.00,
                "l": 45500.50,
                "c": 45800.00,
                "v": 1000
            }
        }"""
        mock_test_data = self.get_mock_test_data(mock_ohlcv_data)
        ws_data_type = "ohlcv"
        expected = r"""
        {'currency_pair': 'BTC_USDT', 'ohlcv': [[1648296000, 45678.9, 45987.0, 45500.5, 45800.0, 1000]], 'end_download_timestamp': '2024-05-09 12:00:00+0000'}
        """
        # Test method.
        self._test_binance_extractor_with_mock_data(
            ws_data_type, mock_test_data, expected
        )

    def test6(self) -> None:
        """
        Check that an empty input is processed correctly.
        """
        # Prepare mock data.
        mock_ohlcv_data = '{"invalid_key" : "invalid_value"}'
        mock_test_data = self.get_mock_test_data(mock_ohlcv_data)
        ws_data_type = "ohlcv"
        expected = "None"
        # Test method.
        self._test_binance_extractor_with_mock_data(
            ws_data_type, mock_test_data, expected
        )

    def _get_subscription_coroutine(
        self,
        binance_extractor,
        ws_data_type: str,
        *,
        valid_data_types=["bid_ask", "trades", "ohlcv"],
        exchange_id: str = "binance",
        currency_pair: List[Any] = ["BTC_USDT"],
        bid_ask_depth: int = 5,
    ) -> Any:
        """
        Get the coroutine for subscribing to WebSocket data.
        """
        hdbg.dassert_in(ws_data_type, valid_data_types)
        func_signature = (
            f"_subscribe_to_websocket_{ws_data_type}_multiple_symbols"
        )
        if ws_data_type == "bid_ask":
            coroutine = getattr(binance_extractor, func_signature)(
                exchange_id, currency_pair, bid_ask_depth
            )
        else:
            coroutine = getattr(binance_extractor, func_signature)(
                exchange_id, currency_pair
            )
        return coroutine

    def _test_binance_extractor_with_mock_data(
        self,
        ws_data_type: str,
        mock_test_data: List[Any],
        expected: str,
        *,
        contract_type: str = "futures",
        start_date: str = "2024-05-02",
        end_date: str = "2024-05-03",
        exchange_id: str = "binance",
        currency_pair: List[Any] = ["BTC_USDT"],
    ) -> None:
        """
        Test BinanceExtractor class functionality using mock WebSocket data..
        """
        try:
            with umock.patch.object(
                hdateti,
                "get_current_time",
                # Mock current time stamp.
                return_value="2024-05-09 12:00:00+0000",
            ), umock.patch.object(
                imvbwbsoma, "create_connection"
            ) as mock_ws_connection:
                # Mock WebSocketConnection.
                mock_ws_connection_instance = mock_ws_connection.return_value
                # Passing data.
                empty_data = self.data(data="{}")
                # Mock the output from the binance to be None until we have subscribed.
                mock_ws_connection_instance.recv_data_frame = umock.MagicMock(
                    return_value=["", empty_data]
                )
                # Initializing BinanceExtractor object.
                binance_extractor = imvbdexex.BinanceExtractor(
                    contract_type,
                    imvbdexex.BinanceNativeTimePeriod.DAILY,
                    ws_data_type,
                )
                # Subscribe.
                with hasynci.solipsism_context() as event_loop:
                    coroutine = self._get_subscription_coroutine(
                        binance_extractor, ws_data_type
                    )
                    hasynci.run(coroutine, event_loop=event_loop)
                # After subscription, output from the binance will be mocked.
                mock_ws_connection_instance.recv_data_frame = umock.MagicMock(
                    side_effect=mock_test_data
                )
                # Pausing code execution to prevent a race condition between the handle and subscribe method.
                time.sleep(1)
                # Test method.
                actual = getattr(
                    binance_extractor,
                    f"_download_websocket_{ws_data_type}",
                )(
                    exchange_id,
                    currency_pair[0],
                    start_date=start_date,
                    end_date=end_date,
                )
                self.assert_equal(str(actual), expected, fuzzy_match=True)
        except Exception as e:
            # In case of bug we want the output from mock_connection to end.
            mock_ws_connection_instance.recv_data_frame = umock.MagicMock(
                side_effect=mock_test_data
            )
            raise e
        finally:
            binance_extractor.close()


class TestBinanceExtractor4(hunitest.TestCase):
    def test_download_bid_ask(self) -> None:
        """
        Test the `download_bid_ask` method.
        """
        # Prepare parameters.
        currency_pair = "BTC_USDT"
        exchange_id = "binance"
        start_date = pd.Timestamp("2023-09-01 00:00:00")
        end_date = pd.Timestamp("2023-09-01 23:59:00")
        contract_type = "futures"
        data_type = "bid_ask"
        # Mock API response.
        mock_data = {
            "data": [
                {"day": "2023-09-01", "url": "http://mock_url"},
            ]
        }
        mock_response = umock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_data
        snap_df = {
            "symbol": {0: "ZILUSDT", 1: "ZILUSDT", 2: "ZILUSDT"},
            "timestamp": {0: 1693612789005, 1: 1693612789005, 2: 1693612789005},
            "trans_id": {
                0: 1693584963865803585,
                1: 1693584963865803585,
                2: 1693584963865803585,
            },
            "first_update_id": {
                0: 3225154735222,
                1: 3225154735222,
                2: 3225154735222,
            },
            "last_update_id": {
                0: 3225154735222,
                1: 3225154735222,
                2: 3225154735222,
            },
            "side": {0: "a", 1: "a", 2: "a"},
            "update_type": {0: "snap", 1: "snap", 2: "snap"},
            "price": {0: 0.01785, 1: 0.01786, 2: 0.01788},
            "qty": {0: 734.0, 1: 15445.0, 2: 1771.0},
        }
        update_df = {
            "symbol": {0: "ZILUSDT", 1: "ZILUSDT", 2: "ZILUSDT"},
            "timestamp": {0: 1693526389012, 1: 1693526389161, 2: 1693526389246},
            "trans_id": {
                0: 1693526389012249955,
                1: 1693526389161711482,
                2: 1693526389243901107,
            },
            "first_update_id": {
                0: 3221489207136,
                1: 3221489211639,
                2: 3221489222922,
            },
            "last_update_id": {
                0: 3221489207136,
                1: 3221489211639,
                2: 3221489222922,
            },
            "side": {0: "a", 1: "a", 2: "a"},
            "update_type": {0: "set", 1: "set", 2: "set"},
            "price": {0: 0.01615, 1: 0.01615, 2: 0.01616},
            "qty": {0: 164453.0, 1: 134840.0, 2: 899880.0},
        }
        try:
            with umock.patch.object(
                imvbdexex.imvbdeapcl.BinanceAPIClient,
                "_get",
                # Mock current time stamp.
                return_value=mock_response,
            ), umock.patch.object(
                imvbdexex.BinanceExtractor,
                "extract_all_files_from_tar_to_dataframe",
            ) as mock_extract:
                mock_extract.return_value = [
                    pd.DataFrame(snap_df),
                    pd.DataFrame(update_df),
                ]
                binance_extractor = imvbdexex.BinanceExtractor(
                    contract_type,
                    imvbdexex.BinanceNativeTimePeriod.DAILY,
                    data_type,
                )
                # Run.
                data = binance_extractor._download_bid_ask(
                    exchange_id,
                    currency_pair,
                    start_timestamp=start_date,
                    end_timestamp=end_date,
                )
        except Exception as e:
            raise e
        finally:
            binance_extractor.close()
        # Compare results.
        expected_df = pd.concat(
            [pd.DataFrame(snap_df), pd.DataFrame(update_df)], ignore_index=True
        )
        expected_df.rename(columns={"symbol": "currency_pair"}, inplace=True)
        expected = hpandas.df_to_str(expected_df)
        actual = hpandas.df_to_str(next(data))
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestBinanceExtractorValidateDateRange(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that empty date values are handled correctly.
        """
        # Define test params.
        start_date = None
        end_date = None
        # Run.
        with self.assertRaises(AssertionError) as result:
            imvbdexex.BinanceExtractor._validate_date_range(
                start_timestamp=start_date,
                end_timestamp=end_date,
            )
        actual = str(result.exception)
        # Define expected value.
        expected = r"""

        ################################################################################
        * Failed assertion *
        'None'
        !=
        'None'
        Start/End time cannot be none.
        ################################################################################

        """
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test2(self) -> None:
        """
        Check that days difference exceeding 7 days is handled.
        """
        # Define test params.
        start_date = pd.Timestamp("2024-01-01 00:00:00")
        end_date = pd.Timestamp("2024-01-11 00:00:00")
        # Run.
        with self.assertRaises(AssertionError) as result:
            imvbdexex.BinanceExtractor._validate_date_range(
                start_timestamp=start_date,
                end_timestamp=end_date,
            )
        actual = str(result.exception)
        # Define expected value.
        expected = r"""

        ################################################################################
        * Failed assertion *
        10 < 7
        Invalid date range. Difference should not be more than 7 days.
        ################################################################################

        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestExtractAllFilesFromTarToDataframe(hunitest.TestCase):
    def helper(self) -> Any:
        """
        Prepare input file with dummy data for testing.
        """
        # Get file path for storing data.
        scratch_space = self.get_scratch_space()
        filename = "test_data.tar.gz"
        file_path = imvbdexex.os.path.join(scratch_space, filename)
        # Create csv files with data.
        symbol = "T_DEPTH"
        currency_pair = "ALT_USDT"
        data = self._get_dummy_csv_data(symbol, currency_pair)
        data.to_csv(
            imvbdexex.os.path.join(scratch_space, "data1.csv"), index=False
        )
        symbol = "S_DEPTH"
        currency_pair = "UNI_USDT"
        data = self._get_dummy_csv_data(symbol, currency_pair)
        data.to_csv(
            imvbdexex.os.path.join(scratch_space, "data2.csv"), index=False
        )
        # Compress data files into tar file.
        with imvbdexex.tarfile.open(file_path, "w:gz") as tar:
            tar.add(imvbdexex.os.path.join(scratch_space, "data1.csv"))
            tar.add(imvbdexex.os.path.join(scratch_space, "data2.csv"))
        return file_path

    def test1(self) -> None:
        """
        Check the outcome of the function
        extract_all_files_from_tar_to_dataframe.
        """
        # Define test params.
        mock_url = "https://mock_url"
        # Fetch data for testing.
        filepath = self.helper()
        # Mock get call for fetching tar file.
        with umock.patch.object(imvbdexex.requests, "get") as mock_get_request:
            mock_get_request.return_value = self._mock_response(filepath)
            # Run.
            actual = imvbdexex.BinanceExtractor.extract_all_files_from_tar_to_dataframe(
                mock_url
            )
            # Define expected value.
            expected = r"""
            [      symbol    currency_pair  price  qty
                0  T_DEPTH      ALT_USDT     10    5
                1  T_DEPTH      ALT_USDT     11    6
                2  T_DEPTH      ALT_USDT     12    7
                3  T_DEPTH      ALT_USDT     13    8
                4  T_DEPTH      ALT_USDT     14    9
                5  T_DEPTH      ALT_USDT     15    5
                6  T_DEPTH      ALT_USDT     16    6
                7  T_DEPTH      ALT_USDT     17    7
                8  T_DEPTH      ALT_USDT     18    8
                9  T_DEPTH      ALT_USDT     19    9,  symbol currency_pair  price  qty
                0  S_DEPTH      UNI_USDT     10    5
                1  S_DEPTH      UNI_USDT     11    6
                2  S_DEPTH      UNI_USDT     12    7
                3  S_DEPTH      UNI_USDT     13    8
                4  S_DEPTH      UNI_USDT     14    9
                5  S_DEPTH      UNI_USDT     15    5
                6  S_DEPTH      UNI_USDT     16    6
                7  S_DEPTH      UNI_USDT     17    7
                8  S_DEPTH      UNI_USDT     18    8
                9  S_DEPTH      UNI_USDT     19    9]
            """
            self.assert_equal(str(actual), expected, fuzzy_match=True)

    def _get_dummy_csv_data(
        self, symbol: str, currency_pair: str
    ) -> pd.DataFrame:
        """
        Fetch dummy dataframe for file.
        """
        timestamp_index = pd.date_range("2024-01-01", periods=10, freq="T")
        csv_data = {
            "symbol": [symbol] * 10,
            "currency_pair": [currency_pair] * 10,
            "price": list(range(10, 20)),
            "qty": list(range(5, 10)) * 2,
        }
        df = pd.DataFrame(data=csv_data)
        df.index = timestamp_index
        return df

    def _mock_response(self, file_path: str) -> imvbdexex.requests.Response:
        """
        Build mock response for url request with success status code.
        """
        mock_resp = imvbdexex.requests.Response()
        mock_resp.status_code = 200
        mock_resp.raw = open(file_path, "rb")
        return mock_resp


class TestCheckDownloadResponseForError(hunitest.TestCase):
    def helper(
        self, status_code: int, message: str = ""
    ) -> imvbdexex.requests.Response:
        """
        Create mock response for test using provided params.
        """
        # Define parameters.
        mock_response = imvbdexex.requests.Response()
        mock_response.status_code = status_code
        mock_response._content = bytes(message, encoding="utf-8")
        return mock_response

    def test1(self) -> None:
        """
        Check that status code 200 is processed correctly.
        """
        # Define test params.
        status_code = 200
        mock_response = self.helper(status_code)
        # Run.
        imvbdexex.BinanceExtractor._check_download_response_for_error(
            mock_response
        )

    def test2(self) -> None:
        """
        Check that error code is handled correctly.
        """
        # Define test params.
        error_code = 429
        message = "Hit limit exceeded"
        expected = r"""
        Hit limit exceeded
        """
        mock_response = self.helper(error_code, message)
        with self.assertRaises(AssertionError) as ae:
            # Run.
            imvbdexex.BinanceExtractor._check_download_response_for_error(
                mock_response
            )
        actual = str(ae.exception)
        self.assert_equal(actual, expected, fuzzy_match=True)
