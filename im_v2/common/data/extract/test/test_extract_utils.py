import argparse
import asyncio
import unittest.mock as umock
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import pytest

import helpers.henv as henv
import helpers.hmoto as hmoto
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import helpers.hunit_test as hunitest
import im_v2.ccxt.data.extract.extractor as imvcdexex
import im_v2.ccxt.db.utils as imvccdbut
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.data.transform.resample_daily_bid_ask_data as imvcdtrdbad
import im_v2.common.db.db_utils as imvcddbut
import im_v2.crypto_chassis.data.extract.extractor as imvccdexex


class TestDownloadExchangeDataToDbPeriodically1(hunitest.TestCase):
    # Regular mock for capturing logs.
    log_patch = umock.patch.object(imvcdeexut, "_LOG")
    # Mock call to function that is calling external provider.
    realtime_download_patch = umock.patch.object(
        imvcdeexut,
        "_download_exchange_data_to_db_with_timeout",
        spec=imvcdeexut._download_exchange_data_to_db_with_timeout,
    )
    # Mock current time calls.
    timedelta_patch = umock.patch.object(
        imvcdeexut, "timedelta", spec=imvcdeexut.timedelta
    )
    datetime_patch = umock.patch.object(
        imvcdeexut, "datetime", spec=imvcdeexut.datetime
    )
    sleep_patch = umock.patch.object(
        imvcdeexut.time, "sleep", spec=imvcdeexut.time.sleep
    )

    def setUp(self) -> None:
        super().setUp()
        # Create new mocks from patch's start() method.
        self.log_mock: umock.MagicMock = self.log_patch.start()
        self.realtime_download_mock: umock.MagicMock = (
            self.realtime_download_patch.start()
        )
        self.timedelta_mock: umock.MagicMock = self.timedelta_patch.start()
        self.datetime_mock: umock.MagicMock = self.datetime_patch.start()
        self.sleep_mock: umock.MagicMock = self.sleep_patch.start()
        # Commonly used extractor mock.
        self.extractor_mock = umock.create_autospec(
            imvcdexex.CcxtExtractor, instance=True
        )
        # Commonly used kwargs across the tests.
        self.kwargs = {
            "data_type": "ohlcv",
            "exchange_id": "binance",
            "universe": "small",
            "db_stage": "dev",
            "db_table": "ccxt_ohlcv_test",
            "aws_profile": "ck",
            "s3_path": "s3://cryptokaizen-data-test/realtime/",
            "interval_min": 1,
            "start_time": "2022-08-04 21:17:35",
            "stop_time": "2022-08-04 21:20:35",
            "method": "rest",
        }
        # Predefined side effects for successful run.
        iteration_delay_sec = timedelta(seconds=1)
        time_window_min = timedelta(minutes=5)
        interval_mins = [timedelta(minutes=1) for _ in range(3)]
        self.timedelta_side_effect = [
            iteration_delay_sec,
            time_window_min,
            *interval_mins,
            # Exit on second iteration.
            iteration_delay_sec,
        ]
        #
        enter_while_loop = [datetime(2022, 8, 4, 21, 17, 34) for _ in range(3)]
        end_timestamp = datetime(2022, 8, 4, 21, 17, 35)
        complete_without_grid_align = [
            datetime(2022, 8, 4, 21, 18, 15) for _ in range(3)
        ]
        second_iteration_exit = datetime(2022, 8, 4, 21, 22, 45)
        self.datetime_side_effect = [
            *enter_while_loop,
            end_timestamp,
            *complete_without_grid_align,
            # Exit on second iteration.
            second_iteration_exit,
        ]

    def tearDown(self) -> None:
        self.log_patch.stop()
        self.realtime_download_patch.stop()
        self.timedelta_patch.stop()
        self.datetime_patch.stop()
        self.sleep_patch.stop()
        # Deallocate in reverse order to avoid race conditions.
        super().tearDown()

    def call_download_realtime_for_one_exchange_periodically(
        self, additional_kwargs: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Test directly function call for coverage increase.
        """
        # Prepare inputs and keep original kwargs intact.
        kwargs = {**self.kwargs}
        if additional_kwargs:
            kwargs.update(additional_kwargs)
        # Run.
        imvcdeexut.download_realtime_for_one_exchange_periodically(
            kwargs, self.extractor_mock
        )
        # Check call.
        self.assertEqual(self.realtime_download_mock.call_count, 1)
        actual_args = tuple(self.realtime_download_mock.call_args)
        expected_args = (
            (
                self.kwargs,
                self.extractor_mock,
                pd.Timestamp("2022-08-04 21:12:00"),
                datetime(2022, 8, 4, 21, 17, 0),
            ),
            {},
        )
        self.assertEqual(actual_args, expected_args)

    def test_function_call1(self) -> None:
        """
        Verify clean periodical download without any issues.
        """
        # Set mock return values for exactly one iteration.
        self.timedelta_mock.side_effect = self.timedelta_side_effect
        self.datetime_mock.now.side_effect = self.datetime_side_effect
        # Run.
        self.call_download_realtime_for_one_exchange_periodically()
        # Check mock states.
        actual_logs = str(self.log_mock.method_calls)
        expected_logs = r"""
            [call.info('Delay %s sec until next iteration', 1.0),
            call.info('Successfully completed, iteration took %s sec', 40.0)]
        """
        self.assert_equal(actual_logs, expected_logs, fuzzy_match=True)
        #
        actual_calls = str(self.timedelta_mock.call_args_list)
        expected_calls = r"""
            [call(seconds=1.0),
            call(minutes=5),
            call(minutes=1),
            call(minutes=1),
            call(minutes=1),
            call(seconds=20.0)]
        """
        self.assert_equal(actual_calls, expected_calls, fuzzy_match=True)

    def test_function_call2(self) -> None:
        """
        Verify download that takes more time than `interval_min`.
        """
        # Set mock return values for one iteration.
        timedelta_for_align = self.timedelta_side_effect[:-1]
        timedelta_for_align.append(self.timedelta_side_effect[-2])
        timedelta_for_align.append(self.timedelta_side_effect[-1])
        self.timedelta_mock.side_effect = timedelta_for_align
        #
        long_download = self.datetime_side_effect[:4]
        long_download.extend([datetime(2022, 8, 4, 21, 18, 45) for _ in range(3)])
        long_download.append(self.datetime_side_effect[-1])
        self.datetime_mock.now.side_effect = long_download
        # Run.
        self.call_download_realtime_for_one_exchange_periodically()
        # Check mock states.
        actual_logs = str(self.log_mock.method_calls)
        expected_logs = r"""
            [call.info('Delay %s sec until next iteration', 1.0),
             call.error('The download was not finished in %s minutes.', 1),
             call.debug('Initial start time before align `%s`.', Timestamp('2022-08-04 21:17:35')),
             call.debug('Start time after align `%s`.', Timestamp('2022-08-04 21:18:35'))]
        """
        self.assert_equal(actual_logs, expected_logs, fuzzy_match=True)
        #
        actual_calls = str(self.timedelta_mock.call_args_list)
        expected_calls = r"""
            [call(seconds=1.0),
             call(minutes=5),
             call(minutes=1),
             call(minutes=1),
             call(minutes=1),
             call(minutes=1),
             call(seconds=0)]
        """
        self.assert_equal(actual_calls, expected_calls, fuzzy_match=True)

    def test_function_call3(self) -> None:
        """
        Verify runtime error.
        """
        # Set mock return values for 5 iterations.
        self.realtime_download_mock.side_effect = [
            Exception("Dummy1"),
            Exception("Dummy2"),
            Exception("Dummy3"),
            Exception("Dummy4"),
            Exception("Dummy5"),
        ]
        #
        timedelta_for_error = self.timedelta_side_effect[:3]
        [timedelta_for_error.extend(timedelta_for_error) for _ in range(4)]
        self.timedelta_mock.side_effect = timedelta_for_error
        #
        download_for_error = self.datetime_side_effect[:5]
        [download_for_error.extend(download_for_error) for _ in range(4)]
        self.datetime_mock.now.side_effect = download_for_error
        with self.assertRaises(RuntimeError) as fail:
            # Run.
            self.call_download_realtime_for_one_exchange_periodically()
        actual_error = str(fail.value)
        expected_error = "5 consecutive downloads were failed"
        self.assert_equal(expected_error, actual_error)
        # Check mock states.
        actual_logs = str(self.log_mock.method_calls)
        expected_logs = r"""
            [call.info('Delay %s sec until next iteration', 1.0),
             call.error('Download failed %s', 'Dummy1'),
             call.info('Start repeat download immediately.'),
             call.info('Delay %s sec until next iteration', 0),
             call.error('Download failed %s', 'Dummy2'),
             call.info('Start repeat download immediately.'),
             call.info('Delay %s sec until next iteration', 0),
             call.error('Download failed %s', 'Dummy3'),
             call.info('Start repeat download immediately.'),
             call.info('Delay %s sec until next iteration', 0),
             call.error('Download failed %s', 'Dummy4'),
             call.info('Start repeat download immediately.'),
             call.info('Delay %s sec until next iteration', 0),
             call.error('Download failed %s', 'Dummy5')]
        """
        self.assert_equal(actual_logs, expected_logs, fuzzy_match=True)

    def test_invalid_input1(self) -> None:
        """
        Run with wrong `interval_min`.
        """
        additional_kwargs = {"interval_min": 0}
        self.datetime_mock.now.return_value = datetime(2020, 8, 4, 21, 17, 36)
        with self.assertRaises(AssertionError) as fail:
            # Run.
            self.call_download_realtime_for_one_exchange_periodically(
                additional_kwargs=additional_kwargs
            )
        # Check output for error.
        actual_error = str(fail.exception)
        expected_error = r"""
            * Failed assertion *
            1 <= 0
            interval_min: 0 should be greater than 0
        """
        self.assert_equal(actual_error, expected_error, fuzzy_match=True)

    def test_invalid_input2(self) -> None:
        """
        Run with `start_time` in the past.
        """
        self.datetime_mock.now.return_value = datetime(2022, 8, 4, 21, 17, 36)
        with self.assertRaises(AssertionError) as fail:
            # Run.
            self.call_download_realtime_for_one_exchange_periodically()
        # Check output for error.
        actual_error = str(fail.exception)
        expected_error = r"""
            * Failed assertion *
            2022-08-04 21:17:36 < 2022-08-04 21:17:35
            start_time is in the past
        """
        self.assert_equal(actual_error, expected_error, fuzzy_match=True)

    def test_invalid_input3(self) -> None:
        """
        Run with `start_time` greater than the `stop_time`.
        """
        additional_kwargs = {"start_time": "2022-08-04 21:20:36"}
        self.datetime_mock.now.return_value = datetime(2022, 8, 4, 21, 17, 34)
        with self.assertRaises(AssertionError) as fail:
            # Run.
            self.call_download_realtime_for_one_exchange_periodically(
                additional_kwargs=additional_kwargs
            )
        # Check output for error.
        actual_error = str(fail.exception)
        expected_error = r"""
            * Failed assertion *
            2022-08-04 21:20:36 < 2022-08-04 21:20:35
            stop_time is less than start_time
        """
        self.assert_equal(actual_error, expected_error, fuzzy_match=True)

    def test_invalid_input4(self) -> None:
        """
        Run with `start_time` with different timezone info.
        """
        additional_kwargs = {"start_time": "2022-08-04 21:17:35+02:00"}
        self.datetime_mock.now.return_value = datetime(2022, 8, 4, 21, 17, 34)
        with self.assertRaises(AssertionError) as fail:
            # Run.
            self.call_download_realtime_for_one_exchange_periodically(
                additional_kwargs=additional_kwargs
            )
        # Check output for error.
        actual_error = str(fail.exception)
        expected_error = r"""
            * Failed assertion *
            'True'
            ==
            'False'
            datetime1='2022-08-04 21:17:35+02:00' and datetime2='2022-08-04 21:20:35' are not compatible
        """
        self.assert_equal(actual_error, expected_error, fuzzy_match=True)


class TestDownloadExchangeDataToDbPeriodically2(hunitest.TestCase):
    """
    Test `download_exchange_data_to_db_periodically` function to download
    bid_ask data through websockets.
    """

    def setUp(self) -> None:
        # Add an isolated events loop.
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # Mock DB connection.
        self.mock_connection_manager = umock.patch.object(
            imvcdeexut.imvcddbut,
            "DbConnectionManager",
            autospec=True,
            return_value=umock.MagicMock(),
        )
        self.mock_connection_manager.start()
        super().setUp()

    def get_mock_ccxt_okx_data(self, timestamp: int) -> Dict[str, Any]:
        """
        Get mock data for OKEx exchange.

        :param timestamp: timestamp in milliseconds.
        :return: mock data.
        """
        return {
            "bids": [[24017.8, 0.0028975] for _ in range(20)],
            "asks": [[24017.9, 0.0028975] for _ in range(20)],
            "timestamp": timestamp,
            "symbol": None,
        }

    @pytest.mark.slow("18 seconds")
    def test_realtime_bid_ask_download(self) -> None:
        """
        Test downloading bid_ask data through websockets.

        Mock data is generated for OKEx exchange. Check that data is
        downloaded and saved to DB.
        """
        # Prepare mock data.
        start_time = pd.Timestamp.now() + pd.Timedelta(seconds=5)
        stop_time = start_time + pd.Timedelta(seconds=10)
        args = {
            "download_mode": "realtime",
            "downloading_entity": "manual",
            "action_tag": "downloaded_200ms",
            "vendor": "ccxt",
            "exchange_id": "okx",
            "universe": "v7.3",
            "db_stage": "test",
            "db_table": "ccxt_bid_ask_futures_raw",
            "aws_profile": "ck",
            "data_type": "bid_ask",
            "data_format": "postgres",
            "contract_type": "futures",
            "start_time": start_time,
            "stop_time": stop_time,
            "method": "websocket",
        }

        # Hack asyncio for mocking.
        async def async_magic() -> None:
            pass

        umock.MagicMock.__await__ = lambda x: async_magic().__await__()
        mock_okx = umock.MagicMock(spec=imvcdexex.ccxtpro.okx())
        imvcdexex.ccxtpro.okx.return_value = mock_okx
        mock_okx.watchOrderBook.return_value = {}
        mock_okx.orderbooks["some_symbol"].limit.return_value = [
            self.get_mock_ccxt_okx_data(
                start_time.timestamp() * 1000 + milliseconds
            )
            for milliseconds in range(0, 10 * 1000, 200)
        ]
        exchange = imvcdexex.CcxtExtractor(
            args["exchange_id"], args["contract_type"]
        )
        # Run.
        with umock.patch.object(
            imvcdeexut.imvcddbut, "save_data_to_db"
        ) as save_data_to_db:
            imvcdeexut.download_realtime_for_one_exchange_periodically(
                args, exchange
            )
        # Check output.
        # Get the first dataset that was saved to DB.
        first_df_to_save = save_data_to_db.call_args[0][0]
        # Check that all timestamps are between date-time range.
        self.assertTrue(
            all(
                first_df_to_save["timestamp"].between(
                    start_time.timestamp() * 1000,
                    stop_time.timestamp() * 1000,
                )
            )
        )
        # Make sure that all levels are in the range from 1 to 10.
        self.assertTrue(all(first_df_to_save["level"].unique() == range(1, 11)))

    def tearDown(self) -> None:
        self.mock_connection_manager.stop()
        self.loop.close()
        super().tearDown()


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadExchangeDataToDb1(
    hmoto.S3Mock_TestCase, imvcddbut.TestImDbHelper
):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def setUp(self) -> None:
        super().setUp()
        # Initialize database.
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)

    def tearDown(self) -> None:
        super().tearDown()
        # Drop table used in tests.
        ccxt_ohlcv_drop_query = "DROP TABLE IF EXISTS ccxt_ohlcv_spot;"
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)

    def call_download_exchange_data_to_db(self, use_s3: bool) -> None:
        """
        Test directly function call for coverage increase.
        """
        # Prepare inputs.
        kwargs = {
            "start_timestamp": "2021-11-10 10:11:00+00:00",
            "end_timestamp": "2021-11-10 10:12:00+00:00",
            "exchange_id": "binance",
            "universe": "v3",
            "data_type": "ohlcv",
            "contract_type": "spot",
            "db_stage": "local",
            "db_table": "ccxt_ohlcv_spot",
            "incremental": False,
            "log_level": "INFO",
            "aws_profile": None,
            "s3_path": None,
            "connection": self.connection,
        }
        extractor = imvcdexex.CcxtExtractor(
            kwargs["exchange_id"], kwargs["contract_type"]
        )
        if use_s3:
            # Update kwargs.
            kwargs.update(
                {
                    "aws_profile": self.mock_aws_profile,
                    "s3_path": f"s3://{self.bucket_name}/",
                }
            )
        # Run.
        imvcdeexut.download_exchange_data_to_db(kwargs, extractor)
        # Get saved data in db.
        select_all_query = "SELECT * FROM ccxt_ohlcv_spot;"
        actual_df = hsql.execute_query_to_df(self.connection, select_all_query)
        # Check data output.
        actual = hpandas.df_to_str(actual_df, num_rows=5000, max_colwidth=15000)
        # pylint: disable=line-too-long
        expected = r"""   id      timestamp        open        high         low       close         volume               currency_pair exchange_id    end_download_timestamp       knowledge_timestamp
            0   1  1636539060000      2.2270      2.2280      2.2250      2.2250  7.188450e+04      ADA_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            1   2  1636539060000     92.4400     92.4700     92.2600     92.2600  1.309350e+03     AVAX_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            2   3  1636539060000    648.9000    649.0000    648.7000    648.9000  6.547400e+02      BNB_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            3   4  1636539060000  66774.0200  66779.9200  66770.0300  66774.0500  1.503426e+01      BTC_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            4   5  1636539060000      0.2736      0.2737      0.2732      0.2733  1.170147e+06     DOGE_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            5   6  1636539060000      5.1910      5.1910      5.1860      5.1860  4.172600e+03      EOS_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            6   7  1636539060000   4716.5200   4716.8400   4715.6400   4715.6500  9.941380e+01      ETH_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            7   8  1636539060000     34.9400     34.9400     34.8800     34.8900  5.722750e+03     LINK_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00
            8   9  1636539060000    242.5400    242.5400    242.3500    242.3500  4.506200e+02      SOL_USDT     binance 2021-11-10 10:12:00+00:00 2021-11-10 10:12:00+00:00"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    @pytest.mark.skip(
        "Cannot be run from the US due to 451 error API error. Run manually."
    )
    @pytest.mark.slow
    @umock.patch.object(imvcdexex.hdateti, "get_current_timestamp_as_string")
    @umock.patch.object(imvcddbut.hdateti, "get_current_time")
    def test_function_call1(
        self,
        mock_get_current_time: umock.MagicMock,
        mock_get_current_timestamp_as_string: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and checking saved content in database.

        Run without saving to s3.
        """
        # Set mock return values.
        mock_get_current_time.return_value = "2021-11-10 10:12:00.000000+00:00"
        mock_get_current_timestamp_as_string.return_value = "20211110-101200"
        # Run.
        use_s3 = False
        self.call_download_exchange_data_to_db(use_s3)
        # Check mock state.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        self.assertEqual(mock_get_current_timestamp_as_string.call_count, 0)
        self.assertEqual(mock_get_current_timestamp_as_string.call_args, None)

    @pytest.mark.skip(reason="CMTask2089 and CmTask3359")
    @umock.patch.object(imvcdexex.hdateti, "get_current_timestamp_as_string")
    @umock.patch.object(imvcdeexut.hdateti, "get_current_time")
    def test_function_call2(
        self,
        mock_get_current_time: umock.MagicMock,
        mock_get_current_timestamp_as_string: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and checking saved content in database.

        Run and save to s3.
        """
        # Set mock return values.
        mock_get_current_time.return_value = "2021-11-10 10:12:00.000000+00:00"
        mock_get_current_timestamp_as_string.return_value = "20211110-000001"
        # Run.
        use_s3 = True
        self.call_download_exchange_data_to_db(use_s3)
        # Check mock state.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        self.assertEqual(mock_get_current_timestamp_as_string.call_count, 9)
        self.assertEqual(
            mock_get_current_timestamp_as_string.call_args.args, ("UTC",)
        )
        # Prepare common `hs3.listdir` params.
        s3_bucket = f"s3://{self.bucket_name}"
        pattern = "*.csv"
        only_files = True
        use_relative_paths = True
        # Check csv files on s3.
        csv_path_list = hs3.listdir(
            s3_bucket,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=self.mock_aws_profile,
        )
        csv_path_list.sort()
        expected = [
            "binance/ADA_USDT_20211110-000001.csv",
            "binance/AVAX_USDT_20211110-000001.csv",
            "binance/BNB_USDT_20211110-000001.csv",
            "binance/BTC_USDT_20211110-000001.csv",
            "binance/DOGE_USDT_20211110-000001.csv",
            "binance/EOS_USDT_20211110-000001.csv",
            "binance/ETH_USDT_20211110-000001.csv",
            "binance/LINK_USDT_20211110-000001.csv",
            "binance/SOL_USDT_20211110-000001.csv",
        ]
        self.assertListEqual(csv_path_list, expected)


def get_simple_crypto_chassis_mock_data(
    start_timestamp: int,
    number_of_seconds: int,
    *,
    currency_pair: str = "ADA_USDT",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp": start_timestamp + sec,
                "bid_price_l1": 0.3481,
                "bid_size_l1": 49676.8,
                "bid_price_l2": 0.3482,
                "bid_size_l2": 49676.8,
                "ask_price_l1": 0.3484,
                "ask_size_l1": 49676.8,
                "ask_price_l2": 0.3485,
                "ask_size_l2": 49676.8,
                "currency_pair": currency_pair,
            }
            for sec in range(number_of_seconds)
        ]
    )


@pytest.mark.slow("Takes around 6 secs")
class TestDownloadResampleBidAskData(hmoto.S3Mock_TestCase):
    def setUp(self) -> None:
        self.start_date = datetime(2022, 1, 1)
        self.end_date = self.start_date + timedelta(seconds=4)
        self.path = (
            "s3://mock_bucket/v3/periodic_daily/manual/downloaded_1sec/"
            "parquet/ohlcv/futures/v3/crypto_chassis/binance/v1_0_0"
        )
        self.src_signature = (
            "periodic_daily.manual.downloaded_1sec"
            ".parquet.ohlcv.futures.v3.crypto_chassis.binance.v1_0_0"
        )
        self.dst_signature = (
            "periodic_daily.manual.resampled_1min"
            ".parquet.ohlcv.futures.v3.crypto_chassis.binance.v1_0_0"
        )
        super().setUp()
        self.s3fs_ = hs3.get_s3fs(self.mock_aws_profile)

    def call_download_historical_data(self) -> None:
        """
        Call download_historical_data with the predefined arguments.
        """
        # Prepare inputs.
        args = {
            "start_timestamp": self.start_date.strftime("%y-%m-%d %H:%M:%S"),
            "end_timestamp": self.end_date.strftime("%y-%m-%d %H:%M:%S"),
            "download_mode": "periodic_daily",
            "downloading_entity": "manual",
            "action_tag": "downloaded_1sec",
            "vendor": "crypto_chassis",
            "exchange_id": "binance",
            "data_type": "ohlcv",
            "contract_type": "futures",
            "universe": "v3",
            "incremental": False,
            "aws_profile": self.mock_aws_profile,
            "s3_path": f"s3://{self.bucket_name}/",
            "log_level": "INFO",
            "data_format": "parquet",
            "unit": "s",
            "universe_part": 1,
            "assert_on_missing_data": False,
        }
        exchange = imvccdexex.CryptoChassisExtractor(args["contract_type"])
        imvcdeexut.download_historical_data(args, exchange)

    @umock.patch.object(imvcdeexut.ivcu, "get_vendor_universe")
    def check_download_historical_data(self, mock_get_vendor_universe):
        """
        First part:

        - run the downloader and mock its request to crypto_chassis
        - downloader save the fixture to the fake AWS S3
        - get data from S3 and compare with expected result
        """

        def mock_download_data(*args, **kwargs) -> pd.DataFrame:
            """
            Mock download_data to return predefined results.
            """
            currency_pair = args[2].lower()
            if "btc" in currency_pair:
                currency_pair = "BTC_USDT"
            else:
                currency_pair = "ADA_USDT"
            return get_simple_crypto_chassis_mock_data(
                start_timestamp=int(self.start_date.timestamp()),
                number_of_seconds=4,
                currency_pair=currency_pair,
            )

        # Let the downloader to put our fixture to the fake S3.
        with umock.patch.object(
            imvccdexex.CryptoChassisExtractor,
            "download_data",
            new=mock_download_data,
        ):
            mock_universe = umock.MagicMock()
            mock_universe.__getitem__.return_value = ["ADA_USDT", "BTC_USDT"]
            mock_get_vendor_universe.return_value = mock_universe
            self.call_download_historical_data()
        # Make sure a list of folder is expected.
        parquet_path_list = hs3.listdir(
            dir_name=self.path,
            pattern="*.parquet",
            only_files=True,
            use_relative_paths=True,
            aws_profile=self.mock_aws_profile,
        )
        parquet_path_list.sort()
        parquet_path_list = [
            # Remove uuid names.
            "/".join(pq_path.split("/")[:-1])
            for pq_path in parquet_path_list
        ]
        expected_list = [
            "currency_pair=ADA_USDT/year=2022/month=1",
            "currency_pair=BTC_USDT/year=2022/month=1",
        ]
        self.assertListEqual(parquet_path_list, expected_list)
        actual_df = hparque.from_parquet(
            file_name=self.path, aws_profile=self.s3fs_
        )
        actual_df = actual_df.drop(["knowledge_timestamp"], axis=1)
        actual = hpandas.df_to_str(actual_df, num_rows=5000, max_colwidth=15000)
        expected = r"""timestamp  bid_price_l1  bid_size_l1  bid_price_l2  bid_size_l2  ask_price_l1  ask_size_l1  ask_price_l2  ask_size_l2 exchange_id currency_pair  year  month
            timestamp
            2022-01-01 00:00:00+00:00  1640995200        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      ADA_USDT  2022      1
            2022-01-01 00:00:01+00:00  1640995201        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      ADA_USDT  2022      1
            2022-01-01 00:00:02+00:00  1640995202        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      ADA_USDT  2022      1
            2022-01-01 00:00:03+00:00  1640995203        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      ADA_USDT  2022      1
            2022-01-01 00:00:00+00:00  1640995200        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      BTC_USDT  2022      1
            2022-01-01 00:00:01+00:00  1640995201        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      BTC_USDT  2022      1
            2022-01-01 00:00:02+00:00  1640995202        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      BTC_USDT  2022      1
            2022-01-01 00:00:03+00:00  1640995203        0.3481      49676.8        0.3482      49676.8        0.3484      49676.8        0.3485      49676.8     binance      BTC_USDT  2022      1"""
        self.assert_equal(actual, expected, fuzzy_match=True)

    def check_resampler(self) -> None:
        """
        Second part:

        - run the resampler
        - resampler save the data to the fake AWS S3
        - get data from S3 and compare with expected result
        - check assert_all_resampled parameter
        """
        # Prepare the data for the resampler.
        base_s3_path = "s3://mock_bucket/"
        run_args = {
            "start_timestamp": "2022-01-01 00:00:00",
            "end_timestamp": "2022-01-01 00:04:00",
            "src_signature": self.src_signature,
            "dst_signature": self.dst_signature,
            "src_s3_path": base_s3_path,
            "dst_s3_path": base_s3_path,
            "assert_all_resampled": True,
        }
        namespace = argparse.Namespace(**run_args)
        # Run the resampler.
        with umock.patch(
            "im_v2.common.data.transform.transform_utils"
            ".NUMBER_LEVELS_OF_ORDER_BOOK",
            2,
        ):
            imvcdtrdbad._run(namespace, aws_profile=self.s3fs_)
        dst_dir = imvcdtrdbad._get_s3_path_from_signature(
            self.dst_signature,
            base_s3_path,
        )
        # Get the result from the fake S3.
        actual_df = hparque.from_parquet(dst_dir, aws_profile=self.s3fs_)
        # Need to exclude knowledge_timestamp that can't predict precisely.
        actual_df = actual_df.drop(["knowledge_timestamp"], axis=1)
        # Compare with expected result.
        actual = hpandas.df_to_str(actual_df, num_rows=5000, max_colwidth=15000)
        expected = r"""timestamp  level_1.bid_price.close  level_1.bid_size.close  level_1.ask_price.close  level_1.ask_size.close  level_1.bid_price.high  level_1.bid_size.max  level_1.ask_price.high  level_1.ask_size.max  level_1.bid_price.low  level_1.bid_size.min  level_1.ask_price.low  level_1.ask_size.min  level_1.bid_price.mean  level_1.bid_size.mean  level_1.ask_price.mean  level_1.ask_size.mean  level_2.bid_price.close  level_2.bid_size.close  level_2.ask_price.close  level_2.ask_size.close  level_2.bid_price.high  level_2.bid_size.max  level_2.ask_price.high  level_2.ask_size.max  level_2.bid_price.low  level_2.bid_size.min  level_2.ask_price.low  level_2.ask_size.min  level_2.bid_price.mean  level_2.bid_size.mean  level_2.ask_price.mean  level_2.ask_size.mean exchange_id currency_pair  year  month
            timestamp
            2022-01-01 00:01:00+00:00 1640995260 0.3481 49676.8 0.3484 49676.8 0.3481 49676.8 0.3484 49676.8 0.3481 49676.8 0.3484 49676.8 0.3481 49676.8 0.3484 49676.8 0.3482 49676.8 0.3485 49676.8 0.3482 49676.8 0.3485 49676.8 0.3482 49676.8 0.3485 49676.8 0.3482 49676.8 0.3485 49676.8 binance ADA_USDT 2022 1
            2022-01-01 00:01:00+00:00 1640995260 0.3481 49676.8 0.3484 49676.8 0.3481 49676.8 0.3484 49676.8 0.3481 49676.8 0.3484 49676.8 0.3481 49676.8 0.3484 49676.8 0.3482 49676.8 0.3485 49676.8 0.3482 49676.8 0.3485 49676.8 0.3482 49676.8 0.3485 49676.8 0.3482 49676.8 0.3485 49676.8 binance BTC_USDT 2022 1"""
        self.assert_equal(actual, expected, fuzzy_match=True)
        # Check that resampler raises an exception when assert_all_resample is True
        # and an empty df is returned for a currency pair.
        run_args = {
            "start_timestamp": "2022-01-01 00:00:00",
            "end_timestamp": "2022-01-01 00:04:00",
            "src_signature": self.src_signature,
            "dst_signature": self.dst_signature,
            "src_s3_path": base_s3_path,
            "dst_s3_path": base_s3_path,
            "assert_all_resampled": True,
        }
        namespace = argparse.Namespace(**run_args)

        def mock_resample_multilevel_bid_ask_data(
            data: pd.DataFrame,
        ) -> pd.DataFrame:
            # Return an empty df for BTC.
            if data["currency_pair"][0] == "BTC_USDT":
                return pd.DataFrame()
            return pd.DataFrame([{"timestamp": datetime.now()}])

        imvcdtrdbad.imvcdttrut.resample_multilevel_bid_ask_data_from_1sec_to_1min = (
            mock_resample_multilevel_bid_ask_data
        )
        with self.assertRaises(RuntimeError) as fail:
            imvcdtrdbad._run(namespace, aws_profile=self.s3fs_)
        self.assertIn("Missing symbols", str(fail))
        self.assertIn("BTC_USDT", str(fail))

    def test_download_and_resample_bid_ask_data(self) -> None:
        """
        Download mocked AWS S3 data, check the output, resample and check the
        output.
        """
        self.check_download_historical_data()
        self.check_resampler()


@pytest.mark.skipif(
    not henv.execute_repo_config_code("is_CK_S3_available()"),
    reason="Run only if CK S3 is available",
)
class TestDownloadHistoricalData1(hmoto.S3Mock_TestCase):
    def call_download_historical_data(
        self, incremental: bool, *, assert_on_missing_data: bool = False
    ) -> None:
        """
        Test directly function call for coverage increase.
        """
        # Prepare inputs.
        args = {
            "start_timestamp": "2021-12-31 23:00:00",
            "end_timestamp": "2022-01-01 01:00:00",
            "exchange_id": "binance",
            "vendor": "crypto_chassis",
            "data_type": "ohlcv",
            "download_mode": "periodic_daily",
            "downloading_entity": "manual",
            "action_tag": "downloaded_1sec",
            "contract_type": "spot",
            "universe": "v3",
            "incremental": incremental,
            "aws_profile": self.mock_aws_profile,
            "s3_path": f"s3://{self.bucket_name}/",
            "log_level": "INFO",
            "data_format": "parquet",
            "unit": "ms",
            "assert_on_missing_data": assert_on_missing_data,
            "universe_part": 1,
        }
        with umock.patch.object(
            imvcdexex.CcxtExtractor,
            "get_exchange_currency_pairs",
            return_value=["BTC_USDT", "ETH_USDT"],
        ):
            exchange = imvcdexex.CcxtExtractor(
                args["exchange_id"], args["contract_type"]
            )
            imvcdeexut.download_historical_data(args, exchange)

    def test_empty_dataset(self):
        """
        Check that an exception is raised if assert_on_missing_data=True
        and an empty df is returned.
        """
        # Mock downloader to return an empty dataframe.
        with umock.patch.object(
            imvcdexex.CcxtExtractor, "download_data", return_value=pd.DataFrame()
        ):
            # Check for an exception raising.
            with self.assertRaises(RuntimeError) as fail:
                self.call_download_historical_data(
                    incremental=False, assert_on_missing_data=True
                )
            self.assertIn("No data", str(fail))

    @pytest.mark.skip(reason="CMTask2089")
    @umock.patch.object(imvcdeexut.hparque, "list_and_merge_pq_files")
    @umock.patch.object(imvcdeexut.hdateti, "get_current_time")
    def test_function_call1(
        self,
        mock_get_current_time: umock.MagicMock,
        mock_list_and_merge: umock.MagicMock,
    ) -> None:
        """
        Test function call with specific arguments that are mimicking command
        line arguments and comparing function output with predefined directory
        structure and file contents.
        """
        # Set mock return values.
        mock_get_current_time.return_value = "2022-02-08 10:12:00.000000+00:00"
        # Create path for incremental mode.
        s3fs_ = hs3.get_s3fs(self.mock_aws_profile)
        with s3fs_.open("s3://mock_bucket/binance/dummy.txt", "w") as f:
            f.write("test")
        # Run.
        incremental = True
        self.call_download_historical_data(incremental)
        # Check mock state.
        self.assertEqual(mock_get_current_time.call_count, 18)
        self.assertEqual(mock_get_current_time.call_args.args, ("UTC",))
        expected_args = mock_list_and_merge.call_args.args
        expected_kwargs = mock_list_and_merge.call_args.kwargs
        self.assertEqual(len(expected_args), 1)
        self.assertEqual(expected_args[0], "s3://mock_bucket/binance")
        self.assertDictEqual(
            expected_kwargs,
            {
                "aws_profile": self.mock_aws_profile,
                "drop_duplicates_mode": "ohlcv",
            },
        )
        # Prepare common `hs3.listdir` params.
        s3_bucket = f"s3://{self.bucket_name}"
        pattern = "*.parquet"
        only_files = True
        use_relative_paths = True
        # Check parquet files on s3.
        parquet_path_list = hs3.listdir(
            s3_bucket,
            pattern,
            only_files,
            use_relative_paths,
            aws_profile=self.mock_aws_profile,
        )
        parquet_path_list.sort()
        parquet_path_list = [
            # Remove uuid names.
            "/".join(path.split("/")[:-1])
            for path in parquet_path_list
        ]
        expected_list = [
            "binance/currency_pair=ADA_USDT/year=2021/month=12",
            "binance/currency_pair=ADA_USDT/year=2022/month=1",
            "binance/currency_pair=AVAX_USDT/year=2021/month=12",
            "binance/currency_pair=AVAX_USDT/year=2022/month=1",
            "binance/currency_pair=BNB_USDT/year=2021/month=12",
            "binance/currency_pair=BNB_USDT/year=2022/month=1",
            "binance/currency_pair=BTC_USDT/year=2021/month=12",
            "binance/currency_pair=BTC_USDT/year=2022/month=1",
            "binance/currency_pair=DOGE_USDT/year=2021/month=12",
            "binance/currency_pair=DOGE_USDT/year=2022/month=1",
            "binance/currency_pair=EOS_USDT/year=2021/month=12",
            "binance/currency_pair=EOS_USDT/year=2022/month=1",
            "binance/currency_pair=ETH_USDT/year=2021/month=12",
            "binance/currency_pair=ETH_USDT/year=2022/month=1",
            "binance/currency_pair=LINK_USDT/year=2021/month=12",
            "binance/currency_pair=LINK_USDT/year=2022/month=1",
            "binance/currency_pair=SOL_USDT/year=2021/month=12",
            "binance/currency_pair=SOL_USDT/year=2022/month=1",
        ]
        self.assertListEqual(parquet_path_list, expected_list)

    @pytest.mark.skip(
        "Cannot be run from the US due to 451 error API error. Run manually."
    )
    def test_function_call2(self) -> None:
        """
        Verify error on non incremental run.
        """
        s3fs_ = hs3.get_s3fs(self.mock_aws_profile)
        with s3fs_.open("s3://mock_bucket/binance/dummy.txt", "w") as f:
            f.write("test")
        incremental = False
        with self.assertRaises(AssertionError) as fail:
            self.call_download_historical_data(incremental)
        self.assertIn(
            "S3 path 's3://mock_bucket/binance' already exist!", str(fail.value)
        )

    @pytest.mark.skip(
        "Cannot be run from the US due to 451 error API error. Run manually."
    )
    def test_function_call3(self) -> None:
        """
        Verify error on incremental run.
        """
        incremental = True
        with self.assertRaises(AssertionError) as fail:
            self.call_download_historical_data(incremental)
        self.assertIn(
            "S3 path 's3://mock_bucket/binance' doesn't exist!", str(fail.value)
        )


# TODO(gp): Difference between amp and cmamp.
@pytest.mark.skip(reason="File '/home/.aws/credentials' doesn't exist")
class TestRemoveDuplicates(hmoto.S3Mock_TestCase, imvcddbut.TestImDbHelper):
    @classmethod
    def get_id(cls) -> int:
        return hash(cls.__name__) % 10000

    def setUp(self) -> None:
        super().setUp()
        # Initialize database.
        ccxt_ohlcv_table_query = imvccdbut.get_ccxt_ohlcv_create_table_query()
        hsql.execute_query(self.connection, ccxt_ohlcv_table_query)

    def tearDown(self) -> None:
        super().tearDown()
        # Drop table used in tests.
        ccxt_ohlcv_drop_query = "DROP TABLE IF EXISTS ccxt_ohlcv_spot;"
        hsql.execute_query(self.connection, ccxt_ohlcv_drop_query)

    def test_remove_duplicates(self) -> None:
        """
        Test if the duplicates are removed from the extracted Dataframe.
        """
        # Define the data to process.
        ccxt_ohlcv_spot = pd.DataFrame(
            data={
                "timestamp": [1636539060000, 1636539120000, 1636569000000],
                "open": [2.227, 2.226, 2.244],
                "high": [2.228, 2.228, 2.245],
                "low": [2.225, 2.225, 2.241],
                "close": [2.225, 2.227, 2.241],
                "volume": [71884.5, 64687.0, 93899.7],
                "currency_pair": ["ADA_USDT", "ADA_USDT", "ADA_USDT"],
                "exchange_id": ["binance", "binance", "binance"],
            }
        )
        # Remove duplicate entities.
        actual_df = imvcdeexut.remove_duplicates(
            db_connection=self.connection,
            data=ccxt_ohlcv_spot,
            db_table="ccxt_ohlcv_spot",
            start_timestamp_as_unix=1636539060000,
            end_timestamp_as_unix=1636539120000,
            exchange_id="binance",
            currency_pair="ADA_USDT",
        )
        # Reset index to make expected and actual Dataframes comparable.
        actual_df = actual_df.reset_index(drop=True)
        # Define the Dataframe with duplicates removed.
        expected_df = pd.DataFrame(
            data={
                "timestamp": [1636569000000],
                "open": [2.244],
                "high": [2.245],
                "low": [2.241],
                "close": [2.241],
                "volume": [93899.7],
                "currency_pair": ["ADA_USDT"],
                "exchange_id": ["binance"],
            }
        )
        # Check the result.
        hunitest.compare_df(expected_df, actual_df)


class TestVerifySchema(hunitest.TestCase):
    def test_valid_df(self) -> None:
        """
        Check if valid Dataframe schema is not changed.
        """
        # Define test Dataframe.
        test_data = {
            "timestamp": [1636539120000, 1636539180000, 1636539240000],
            "open": [2.226, 2.228, 2.23],
            "high": [2.228, 2.232, 2.233],
            "low": [2.225, 2.227, 2.23],
            "close": [2.0, 2.0, 2.0],
            "volume": [64687.0, 59076.3, 58236.2],
            "currency_pair": ["ADA_USDT", "ADA_USDT", "ADA_USDT"],
            "exchange_id": ["binance", "binance", "binance"],
        }
        # Create Dataframe.
        test_df = pd.DataFrame(data=test_data)
        # Function should not change the schema of the dataframe.
        actual_df = imvcdeexut.verify_schema(test_df, "ohlcv")
        # Check the result.
        hunitest.compare_df(test_df, actual_df)

    def test_fix_int_column(self) -> None:
        """
        Test if int column if forced to float.
        """
        # Define test Dataframe data with `close` column with type `int`.
        test_data = {
            "timestamp": [1636539120000, 1636539180000, 1636539240000],
            "open": [2.226, 2.228, 2.23],
            "high": [2.228, 2.232, 2.233],
            "low": [2.225, 2.227, 2.23],
            "close": [2, 2, 2],
            "volume": [64687.0, 59076.3, 58236.2],
            "currency_pair": ["ADA_USDT", "ADA_USDT", "ADA_USDT"],
            "exchange_id": ["binance", "binance", "binance"],
        }
        # Create Dataframe.
        test_df = pd.DataFrame(data=test_data)
        expected_df = test_df.copy()
        # Fix the type of the `close` column to `float64`.
        expected_df["close"] = expected_df["close"].astype("float64")
        # Function should fix the type of `close` column to `int`.
        actual_df = imvcdeexut.verify_schema(test_df, "ohlcv")
        # Check the result.
        hunitest.compare_df(expected_df, actual_df)

    def test_fix_int_column2(self) -> None:
        """
        Test if int64 column if forced to int32.
        """
        # Define test Dataframe data with `year` and `month` columns with type `int64`.
        test_data = {
            "timestamp": [1636539120000, 1636539180000, 1636539240000],
            "open": [2.226, 2.228, 2.23],
            "high": [2.228, 2.232, 2.233],
            "low": [2.225, 2.227, 2.23],
            "year": [2022, 2022, 2022],
            "month": [7, 7, 8],
            "currency_pair": ["ADA_USDT", "ADA_USDT", "ADA_USDT"],
            "exchange_id": ["binance", "binance", "binance"],
        }
        # Create Dataframe.
        test_df = pd.DataFrame(data=test_data)
        expected_df = test_df.copy()
        # Fix the type of the `month` and `year` columns to `int32`.
        expected_df["year"] = expected_df["year"].astype("int32")
        expected_df["month"] = expected_df["month"].astype("int32")
        # Function should fix the type of the columns to `int32`.
        actual_df = imvcdeexut.verify_schema(test_df, "ohlcv")
        # Check the result.
        hunitest.compare_df(expected_df, actual_df)

    def test_numerical_column(self) -> None:
        """
        Test if object typed numerical column is forced to float.
        """
        # Define test Dataframe data with non-numerical `close` column.
        test_data = {
            "timestamp": [1636539120000, 1636539180000, 1636539240000],
            "open": [2.226, 2.228, 2.23],
            "high": [2.228, 2.232, 2.233],
            "low": [2.225, 2.227, 2.23],
            "close": ["2", "2", "2"],
            "volume": [64687.0, 59076.3, 58236.2],
            "currency_pair": ["ADA_USDT", "ADA_USDT", "ADA_USDT"],
            "exchange_id": ["binance", "binance", "binance"],
        }
        # Create Dataframe.
        test_df = pd.DataFrame(data=test_data)
        expected_df = test_df.copy()
        # Fix the type of `close` column to `int32`.
        expected_df["close"] = expected_df["close"].astype("float64")
        # Function should fix the type of the column to `float64`.
        actual_df = imvcdeexut.verify_schema(test_df, "ohlcv")
        # Check the result.
        hunitest.compare_df(expected_df, actual_df)

    def test_non_numerical_column(self) -> None:
        """
        Test if non numerical column that supposed to be numerical produces an
        error.
        """
        # Define test Dataframe data with non-numerical `close` column.
        test_data = {
            "timestamp": [1636539120000, 1636539180000, 1636539240000],
            "open": [2.226, 2.228, 2.23],
            "high": [2.228, 2.232, 2.233],
            "low": [2.225, 2.227, 2.23],
            "close": ["two", "two", "two"],
            "volume": [64687.0, 59076.3, 58236.2],
            "currency_pair": ["ADA_USDT", "ADA_USDT", "ADA_USDT"],
            "exchange_id": ["binance", "binance", "binance"],
        }
        # Create Dataframe.
        test_df = pd.DataFrame(data=test_data)
        # Make sure function raises an error.
        with self.assertRaises(AssertionError) as cm:
            imvcdeexut.verify_schema(test_df, "ohlcv")
        actual = str(cm.exception)
        expected = """
            Invalid dtype of `close` column: expected type `float64`, found `object`
        """
        self.assert_equal(actual, expected, fuzzy_match=True)
