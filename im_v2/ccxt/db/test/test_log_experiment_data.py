import argparse
import os
import unittest.mock as umock

import pandas as pd
import pytest

import helpers.hdatetime as hdateti
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import im_v2.ccxt.db.log_experiment_data as imvcdlexda


class Test_log_experiment_data(hunitest.TestCase):
    mock_data_reader = umock.MagicMock()
    mock_load_db_table = mock_data_reader.load_db_table
    data_reader_patch = umock.patch(
        "im_v2.common.data.client.im_raw_data_client.get_bid_ask_realtime_raw_data_reader",
        return_value=mock_data_reader,
    )

    def _get_test_args(
        self, start_timestamp: str, end_timestamp: str, log_dir: str, exchange_id: str
    ) -> argparse.Namespace:
        """
        Create and return an argument namespace with the given parameters.
        """
        return argparse.Namespace(
            start_timestamp_as_str=start_timestamp,
            end_timestamp_as_str=end_timestamp,
            db_stage="preprod",
            universe="v8",
            data_vendor="CCXT",
            log_level="INFO",
            log_dir=log_dir,
            exchange_id=exchange_id
        )

    def get_log_file(self, dst_dir: str, index_col: str = None) -> pd.DataFrame:
        """
        Retrieve and return the log file from the specified directory.
        """
        log_file_path = os.path.join(dst_dir, "bid_ask_full")
        log_files = os.listdir(log_file_path)
        log_file = os.path.join(log_file_path, log_files[0])
        actual = pd.read_csv(log_file, index_col=index_col)
        actual = hpandas.df_to_str(actual)
        return actual

    # This will be run before and after each test.
    @pytest.fixture(autouse=True)
    def setup_teardown_test(self) -> None:
        # Run before each test.
        self.set_up_test()
        yield
        # Run after each test.
        self.tear_down_test()

    def set_up_test(self) -> None:
        # Create new mocks from patch's start() method.
        self.bid_ask_data_reader_mock: umock.MagicMock = (
            self.mock_data_reader.start()
        )
        self.data_reader_patch.start()

    def tear_down_test(self) -> None:
        self.mock_data_reader.stop()
        self.data_reader_patch.stop()

    def test1(self) -> None:
        """
        Check that the log_experiment_data correctly logs using default
        parameters.
        """
        dst_dir = os.path.join(self.get_scratch_space(), "tmp.log_experiment")
        start_ts = "20240220_124800"
        end_ts = "20240220_131500"
        index_col = "timestamp"
        exchange_id = "binance"
        args = self._get_test_args(start_ts, end_ts, dst_dir, exchange_id)
        # Prepare mock result.
        expected = pd.DataFrame(
            {
                "timestamp": ["2024-02-20 12:48:00", "2024-02-20 12:49:00"],
                "currency_pair": ["BTC_USDT", "BTC_USDT"],
                "bid_price": [50000, 50010],
                "bid_size": [1.0, 2.0],
                "ask_price": [50005, 50015],
                "ask_size": [1.0, 2.0],
                "level": [1, 1],
            }
        ).set_index(index_col)
        self.mock_load_db_table.return_value = expected
        # Test method.
        imvcdlexda._log_experiment_data(args)
        # Verify the contents of the logged file.
        actual = self.get_log_file(dst_dir, index_col)
        expected = hpandas.df_to_str(expected)
        self.assert_equal(actual, expected, fuzzy_match=True)
        # Verify the arguments for load_db_table.
        start_timestamp = hdateti.timestamp_as_str_to_timestamp(start_ts)
        end_timestamp = hdateti.timestamp_as_str_to_timestamp(end_ts)
        self.mock_load_db_table.assert_called_once_with(
            start_timestamp,
            end_timestamp,
            bid_ask_levels=[1],
            deduplicate=True,
            subset=[
                "timestamp",
                "currency_pair",
                "bid_price",
                "bid_size",
                "ask_price",
                "ask_size",
                "level",
            ],
        )

    def test2(self) -> None:
        """
        Test how the function handles empty or None values for start and end
        timestamps.
        """
        dst_dir = os.path.join(self.get_scratch_space(), "tmp.log_experiment")
        # Prepare data.
        args = self._get_test_args("", "", dst_dir, "binance")
        # Test method.
        with self.assertRaises(ValueError) as cm:
            imvcdlexda._log_experiment_data(args)
        # Verify result.
        expected = "Unknown datetime string format, unable to parse: +00:00"
        actual = str(cm.exception)
        self.assert_equal(actual, expected)

    def test3(self) -> None:
        """
        Test the function when load_db_data return empty dataframe.
        """
        dst_dir = os.path.join(self.get_scratch_space(), "tmp.log_experiment")
        # Prepare data.
        args = self._get_test_args("20240220_124800", "20240220_131500", dst_dir, "binance")
        self.mock_load_db_table.return_value = pd.DataFrame()
        # Test method.
        imvcdlexda._log_experiment_data(args)
        # Verify the contents of the logged file.
        expected = r"""
        Empty DataFrame
        Columns: [Unnamed: 0]
        Index: []
        """
        actual = self.get_log_file(dst_dir)
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test4(self) -> None:
        """
        Test the function when load_db_table returns data with missing bid/ask
        price.
        """
        dst_dir = os.path.join(self.get_scratch_space(), "tmp.log_experiment")
        args = self._get_test_args("20240220_124800", "20240220_131500", dst_dir, "cryptocom")
        index_col = "timestamp"
        # Prepare mock result.
        expected = pd.DataFrame(
            {
                "timestamp": ["2024-02-20 12:48:00", "2024-02-20 12:49:00"],
                "currency_pair": ["BTC_USDT", "BTC_USDT"],
                "bid_price": [50000, None],
                "bid_size": [1.0, 2.0],
                "ask_price": [None, 50015],
                "ask_size": [1.0, 2.0],
                "level": [1, 1],
            }
        ).set_index(index_col)
        self.mock_load_db_table.return_value = expected
        # Test method.
        imvcdlexda._log_experiment_data(args)
        # Verify the contents of the logged file.
        actual = self.get_log_file(dst_dir, index_col)
        expected = hpandas.df_to_str(expected)
        self.assert_equal(actual, expected, fuzzy_match=True)
