import unittest.mock as umock

import pandas as pd
import psycopg2 as psycop
import pytest

import helpers.hunit_test as hunitest
import helpers.hdatetime as hdateti
import im_v2.common.db.db_utils as imvcddbut

DB_STAGE = "test"

class TestLoadDBData(hunitest.TestCase):
    def setUp(self):
        """
        Set up common attributes for the test cases.
        """
        super().setUp()
        self.db_connection = umock.MagicMock()
        self.src_table = "test_table"
        self.start_date = pd.Timestamp("2024-01-01")
        self.end_date = pd.Timestamp("2024-01-31")
        self.start_ts = hdateti.convert_timestamp_to_unix_epoch(self.start_date, unit="ms")
        self.end_ts = hdateti.convert_timestamp_to_unix_epoch(self.end_date, unit="ms")


    def test_load_db_data(self):
        """
        Test if the query construction is done correctly when all parameters are provided.
        """
        # Prepare mock data
        currency_pairs = ["BTC_USDT"]
        limit = 100
        bid_ask_levels = [1, 2]
        exchange_id = "Exchange1"
        time_interval_closed = True
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            result = imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_date, self.end_date,
                                            currency_pairs=currency_pairs, limit=limit,
                                            bid_ask_levels=bid_ask_levels, exchange_id=exchange_id,
                                            time_interval_closed=time_interval_closed)
            expected_query = (
                f"SELECT * FROM {self.src_table} WHERE timestamp >= {self.start_ts} AND timestamp <= {self.end_ts} "
                f"AND currency_pair IN ('BTC_USDT') AND level IN (1, 2) AND exchange_id = 'Exchange1' "
                f"ORDER BY timestamp DESC LIMIT 100"
            )
            # Assert that an expected_query is called.
            mock_execute_query_to_df.assert_called_once_with(self.db_connection, expected_query)
        
    def test_load_db_data1(self):
        """
        Test if the function behaves correctly when no data is returned from the database query.
        """
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.return_value = None
            result = imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_date, self.end_date)
            expected_query = (
            f"SELECT * FROM {self.src_table} WHERE timestamp >= {self.start_ts} AND timestamp <= {self.end_ts}"
            )
            mock_execute_query_to_df.assert_called_once_with(self.db_connection, expected_query)
            # Assert that function is returning None.
            self.assertIsNone(result)
    
    def test_load_db_data2(self):
        """
        Test if the function returns an empty dataframe when no data is found in the database.
        """
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.return_value = pd.DataFrame()
            result = imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_date, self.end_date)
            # Assert that function is returning empty dataframe
            self.assertTrue(result.empty)

    def test_load_db_data3(self):
        """
        Test if the function handles the scenario where the start timestamp is greater than the end timestamp.
        """
        # Swap start_ts and end_ts
        start_ts, end_ts = self.end_date, self.start_date
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.side_effect = ValueError
            # Assert that function is raising ValueError
            with self.assertRaises(ValueError): 
                result = imvcddbut.load_db_data(self.db_connection, self.src_table, start_ts, end_ts) 

    def test_load_db_data4(self):
        """
        Test if the function raises a ValueError when invalid currency pairs are provided.
        """
        # Invalid currency pairs
        invalid_currency_pairs = None
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.side_effect = ValueError
            # Assert that function is raising ValueError
            with self.assertRaises(ValueError):
                mock_execute_query_to_df.side_effect = ValueError
                imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_date, self.end_date, currency_pairs=invalid_currency_pairs)

    def test_load_db_data5(self):
        """
        Test if the function raises a ValueError when null parameters are provided for start timestamp.
        """
        # Null timestamp
        start_ts = None
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.side_effect = ValueError
            # Assert that function is raising ValueError
            with self.assertRaises(ValueError):
                imvcddbut.load_db_data(self.db_connection, self.src_table, start_ts, self.end_date)

    def test_load_db_data6(self):
        """
        Test if the function raises a AttributeError when an invalid timestamp format is provided.
        """
        # Invalid timestamp format
        invalid_ts = "invalid_timestamp"
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.side_effect = AttributeError
            # Assert that function is raising AttributeError
            with self.assertRaises(AttributeError):
                imvcddbut.load_db_data(self.db_connection, self.src_table, invalid_ts, self.end_date)
    
    def test_load_db_data7(self):
        """
        Test if functions returns ValueError when Limit parameter is larger than total number of rows returned by database query
        """
        # Mocking execute_query_to_df method
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            mock_execute_query_to_df.return_value = pd.DataFrame()
            mock_execute_query_to_df.side_effect = ValueError
            # Limit is set to very large value
            limit = 1000000
            # Assert that a ValueError is raised
            with self.assertRaises(ValueError):
                imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_date, self.end_date, limit=limit)



class TestDbConnectionManager(hunitest.TestCase):
    # This will be run before and after each test.
    @pytest.fixture(autouse=True)
    def setup_teardown_test(self):
        # Run before each test.
        self.set_up_test()
        yield

    def set_up_test(self) -> None:
        self.mock_cursor = umock.MagicMock()
        self.mock_cursor.execute = umock.MagicMock(return_value=None)
        self.mock_connection = umock.MagicMock()
        self.mock_connection.cursor = umock.MagicMock(
            return_value=self.mock_cursor
        )
        self.mock_get_connection_from_env_vars = umock.MagicMock(
            return_value=self.mock_connection
        )
        imvcddbut.hsql.get_connection_from_env_vars = (
            self.mock_get_connection_from_env_vars
        )

    def test_get_connection1(self) -> None:
        """
        Test the `get_connection` method when some exceptions are raised.
        """
        # Test the case when the connection is valid.
        imvcddbut.DbConnectionManager.get_connection(DB_STAGE)
        self.mock_get_connection_from_env_vars.assert_called_once()
        # Test the case when during the first attempt to get the connection
        # an exception that was expected is raised.
        self.mock_cursor.execute = umock.MagicMock(
            side_effect=psycop.OperationalError("Mysterious expected error")
        )
        imvcddbut.DbConnectionManager.get_connection(DB_STAGE)
        self.assertEqual(self.mock_get_connection_from_env_vars.call_count, 2)
        # Test the case when during the first attempt to get the connection
        # an exception that was not expected is raised.
        self.mock_cursor.execute = umock.MagicMock(
            side_effect=AttributeError("Mysterious unexpected error")
        )
        with self.assertRaises(AttributeError):
            imvcddbut.DbConnectionManager.get_connection(DB_STAGE)


class TestSaveDataToDb(hunitest.TestCase):
    # This will be run before and after each test.
    @pytest.fixture(autouse=True)
    def setup_teardown_test(self):
        # Run before each test.
        self.set_up_test()
        yield

    def set_up_test(self) -> None:
        self.mock_execute_insert_on_conflict_do_nothing_query = umock.MagicMock()
        imvcddbut.hsql.execute_insert_on_conflict_do_nothing_query = (
            self.mock_execute_insert_on_conflict_do_nothing_query
        )

    def test_save_data_to_db1(self) -> None:
        """
        Test the `save_data_to_db` method for the case when some expected
        exceptions are raised, the number of retries is not exceeded and the
        query is executed.
        """
        # Mock results of the `execute_insert_on_conflict_do_nothing_query`
        # function. The first N-1 results are exceptions that are expected to
        # be raised. The last result is None to simulate the case when the
        # query is executed successfully.
        mocked_results = [
            imvcddbut.RETRY_EXCEPTION[0]("DB is down")
            for _ in range(imvcddbut.NUMBER_OF_RETRIES_TO_SAVE - 1)
        ]
        mocked_results += [None]
        self.mock_execute_insert_on_conflict_do_nothing_query.side_effect = (
            mocked_results
        )
        self._call_save_data_to_db()
        # Check that the query was executed the expected number of times.
        self.assertEqual(
            self.mock_execute_insert_on_conflict_do_nothing_query.call_count,
            imvcddbut.NUMBER_OF_RETRIES_TO_SAVE,
        )

    def test_save_data_to_db2(self) -> None:
        """
        Test the `save_data_to_db` method for the case when some unexpected
        exceptions are raised.
        """
        # Mock results of the `execute_insert_on_conflict_do_nothing_query`
        # function. There are unexpected exceptions in the first N-1 results.
        mocked_results = [
            AttributeError("Unexpected error")
            for _ in range(imvcddbut.NUMBER_OF_RETRIES_TO_SAVE - 1)
        ]
        self.mock_execute_insert_on_conflict_do_nothing_query.side_effect = (
            mocked_results
        )
        # Check that exception is raised when the exception is not expected.
        with self.assertRaises(AttributeError):
            self._call_save_data_to_db()

    def test_save_data_to_db3(self) -> None:
        """
        Test the `save_data_to_db` method for the case when some expected
        exceptions are raised, the number of retries is exceeded and exception
        is not caught.
        """
        # Mock results of the `execute_insert_on_conflict_do_nothing_query`
        mocked_results = [
            imvcddbut.RETRY_EXCEPTION[0]("DB is down")
            for _ in range(imvcddbut.NUMBER_OF_RETRIES_TO_SAVE)
        ]
        self.mock_execute_insert_on_conflict_do_nothing_query.side_effect = (
            mocked_results
        )
        # Check that exception is raised when the number of retries exceeded.
        with self.assertRaises(imvcddbut.RETRY_EXCEPTION[0]):
            self._call_save_data_to_db()

    def _call_save_data_to_db(self) -> None:
        """
        Call the `save_data_to_db` method with the stub data.
        """
        # Prepare the stub data.
        data = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        data_type = "bid_ask"
        db_connection = umock.create_autospec(imvcddbut.hsql.DbConnection)
        db_table = "test_table"
        time_zone = "UTC"
        # Call the method under test.
        imvcddbut.save_data_to_db(
            data, data_type, db_connection, db_table, time_zone
        )
