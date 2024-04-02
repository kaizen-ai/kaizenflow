import unittest.mock as umock

import pandas as pd
import psycopg2 as psycop
import pytest

import helpers.hunit_test as hunitest
import im_v2.common.db.db_utils as imvcddbut

DB_STAGE = "test"

class TestLoadDBData(hunitest.TestCase):
    def setUp(self):
        """
        Set up common attributes for the test cases.
        """
        db_connection = umock.MagicMock()
        src_table = "test_table"
        start_ts = pd.Timestamp("2024-01-01")
        end_ts = pd.Timestamp("2024-01-31")

    @pytest.fixture
    def mock_execute_query_to_df(self):
        """
        Fixture for mocking execute_query_to_df function.
        """
        with umock.patch('im_v2.common.db.db_utils.hsql.execute_query_to_df') as mock_execute_query_to_df:
            yield mock_execute_query_to_df

    def test_load_db_data(self, mock_execute_query_to_df):
        """
        Test case : Basic query construction with all the parameters provided.

        This test case checks if the query construction is done correctly when all parameters are provided.
        """
        # Prepare mock data
        currency_pairs = ["BTC_USDT"]
        limit = 100
        bid_ask_levels = [1, 2]
        exchange_id = "Exchange1"
        time_interval_closed = True
        # Test the load_db_data method by passing mock db_connection object.
        result = imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_ts, self.end_ts,
                                       currency_pairs=currency_pairs, limit=limit,
                                       bid_ask_levels=bid_ask_levels, exchange_id=exchange_id,
                                       time_interval_closed=time_interval_closed)
        expected_query = (
            f"SELECT * FROM {self.src_table} WHERE timestamp >= {self.start_ts.value} AND timestamp <= {self.addClassCleanupend_ts.value} "
            f"AND currency_pair IN ('BTC_USDT') AND level IN (1, 2) AND exchange_id = 'Exchange1' "
            f"ORDER BY timestamp DESC LIMIT 100"
        )
        mock_execute_query_to_df.assert_called_once_with(self.db_connection, expected_query)
        
    def test_load_db_data1(self, mock_execute_query_to_df):
        """
        Test case: Handling the scenario where no data is returned from the database query.

        This test checks if the function behaves correctly when no data is returned from the database query.
        """
        # Prepare mock data
        mock_execute_query_to_df.return_value = None
        result = imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_ts, self.end_ts)
        expected_query = (
            f"SELECT * FROM {self.src_table} WHERE timestamp >= {self.start_ts.value} AND timestamp <= {self.end_ts.value} "
        )
        mock_execute_query_to_df.assert_called_once_with(self.db_connection, expected_query)
        self.assertIsNone(result)
    
    def test_load_db_data2(self, mock_execute_query_to_df):
        """
        Test case: Returning an empty dataframe

        This test checks if the function returns an empty dataframe when no data is found in the database.
        """
        # Prepare mock data
        mock_execute_query_to_df.return_value = pd.DataFrame()
        result = imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_ts, self.end_ts)
        self.assertTrue(result.empty)

    def test_load_db_data3(self, mock_execute_query_to_df):
        """
        Test case: start_ts > end_ts

        This test checks if the function handles the scenario where the start timestamp is greater than the end timestamp.
        """
        # Swap start_ts and end_ts
        start_ts, end_ts = self.end_ts, self.start_ts  
        result = imvcddbut.load_db_data(self.db_connection, self.src_table, start_ts, end_ts)
        # Assert that the query is not executed
        mock_execute_query_to_df.assert_not_called()
        # Assert that the result is None
        self.assertIsNone(result) 

    def test_load_db_data4(self, mock_execute_query_to_df):
        """
        Test case: Invalid currency pairs

        This test checks if the function raises a ValueError when invalid currency pairs are provided.
        """
        # Invalid currency pairs
        invalid_currency_pairs = None
        # Assert that a ValueError is raised
        with self.assertRaises(ValueError):
            imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_ts, self.end_ts, currency_pairs=invalid_currency_pairs)

    def test_load_db_data5(self, mock_execute_query_to_df):
        """
        Test case: Null parameter for start_ts

        This test checks if the function raises a ValueError when null parameters are provided for start timestamp.
        """
        # Null timestamp
        start_ts = None
        # Assert that ValueError is raised
        with self.assertRaises(ValueError):
            imvcddbut.load_db_data(self.db_connection, self.src_table, start_ts, self.end_ts)

    def test_load_db_data6(self, mock_execute_query_to_df):
        """
        Test case: Invalid timestamp format

        This test checks if the function raises a TypeError when an invalid timestamp format is provided.
        """
        # Invalid timestamp format
        invalid_ts = "invalid_timestamp"
        # Assert that a TypeError is raised
        with self.assertRaises(TypeError):
            imvcddbut.load_db_data(self.db_connection, self.src_table, invalid_ts, self.end_ts)
    
    def test_load_db_data7(self, mock_execute_query_to_df):
        """
        Test case: Limit parameter is larger than total number of rows returned by database query

        This test checks if the function raises a ValueError when the limit parameter is larger than the total number of rows returned by the database query
        """
        # Empty DataFrame returned from query
        mock_execute_query_to_df.return_value = pd.DataFrame()
        # Limit is set to very large value
        limit = 1000000
        # Assert that a ValueError is raised
        with self.assertRaises(ValueError):
            imvcddbut.load_db_data(self.db_connection, self.src_table, self.start_ts, self.end_ts, limit=limit)



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
