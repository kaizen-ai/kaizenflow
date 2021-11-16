import logging
import os

import pandas as pd
import psycopg2.errors as perrors
import pytest

import helpers.git as hgit
import helpers.sql as hsql
import helpers.system_interaction as hsyint
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


# @pytest.mark.skipif(not hgit.is_amp(), reason="Only run in amp")
class TestSql1(huntes.TestCase):
    def setUp(self) -> None:
        """
        Initialize the test container.
        """
        super().setUp()
        self.docker_compose_file_path = os.path.join(
            hgit.get_amp_abs_path(), "im/devops/compose/docker-compose.yml"
        )
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} "
            "up -d im_postgres_local"
        )
        hsyint.system(cmd, suppress_output=False)
        # Set DB credentials.
        self.dbname = "im_postgres_db_local"
        self.host = "localhost"
        self.port = 5432
        self.password = "alsdkqoen"
        self.user = "aljsdalsd"

    def tearDown(self) -> None:
        """
        Bring down the test container.
        """
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} down -v"
        )
        hsyint.system(cmd, suppress_output=False)

        super().tearDown()

    @pytest.mark.slow()
    def test_waitdb(self) -> None:
        """
        Smoke test.
        """
        # TODO(Dan3): change to env
        hsql.wait_db_connection(self.dbname, self.port, self.host)

    @pytest.mark.slow()
    def test_db_connection_to_tuple(self) -> None:
        """
        Verify that connection string is correct.
        """
        hsql.wait_db_connection(self.dbname, self.port, self.host)
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        actual_details = hsql.db_connection_to_tuple(self.connection)
        expected = {
            "dbname": self.dbname,
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
        }
        self.assertEqual(actual_details._asdict(), expected)

    @pytest.mark.slow()
    def test_create_database(self) -> None:
        """
        Verify that db is creating.
        """
        hsql.wait_db_connection(self.dbname, self.port, self.host)
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        hsql.create_database(self.connection, dbname="test_db")
        self.assertIn("test_db", hsql.get_db_names(self.connection))

    def test_create_insert_query(self) -> None:
        """
        Verify that query is correct.
        """
        self._create_test_table()
        test_data = self._get_test_data()
        actual_query = hsql._create_insert_query(test_data, "test_table")
        self.check_string(actual_query)

    @pytest.mark.slow()
    def test_execute_insert_query1(self) -> None:
        """
        Verify that dataframe insertion is correct.
        """
        self._create_test_table()
        test_data = self._get_test_data()
        # Try uploading test data.
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        hsql.execute_insert_query(self.connection, test_data, "test_table")
        # Load data.
        df = hsql.execute_query(self.connection, "SELECT * FROM test_table")
        actual = huntes.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    @pytest.mark.slow()
    def test_copy_rows_with_copy_from1(self) -> None:
        """
        Verify that dataframe insertion via buffer is correct.
        """
        self._create_test_table()
        test_data = self._get_test_data()
        # Try uploading test data.
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        hsql.copy_rows_with_copy_from(self.connection, test_data, "test_table")
        # Load data.
        df = hsql.execute_query(self.connection, "SELECT * FROM test_table")
        actual = huntes.convert_df_to_json_string(df, n_tail=None)
        self.check_string(actual)

    @pytest.mark.slow()
    def test_remove_database1(self) -> None:
        """
        Create database 'test_db_to_remove' and remove it.
        """
        hsql.wait_db_connection(self.dbname, self.port, self.host)
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        hsql.create_database(
            self.connection,
            dbname="test_db_to_remove",
        )
        hsql.remove_database(self.connection, "test_db_to_remove")
        db_list = hsql.get_db_names(self.connection)
        self.assertNotIn("test_db_to_remove", db_list)

    @pytest.mark.slow()
    def test_remove_database_invalid(self) -> None:
        """
        Test failed assertion for passing db name that does not exist.
        """
        hsql.wait_db_connection(self.dbname, self.port, self.host)
        self.connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        with self.assertRaises(perrors.InvalidCatalogName):
            hsql.remove_database(self.connection, "db does not exist")

    def _create_test_table(self) -> None:
        """
        Create a test table.
        """
        query = """CREATE TABLE IF NOT EXISTS test_table(
                    id SERIAL PRIMARY KEY,
                    column_1 NUMERIC,
                    column_2 VARCHAR(255)
                    )
                    """
        hsql.wait_db_connection(self.dbname, self.port, self.host)
        connection, _ = hsql.get_connection(
            self.dbname,
            self.host,
            self.user,
            self.port,
            self.password,
            autocommit=True,
        )
        connection.cursor().execute(query)

    def _get_test_data(self) -> pd.DataFrame:
        test_data = pd.DataFrame(
            columns=["id", "column_1", "column_2"],
            data=[
                [
                    1,
                    1000,
                    "test_string_1",
                ],
                [
                    2,
                    1001,
                    "test_string_2",
                ],
                [
                    3,
                    1002,
                    "test_string_3",
                ],
                [
                    4,
                    1003,
                    "test_string_4",
                ],
                [
                    5,
                    1004,
                    "test_string_5",
                ],
            ],
        )
        return test_data
