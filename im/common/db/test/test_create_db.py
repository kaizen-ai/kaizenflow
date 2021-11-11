import logging
import os

import psycopg2.errors as perrors

import helpers.git as hgit
import helpers.sql as hsql
import helpers.system_interaction as hsyint
import helpers.unit_test as huntes
import im.common.db.create_db as imcodbcrdb
import pytest

_LOG = logging.getLogger(__name__)


class TestCreateDB(huntes.TestCase):
    def setUp(self):
        """
        Initialize the test database inside test container 
        """
        super().setUp()
        self.docker_compose_file_path = os.path.join(
            hgit.get_amp_abs_path(),
            "im/devops/compose/docker-compose.yml"
        )
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} "
            "up -d im_postgres_local"
        )
        hsyint.system(cmd, suppress_output=False)
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        password = "alsdkqoen"
        user = "aljsdalsd"
        hsql.check_db_connection(dbname, port, host)
        self.connection, _ = hsql.get_connection(
            dbname,
            host,
            user,
            port,
            password,
            autocommit=True,
        )

    def tearDown(self):
        """
        Bring down the database inside the test container.
        """
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} down -v"
        )
        self.connection.close()
        hsyint.system(cmd, suppress_output=False)
        super().tearDown()

    @pytest.mark.slow()
    def test_create_all_tables1(self):
        """
        Verify that all necessary tables are created inside the DB.
        """
        imcodbcrdb.create_all_tables(self.connection)
        expected = sorted(
            [
                "ccxt_ohlcv",
                "currency_pair",
                "exchange",
                "exchange_name",
                "ib_daily_data",
                "ib_minute_data",
                "ib_tick_bid_ask_data",
                "ib_tick_data",
                "kibot_daily_data",
                "kibot_minute_data",
                "kibot_tick_bid_ask_data",
                "kibot_tick_data",
                "symbol",
                "trade_symbol",
            ]
        )
        actual = sorted(hsql.get_table_names(self.connection))
        self.assertEqual(actual, expected)

    @pytest.mark.slow()
    def test_remove_database(self):
        """
        Create database 'test_db_to_remove' and remove it.
        """
        imcodbcrdb.create_database(
                self.connection,
                new_db="test_db_to_remove",
        )
        imcodbcrdb.remove_database(self.connection, "test_db_to_remove")
        db_list = hsql.get_db_names(self.connection)
        self.assertNotIn("test_db_to_remove", db_list)

    def test_remove_database_invalid(self):
        """
        Test failed assertion for passing db name that does not exist.
        """
        with self.assertRaises(perrors.InvalidCatalogName):
            imcodbcrdb.remove_database(self.connection, "db does not exist")
