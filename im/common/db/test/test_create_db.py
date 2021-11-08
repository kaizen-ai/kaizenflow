import logging

import psycopg2.errors as perrors

import helpers.sql as hsql
import helpers.system_interaction as hsyint
import helpers.unit_test as huntes
import im.common.db.create_db as imcodbcrdb

_LOG = logging.getLogger(__name__)


class TestCreateDB(huntes.TestCase):
    def setUp(self):
        """
        Initialize the test database inside test container 
        """
        super().setUp()
        cmd = ("sudo docker-compose "
               "--file im/devops/compose/docker-compose.yml "
               "up -d im_postgres_local")
        hsyint.system(cmd, suppress_output=False)
        dbname = "im_postgres_db_local"
        host = "localhost"
        port = 5432
        password = "alsdkqoen"
        user = "aljsdalsd"
        hsql.check_db_connection(dbname, port, host)
        #TODO(Dan3): Find a way to pass the envfile to docker-compose.
        # self.connection, _ = hsql.get_connection_from_env_vars()
        self.connection, _ = hsql.get_connection(
            dbname,
            host,
            user,
            port,
            password,
            autocommit=True,
        )
        _LOG.info("done...")

    def tearDown(self):
        """
        Kill the test database.
        """
        cmd = ("sudo docker-compose "
               "--file im/devops/compose/docker-compose.yml down -v")
        self.connection.close()
        hsyint.system(cmd, suppress_output=False)
        super().tearDown()

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

    def test_remove_database1(self):
        """
        Verify that random database removed.
        """
        db_names = hsql.get_db_names(self.connection)
        initial_db_num = len(db_names)
        #TODO(Dan3): Change to using the env file.
        db_names.remove('im_postgres_db_local')
        if db_names:
            imcodbcrdb.remove_database(self.connection, db_names[0])
            result_db_num = len(hsql.get_db_names(self.connection))
            self.assertLess(result_db_num, initial_db_num)

    def test_remove_database2(self):
        """
        Create database 'test_db_to_remove' and removing it.
        """
        imcodbcrdb.create_database(self.connection, db="test_db_to_remove", force=force)
        db_list_before =  hsql.get_db_names(self.connection)
        is_included_before = "test_db_to_remove" in db_list_before
        imcodbcrdb.remove_database(self.connection, "test_db_to_remove")
        db_list_after = hsql.get_db_names(self.connection)
        is_excluded_after = "test_db_to_remove" not in db_list_after
        self.assertEqual(is_included_before, is_excluded_after)

    def test_remove_database_invalid(self):
        """
        Test failed assertion for passing db name that don't exist.
        """
        with self.assertRaises(perrors.InvalidCatalogName):
            imcodbcrdb.remove_database(self.connection, "db does not exist")
