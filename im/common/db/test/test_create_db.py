import logging

from psycopg2.errors import InvalidCatalogName

import helpers.sql as hsql
import helpers.system_interaction as hsyint
import helpers.unit_test as huntes
import im.common.db.create_db as imcodbcrdb

_LOG = logging.getLogger(__name__)


class TestCreateDB(huntes.TestCase):
    def setUp(self):
        """
        Bring up the DB.
        """

        super().setUp()
        cmd = "sudo docker-compose --file im/devops/compose/docker-compose.yml up -d im_postgres_local"
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
        Bring up the DB.
        """
        cmd = "sudo docker-compose --file im/devops/compose/docker-compose.yml down -v"
        self.connection.close()
        hsyint.system(cmd, suppress_output=False)
        super().tearDown()

    def test_create_all_tables1(self):
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
        db_names = hsql.get_db_names(self.connection)
        before = len(db_names)
        #later change to env
        db_names.remove('im_postgres_db_local')
        if db_names:
            imcodbcrdb.remove_database(self.connection, db_names[0])
            after = len(hsql.get_db_names(self.connection))
            self.assertLess(after, before)

    def test_remove_database_invalid(self):
        with self.assertRaises(InvalidCatalogName):
            imcodbcrdb.remove_database(self.connection, "db does not exist")
