import logging
import os

import pytest

import helpers.git as hgit
import helpers.sql as hsql
import helpers.system_interaction as hsysinte
import helpers.unit_test as hunitest
import im.common.db.create_db as imcdbcrdb

_LOG = logging.getLogger(__name__)


class TestCreateDb1(hunitest.TestCase):
    def setUp(self) -> None:
        """
        Initialize the test database inside test container.
        """
        super().setUp()
        self.docker_compose_file_path = os.path.join(
            hgit.get_amp_abs_path(), "im_v2/devops/compose/docker-compose.yml"
        )
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} "
            "up -d im_postgres_local"
        )
        hsysinte.system(cmd, suppress_output=False)
        host = "localhost"
        dbname = "im_postgres_db_local"
        port = 5432
        user = "aljsdalsd"
        password = "alsdkqoen"
        hsql.wait_db_connection(host, dbname, port)
        self.connection = hsql.get_connection(
            host,
            dbname,
            port,
            user,
            password,
            autocommit=True,
        )

    def tearDown(self) -> None:
        """
        Bring down the test container.
        """
        cmd = (
            "sudo docker-compose "
            f"--file {self.docker_compose_file_path} down -v"
        )
        self.connection.close()
        hsysinte.system(cmd, suppress_output=False)
        super().tearDown()

    @pytest.mark.slow()
    def test_up1(self) -> None:
        """
        Verify that the DB is up.
        """
        db_list = hsql.get_db_names(self.connection)
        _LOG.info("db_list=%s", db_list)

    @pytest.mark.slow()
    def test_create_all_tables1(self) -> None:
        """
        Verify that all necessary tables are created inside the DB.
        """
        imcdbcrdb.create_all_tables(self.connection)
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
    def test_create_im_database(self) -> None:
        imcdbcrdb.create_im_database(connection=self.connection, new_db="test_db")
        db_list = hsql.get_db_names(self.connection)
        self.assertIn("test_db", db_list)
