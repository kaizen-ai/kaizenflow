import logging
import os

import pandas as pd
import pytest

import helpers.git as hgit
import helpers.sql as hsql
import helpers.system_interaction as hsyint
import helpers.unit_test as huntes
import im.ccxt.db.utils as imccdbuti

_LOG = logging.getLogger(__name__)


@pytest.mark.skipif(not hgit.is_amp(), reason="Only run in amp")
class TestUtils(huntes.TestCase):
    def setUp(self) -> None:
        """
        Initialize the test database inside test container.
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
        self.connection, self.cursor = hsql.get_connection(
            dbname,
            host,
            user,
            port,
            password,
            autocommit=True,
        )

        self.df_to_insert = pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "currency_pair",
                "exchange_id",
            ],
            data=[
                [
                    1,
                    1631145600000,
                    30.0,
                    40.0,
                    50.0,
                    60.0,
                    70.0,
                    "BTC/USDT",
                    "binance",
                ],
                [
                    2,
                    1631145660000,
                    31.0,
                    41.0,
                    51.0,
                    61.0,
                    71.0,
                    "BTC/USDT",
                    "binance",
                ],
                [
                    3,
                    1631145720000,
                    32.0,
                    42.0,
                    52.0,
                    62.0,
                    72.0,
                    "ETH/USDT",
                    "binance",
                ],
                [
                    4,
                    1631145780000,
                    33.0,
                    43.0,
                    53.0,
                    63.0,
                    73.0,
                    "BTC/USDT",
                    "kucoin",
                ],
                [
                    5,
                    1631145840000,
                    34.0,
                    44.0,
                    54.0,
                    64.0,
                    74.0,
                    "BTC/USDT",
                    "binance",
                ],
            ],
        )

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

    def test_copy_rows_with_copy_from1(self) -> None:
        """
        Verify that dataframe insertion via buffer is correct.
        """
        self.cursor.execute(imccdbuti.get_ccxt_ohlcv_create_table_query())
        imccdbuti.copy_rows_with_copy_from(
            self.connection, self.df_to_insert, "ccxt_ohlcv"
        )
        df = hsql.execute_query(self.connection, "SELECT * FROM ccxt_ohlcv")
        actual = huntes.convert_df_to_json_string(df)
        self.check_string(actual)

    def test_execute_insert_query1(self) -> None:
        """
        Verify that dataframe insertion is correct.
        """
        expected = self.df_to_insert.to_dict()
        self.cursor.execute(imccdbuti.get_ccxt_ohlcv_create_table_query())
        imccdbuti.execute_insert_query(
            self.connection, self.df_to_insert, "ccxt_ohlcv"
        )
        df = hsql.execute_query(self.connection, "SELECT * FROM ccxt_ohlcv")
        actual = huntes.convert_df_to_json_string(df) 
        self.check_string(actual)


class TestUtils1(huntes.TestCase):
    def test_create_insert_query(self) -> None:
        """
        Verify that query is correct.
        """
        df_to_insert = pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "currency_pair",
                "exchange_id",
            ],
            data=[
                [
                    1,
                    1631145600000,
                    30.0,
                    40.0,
                    50.0,
                    60.0,
                    70.0,
                    "BTC/USDT",
                    "binance",
                ]
            ]
        )
        actual_query = imccdbuti._create_insert_query(
            df_to_insert,
            "ccxt_ohlcv"
        )
        self.check_string(actual_query)
