import datetime
import os

import pandas as pd
import psycopg2
import psycopg2.sql as psql
import pytest

import helpers.io_ as hio
import helpers.unit_test as hut
import vendors_amp.kibot.data.load.sql_data_loader as vkdlsq
import vendors_amp.common.data.types as vkdtyp
import vendors_amp.kibot.sql_writer_backend as vksqlw

DB_SCHEMA_FILE = os.path.join(
    os.path.dirname(__file__), "../../../compose/init_sql/db.sql"
)


class TestDbSchemaFile(hut.TestCase):
    """
    Test SQL initialization file existence.
    """

    def test_exist1(self) -> None:
        """
        Test that schema SQL file exists.
        """
        self.assertTrue(os.path.exists(DB_SCHEMA_FILE))


@pytest.mark.skipif(
    not (
        (
            os.environ.get("STAGE") == "TEST"
            and os.environ.get("POSTGRES_HOST") == "kibot_postgres_test"
        )
        or (
            os.environ.get("STAGE") == "LOCAL"
            and os.environ.get("POSTGRES_HOST") == "kibot_postgres_local"
        )
    ),
    reason="Testable only inside kibot container",
)
class TestSqlDataLoader1(hut.TestCase):
    """
    Test writing operation to Postgresql kibot db.
    """

    def setUp(self) -> None:
        super().setUp()
        # Get postgresql connection parameters.
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        self.dbname = self._get_test_name().replace("/", "").replace(".", "")
        # Create database for test.
        create_database(self.dbname)
        # Initialize writer class to test.
        writer = vksqlw.SQLWriterKibotBackend(self.dbname, user, password, host, port)
        # Apply production schema to created database.
        with writer.conn as conn:
            with conn.cursor() as curs:
                curs.execute(hio.from_file(DB_SCHEMA_FILE))
        # Add data to database.
        self._prepare_tables(writer)
        writer.close()
        # Create loader.
        self._loader = vkdlsq.SQLKibotDataLoader(
            self.dbname, user, password, host, port
        )

    def tearDown(self) -> None:
        # Close connection.
        self._loader.conn.close()
        # Remove created database.
        remove_database(self.dbname)
        super().tearDown()

    def test_get_symbol_id1(self) -> None:
        """
        Test correct mapping from symbol name to internal id.
        """
        actual = self._loader.get_symbol_id("ABC0")
        expected = 10
        self.assertEqual(actual, expected)

    def test_get_symbol_id2(self) -> None:
        """
        Test incorrect symbol.
        """
        with self.assertRaises(AssertionError):
            self._loader.get_symbol_id("_")

    def test_get_exchange_id1(self) -> None:
        """
        Test correct mapping from exchange name to internal id.
        """
        actual = self._loader.get_exchange_id("CME")
        expected = 10
        self.assertEqual(actual, expected)

    def test_get_exchange_id2(self) -> None:
        """
        Test incorrect exchange.
        """
        with self.assertRaises(AssertionError):
            self._loader.get_exchange_id("_")

    def test_get_trade_symbol_id1(self) -> None:
        """
        Test correct mapping from symbol/exchange pair to internal trade symbol
        id.
        """
        actual = self._loader.get_trade_symbol_id(11, 11)
        expected = 12
        self.assertEqual(actual, expected)

    def test_get_trade_symbol_id2(self) -> None:
        """
        Test incorrect trade symbol.
        """
        with self.assertRaises(AssertionError):
            self._loader.get_trade_symbol_id(9, 9)

    def test_read_data1(self) -> None:
        """
        Test correct minute data reading for ZYX9 on CME.
        """
        # Get data.
        actual = self._loader._read_data("CME", "ZYX9", vkdtyp.Frequency.Minutely)
        # Convert to string.
        actual_string = hut.convert_df_to_string(actual)
        # Compare with golden.
        self.check_string(actual_string)

    def test_read_data2(self) -> None:
        """
        Test correct daily data reading for ETF0 on LSE.
        """
        # Get data.
        actual = self._loader._read_data("LSE", "ZYX9", vkdtyp.Frequency.Daily)
        # Convert to string.
        actual_string = hut.convert_df_to_string(actual)
        # Compare with golden.
        self.check_string(actual_string)

    def test_read_data3(self) -> None:
        """
        Test failed assertion for unexisting exchange.
        """
        # Get data.
        with self.assertRaises(AssertionError):
            self._loader._read_data("", "ZYX9", vkdtyp.Frequency.Daily)

    def test_read_data4(self) -> None:
        """
        Test failed assertion on for unexisting.
        """
        # Get data.
        with self.assertRaises(AssertionError):
            self._loader._read_data("CME", "", vkdtyp.Frequency.Minutely)

    @classmethod
    def _prepare_tables(cls, writer: vksqlw.SQLWriterKibotBackend) -> None:
        """
        Insert Symbol, Exchange and TradeSymbol entries to make test work.

        See `DB_SCHEMA_FILE` for more info.
        """
        with writer.conn as conn:
            with conn.cursor() as curs:
                # Fill Symbol table.
                for symbol_id, name, asset_class in [
                    (10, "ABC0", "Futures"),
                    (11, "ZYX9", "Futures"),
                    (12, "ETF0", "etfs"),
                ]:
                    curs.execute(
                        "INSERT INTO Symbol (id, code, asset_class) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        [
                            symbol_id,
                            name,
                            asset_class,
                        ],
                    )
                # Fill Exchange table.
                for exchange_id, name in [
                    (10, "CME"),
                    (11, "LSE"),
                ]:
                    curs.execute(
                        "INSERT INTO Exchange (id, name) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        [
                            exchange_id,
                            name,
                        ],
                    )
                # Fill TradeSymbol table.
                for trade_symbol_id, exchange_id, symbol_id in [
                    (10, 10, 10),
                    (11, 10, 11),
                    (12, 11, 11),
                    (13, 11, 12),
                ]:
                    curs.execute(
                        "INSERT INTO TradeSymbol (id, exchange_id, symbol_id) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        [
                            trade_symbol_id,
                            exchange_id,
                            symbol_id,
                        ],
                    )
                # Fill minute and daily data.
                for trade_symbol_id in [10, 11, 12, 13]:
                    generated_data = cls._generate_test_data(trade_symbol_id)
                    writer.insert_bulk_daily_data(
                        generated_data.drop(columns=["datetime"])
                    )
                    writer.insert_bulk_minute_data(
                        generated_data.drop(columns=["date"])
                    )

    @staticmethod
    def _generate_test_data(seed: int) -> pd.DataFrame:
        """
        Generate dataframe with some data based on symbol.
        """
        nrows = 5
        df = pd.DataFrame(
            {
                "trade_symbol_id": [seed] * nrows,
                "date": [
                    datetime.date(2021, 1, i + 1).isoformat()
                    for i in range(nrows)
                ],
                "datetime": [
                    datetime.datetime(2021, 1, 1, 1, i + 1, 0).isoformat()
                    for i in range(nrows)
                ],
                "open": [seed + i for i in range(nrows)],
                "high": [seed + 2 * i for i in range(nrows)],
                "low": [seed - i for i in range(nrows)],
                "close": [seed] * nrows,
                "volume": [seed * 100] * nrows,
            }
        )
        return df


# TODO(plyq): Move it to common place, e.g. helpers.
def create_database(dbname: str) -> None:
    """
    Create database in current environment.
    """
    # Initialize connection.
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    default_dbname = os.environ["POSTGRES_DB"]
    connection = psycopg2.connect(
        dbname=default_dbname,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Make DROP/CREATE DATABASE executable from transaction block.
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Create a database from scratch.
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                psql.SQL("DROP DATABASE IF EXISTS {};").format(
                    psql.Identifier(dbname)
                )
            )
            cursor.execute(
                psql.SQL("CREATE DATABASE {};").format(psql.Identifier(dbname))
            )
    # Close connection.
    connection.close()


# TODO(plyq): Move it to common place, e.g. helpers.
def remove_database(dbname: str) -> None:
    """
    Remove database in current environment.
    """
    # Initialize connection.
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    default_dbname = os.environ["POSTGRES_DB"]
    connection = psycopg2.connect(
        dbname=default_dbname,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Make DROP DATABASE executable from transaction block.
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Drop database.
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                psql.SQL("DROP DATABASE {};").format(psql.Identifier(dbname))
            )
    # Close connection.
    connection.close()
