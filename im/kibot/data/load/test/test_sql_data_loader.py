import datetime

import pandas as pd
import pytest

import helpers.sql as hsql
import helpers.unit_test as hunitest
import im.common.data.types as imcodatyp
import im.common.db.create_db as imcdbcrdb
import im.common.db.utils as imcodbuti
import im.kibot.data.load.kibot_sql_data_loader as ikdlksdlo
import im.kibot.sql_writer as imkisqwri


@pytest.mark.skipif(
    not imcodbuti.is_inside_im_container(),
    reason="Testable only inside IM container",
)
class TestSqlDataLoader1(hunitest.TestCase):
    """
    Test writing operation to PostgreSQL Kibot DB.
    """

    def setUp(self) -> None:
        super().setUp()
        # Get PostgreSQL connection parameters.
        self._connection = hsql.get_connection_from_env_vars()[0]
        self._new_db = self._get_test_name().replace("/", "").replace(".", "")
        # Create database for test.
        imcdbcrdb.create_database(
            connection=self._connection,
            new_db=self._new_db,
            force=True,
        )
        # Initialize writer class to test.
        writer = imkisqwri.KibotSqlWriter(
            self._new_db, self._user, self._password, self._host, self._port
        )
        # Add data to database.
        self._prepare_tables(writer)
        writer.close()
        # Create loader.
        self._loader = ikdlksdlo.KibotSqlDataLoader(
            self._new_db, self._user, self._password, self._host, self._port
        )

    def tearDown(self) -> None:
        # Close connection.
        self._loader.conn.close()
        # Remove created database.
        imcdbcrdb.remove_database(
            connection=self._connection, db_to_drop=self._new_db
        )
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
        actual = self._loader._read_data(
            "CME", "ZYX9", imcodatyp.Frequency.Minutely
        )
        # Convert to string.
        actual_string = hunitest.convert_df_to_string(actual)
        # Compare with golden.
        self.check_string(actual_string, fuzzy_match=True)

    def test_read_data2(self) -> None:
        """
        Test correct daily data reading for ETF0 on LSE.
        """
        # Get data.
        actual = self._loader._read_data("LSE", "ZYX9", imcodatyp.Frequency.Daily)
        # Convert to string.
        actual_string = hunitest.convert_df_to_string(actual)
        # Compare with golden.
        self.check_string(actual_string, fuzzy_match=True)

    def test_read_data3(self) -> None:
        """
        Test failed assertion for invalid exchange.
        """
        # Get data.
        with self.assertRaises(AssertionError):
            self._loader._read_data("", "ZYX9", imcodatyp.Frequency.Daily)

    def test_read_data4(self) -> None:
        """
        Test failed assertion for invalid ticker.
        """
        # Get data.
        with self.assertRaises(AssertionError):
            self._loader._read_data("CME", "", imcodatyp.Frequency.Minutely)

    @classmethod
    def _prepare_tables(cls, writer: imkisqwri.KibotSqlWriter) -> None:
        """
        Insert Symbol, Exchange and TradeSymbol entries to make test work.

        See `common/db/sql/` for more info.
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
