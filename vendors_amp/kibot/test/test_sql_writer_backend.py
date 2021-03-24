import os

import pandas as pd
import pytest

import helpers.unit_test as hut
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.common.db.init as vcdini
import vendors_amp.kibot.sql_writer_backend as vksqlw


@pytest.mark.skipif(
    not vcdini.is_inside_im_container(),
    reason="Testable only inside kibot container",
)
class TestSqlWriterBackend1(hut.TestCase):
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
        self._dbname = self._get_test_string()
        # Create database for each test.
        vcdini.create_database(
            self._dbname,
            vcdini.get_init_sql_files(),
            force=True,
        )
        # Initialize writer class to test.
        self._writer = vksqlw.SQLWriterKibotBackend(
            self._dbname, user, password, host, port
        )
        # Define constant id-s for records across the test.
        self._symbol_id = 10
        self._exchange_id = 20
        self._trade_symbol_id = 30

    def tearDown(self) -> None:
        # Close connection.
        self._writer.close()
        # Remove created database.
        vcdini.remove_database(self._dbname)
        super().tearDown()

    def test_ensure_symbol_exist1(self) -> None:
        """
        Test adding a new symbol to Symbol table.
        """
        self._writer.ensure_symbol_exists(
            symbol=self._get_test_string(), asset_class=vcdtyp.AssetClass.Futures
        )
        self._check_saved_data(table="Symbol")

    def test_get_remains_data_to_load(self) -> None:
        """
        Slicing Pandas Dataframe to load.

        This test, mock the situation, when loading process interrupted
        somehow. Then, we need to load remaining data from the Pandas
        Dataframe.
        """
        # Load the daily data.
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        df = pd.DataFrame(
            {
                "trade_symbol_id": [self._trade_symbol_id] * 3,
                "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
                "open": [10.0] * 3,
                "high": [15] * 3,
                "low": [9] * 3,
                "close": [12.5] * 3,
                "volume": [1000] * 3,
            }
        )
        self._writer.insert_bulk_daily_data(df=df)
        # Mock the situation, when loading process interrupted somehow.
        # Literally delete the tail of the data.
        with self._writer.conn as conn:
            with conn.cursor() as curs:
                curs.execute(
                    "DELETE FROM KibotDailyData WHERE date > '2021-01-01'"
                )
        # Get remains of pandas Dataframe to load.
        df = self._writer.get_remains_data_to_load(
            self._trade_symbol_id, df, vcdtyp.Frequency.Daily
        )
        # Convert dataframe to string.
        txt = hut.convert_df_to_string(df)
        # Check the output against the golden.
        self.check_string(txt, fuzzy_match=True)

    def test_ensure_exchange_exist1(self) -> None:
        """
        Test adding a new exchange to Exchange table.
        """
        self._writer.ensure_exchange_exists(exchange=self._get_test_string())
        self._check_saved_data(table="Exchange")

    def test_ensure_trade_symbol_exist1(self) -> None:
        """
        Test adding a new symbol to TradeSymbol table.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=False
        )
        self._writer.ensure_trade_symbol_exists(
            symbol_id=self._symbol_id, exchange_id=self._exchange_id
        )
        self._check_saved_data(table="TradeSymbol")

    def test_insert_bulk_daily_data1(self) -> None:
        """
        Test adding a dataframe to KibotDailyData table.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        df = pd.DataFrame(
            {
                "trade_symbol_id": [self._trade_symbol_id] * 3,
                "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
                "open": [10.0] * 3,
                "high": [15] * 3,
                "low": [9] * 3,
                "close": [12.5] * 3,
                "volume": [1000] * 3,
            }
        )
        self._writer.insert_bulk_daily_data(df=df)
        self._check_saved_data(table="KibotDailyData")

    def test_insert_bulk_daily_data_with_holes(self) -> None:
        """
        Test adding a dataframe to KibotDailyData table if some data is
        missing.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        df = pd.DataFrame(
            {
                "trade_symbol_id": [self._trade_symbol_id] * 3,
                "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
                "open": [10.0] * 3,
                "high": [15] * 3,
                "low": [9] * 3,
                "close": [12.5] * 3,
                "volume": [1000] * 3,
            }
        )
        self._writer.insert_bulk_daily_data(df=df)
        with self._writer.conn as conn:
            with conn.cursor() as curs:
                curs.execute(
                    "delete from KibotDailyData where date = '2021-01-02'"
                )
        self._writer.insert_bulk_daily_data(df=df)
        self._check_saved_data(table="KibotDailyData")

    def test_insert_daily_data1(self) -> None:
        """
        Test adding a one bar data to KibotDailyData table.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        self._writer.insert_daily_data(
            trade_symbol_id=self._trade_symbol_id,
            date="2021-01-01",
            open_val=10.0,
            high_val=15,
            low_val=9,
            close_val=12.5,
            volume_val=1000,
        )
        self._check_saved_data(table="KibotDailyData")

    def test_insert_bulk_minute_data1(self) -> None:
        """
        Test adding a dataframe to KibotMinuteData table.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        df = pd.DataFrame(
            {
                "trade_symbol_id": [self._trade_symbol_id] * 3,
                "datetime": [
                    "2021-02-10T13:50:00Z",
                    "2021-02-10T13:51:00Z",
                    "2021-02-10T13:52:00Z",
                ],
                "open": [10.0] * 3,
                "high": [15] * 3,
                "low": [9] * 3,
                "close": [12.5] * 3,
                "volume": [1000] * 3,
            }
        )
        self._writer.insert_bulk_minute_data(df=df)
        self._check_saved_data(table="KibotMinuteData")

    def test_insert_bulk_minute_data_with_holes(self) -> None:
        """
        Test adding a dataframe to KibotMinuteData table if some data is
        missing.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        df = pd.DataFrame(
            {
                "trade_symbol_id": [self._trade_symbol_id] * 3,
                "datetime": [
                    "2021-02-10T13:50:00Z",
                    "2021-02-10T13:51:00Z",
                    "2021-02-10T13:52:00Z",
                ],
                "open": [10.0] * 3,
                "high": [15] * 3,
                "low": [9] * 3,
                "close": [12.5] * 3,
                "volume": [1000] * 3,
            }
        )
        with self._writer.conn as conn:
            with conn.cursor() as curs:
                curs.execute(
                    "DELETE FROM KibotMinuteData "
                    "WHERE datetime = '2021-02-10T13:51:00Z'"
                )
        self._writer.insert_bulk_minute_data(df=df)
        self._check_saved_data(table="KibotMinuteData")

    def test_insert_minute_data1(self) -> None:
        """
        Test adding a one bar data to KibotMinuteData table.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        self._writer.insert_minute_data(
            trade_symbol_id=self._trade_symbol_id,
            date_time="2021-02-10T13:50:00Z",
            open_val=10.0,
            high_val=15,
            low_val=9,
            close_val=12.5,
            volume_val=1000,
        )
        self._check_saved_data(table="KibotMinuteData")

    def test_insert_tick_data1(self) -> None:
        """
        Test adding a one tick data to KibotTickData table.
        """
        self._prepare_tables(
            insert_symbol=True, insert_exchange=True, insert_trade_symbol=True
        )
        self._writer.insert_tick_data(
            trade_symbol_id=self._trade_symbol_id,
            date_time="2021-02-10T13:50:00Z",
            price_val=10.0,
            size_val=15,
        )
        self._check_saved_data(table="KibotTickData")

    def _prepare_tables(
        self,
        insert_symbol: bool,
        insert_exchange: bool,
        insert_trade_symbol: bool,
    ) -> None:
        """
        Insert Symbol, Exchange and TradeSymbol entries to make test work.

        See `common/db/sql` for more info.
        """
        with self._writer.conn:
            with self._writer.conn.cursor() as curs:
                # Fill Symbol table.
                if insert_symbol:
                    curs.execute(
                        "INSERT INTO Symbol (id, code, asset_class) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        [
                            self._symbol_id,
                            "ZS1M",
                            "Futures",
                        ],
                    )
                # Fill Exchange table.
                if insert_exchange:
                    curs.execute(
                        "INSERT INTO Exchange (id, name) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        [
                            self._exchange_id,
                            "CME",
                        ],
                    )
                # Fill TradeSymbol table.
                if insert_trade_symbol:
                    curs.execute(
                        "INSERT INTO TradeSymbol (id, exchange_id, symbol_id) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        [
                            self._trade_symbol_id,
                            self._exchange_id,
                            self._symbol_id,
                        ],
                    )

    def _get_test_string(self) -> str:
        string: str = self._get_test_name().replace("/", "").replace(".", "")
        return string

    def _check_saved_data(self, table: str) -> None:
        """
        Check data saved in Postgresql by test.

        :param table: table name
        """
        # Construct query to retrieve the data.
        query = "SELECT * FROM %s;" % table
        # Get data in pandas format based on query.
        res = pd.read_sql(query, self._writer.conn)
        # Exclude changeable columns.
        columns_to_check = list(res.columns)
        for column_to_remove in ["id", "start_date"]:
            if column_to_remove in columns_to_check:
                columns_to_check.remove(column_to_remove)
        # Convert dataframe to string.
        txt = hut.convert_df_to_string(res[columns_to_check])
        # Check the output against the golden.
        self.check_string(txt, fuzzy_match=True)
