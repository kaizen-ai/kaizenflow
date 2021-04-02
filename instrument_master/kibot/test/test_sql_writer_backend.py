import pandas as pd
import pytest

import helpers.unit_test as hut
import instrument_master.common.data.types as vcdtyp
import instrument_master.common.db.init as vcdini
import instrument_master.common.test.utils as vctuti
import instrument_master.kibot.sql_writer_backend as vksqlw


# TODO(*): -> TestKibotSqlWriterBackend1
@pytest.mark.skipif(
    not vcdini.is_inside_im_container(),
    reason="Testable only inside IM container",
)
class TestSqlWriterBackend1(vctuti.SqlWriterBackendTestCase):
    """
    Test writing operation to PostgreSQL DB.
    """

    def setUp(self) -> None:
        super().setUp()
        # Initialize writer class to test.
        self._writer = vksqlw.KibotSqlWriterBackend(
            dbname=self._dbname,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )

    def test_ensure_symbol_exist1(self) -> None:
        """
        Test adding a new symbol to symbol table.
        """
        self._writer.ensure_symbol_exists(
            symbol=self._get_test_string(), asset_class=vcdtyp.AssetClass.Futures
        )
        self._check_saved_data(table="Symbol")

    # TODO(*): -> remaining
    def test_get_remains_data_to_load(self) -> None:
        """
        This test mocks the situation when the loading process was interrupted
        We then need to load remaining data from the pandas Dataframe.
        """
        # Load the daily data.
        # TODO(*): Extract the common code into a helper.
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
        # Mock the situation when the loading process is interrupted. To simulate
        # this, by deleting the tail of the data.
        with self._writer.conn as conn:
            with conn.cursor() as curs:
                curs.execute(
                    "DELETE FROM KibotDailyData WHERE date > '2021-01-01'"
                )
        # Get the remaining of pandas Dataframe to load.
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
