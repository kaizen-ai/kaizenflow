import psycopg2
import os
import vendors_amp.docker.sql_writer_backend as vdsqlw
import helpers.unit_test as hut
import pandas as pd
from typing import Optional
import vendors_amp.kibot.data.types as vakdt


class TestSqlWriterBackend1(hut.TestCase):
    """
    Test writing operation to Postgresql kibot db.
    """
    def setUp(self) -> None:
        super().setUp()
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        user = os.environ["POSTGRES_USER"]
        pwrd = os.environ["POSTGRES_PASSWORD"]
        dbnm = os.environ["POSTGRES_DB"]
        self._writer = vdsqlw.SQLWriterBackend(dbnm, user, pwrd, host, port)

    def tearDown(self) -> None:
        # Remove data from key tables.
        with self._writer.conn:
            with self._writer.conn.cursor() as curs:
                curs.execute("DELETE FROM TradeSymbol WHERE id = %s",
                             [self._get_test_number()])
                curs.execute("DELETE FROM Exchange WHERE id = %s",
                             [self._get_test_number()])
                curs.execute("DELETE FROM Symbol WHERE id = %s",
                             [self._get_test_number()])
        # Close connection.
        self._writer.close()
        super().tearDown()

    def _prepare_test(
        self,
        insert_symbol: bool,
        insert_exchange: bool,
        insert_trade_symbol: bool,
    ) -> None:
        """
        Insert Symbol, Exchange and TradeSymbol entries to make test work.
        """
        with self._writer.conn:
            with self._writer.conn.cursor() as curs:
                if insert_symbol:
                    curs.execute(
                        "INSERT INTO Symbol (id, code, asset_class) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        [
                            self._get_test_number(),
                            self._get_test_string(), "Futures"
                        ],
                    )
                print("Symbols:")
                print(pd.read_sql("SELECT * FROM Symbol", self._writer.conn))
                if insert_exchange:
                    curs.execute(
                        "INSERT INTO Exchange (id, name) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        [
                            self._get_test_number(),
                            self._get_test_string(),
                        ],
                    )
                print("Exchs:")
                print(pd.read_sql("SELECT * FROM Exchange", self._writer.conn))
                if insert_trade_symbol:
                    curs.execute(
                        "INSERT INTO TradeSymbol (id, exchange_id, symbol_id) "
                        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        [
                            self._get_test_number(),
                            self._get_test_number(),
                            self._get_test_number()
                        ],
                    )

    def _get_test_string(self) -> str:
        string: str = self._get_test_name().replace("/", "").replace(".", "")
        return string

    def _get_test_number(self) -> int:
        string = self._get_test_string()
        return hash(string) % 10**8

    def _check_saved_data(self, table: str, test_id_field: str,
                          test_id_type: type) -> None:
        """
        Check data saved in Postgresql by test.

        :param table: table name
        :param test_id_field: field in table to identify from which test
            data was generated
        :param test_id_type: type of id field: str or int.
        """
        condition = ""
        if test_id_type == int:
            condition = "WHERE %s = %s" % (test_id_field,
                                           self._get_test_number())
        elif test_id_type == str:
            condition = "WHERE %s = '%s'" % (test_id_field,
                                             self._get_test_string())
        query = "SELECT * FROM %s %s;" % (table, condition)
        res = pd.read_sql_query(query, self._writer.conn)
        self.check_string(hut.convert_df_to_string(res))
        remove_query = "DELETE FROM %s %s" % (table, condition)
        with self._writer.conn:
            with self._writer.conn.cursor() as curs:
                curs.execute(remove_query)

    def test_ensure_symbol_exist1(self) -> None:
        self._writer.ensure_symbol_exists(symbol=self._get_test_string(),
                                          asset_class=vakdt.AssetClass.Futures)
        self._check_saved_data(table="Symbol",
                               test_id_field="code",
                               test_id_type=str)

    def test_ensure_trade_symbol_exist1(self) -> None:
        self._prepare_test(insert_symbol=True,
                           insert_exchange=True,
                           insert_trade_symbol=False)
        self._writer.ensure_trade_symbol_exists(
            symbol_id=self._get_test_number(),
            exchange_id=self._get_test_number())
        self._check_saved_data(table="TradeSymbol",
                               test_id_field="symbol_id",
                               test_id_type=int)

    def test_insert_bulk_daily_data1(self) -> None:
        self._prepare_test(insert_symbol=True,
                           insert_exchange=True,
                           insert_trade_symbol=True)
        df = pd.DataFrame({
            "trade_symbol_id": [self._get_test_number()] * 3,
            "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "open": [10.] * 3,
            "high": [15] * 3,
            "low": [9] * 3,
            "close": [12.5] * 3,
            "volume": [1000] * 3,
        })
        self._writer.insert_bulk_daily_data(df=df)
        self._check_saved_data(table="DailyData",
                               test_id_field="trade_symbol_id",
                               test_id_type=int)

    def test_insert_daily_data1(self) -> None:
        self._prepare_test(insert_symbol=True,
                           insert_exchange=True,
                           insert_trade_symbol=True)
        self._writer.insert_daily_data(
            trade_symbol_id=self._get_test_number(),
            date="2021-01-01",
            open_val=10.,
            high_val=15,
            low_val=9,
            close_val=12.5,
            volume_val=1000,
        )
        self._check_saved_data(table="DailyData",
                               test_id_field="trade_symbol_id",
                               test_id_type=int)

    def test_insert_bulk_minute_data1(self) -> None:
        self._prepare_test(insert_symbol=True,
                           insert_exchange=True,
                           insert_trade_symbol=True)
        df = pd.DataFrame({
            "trade_symbol_id": [self._get_test_number()] * 3,
            "datetime": [
                "2021-02-10T13:50:00Z", "2021-02-10T13:51:00Z",
                "2021-02-10T13:52:00Z"
            ],
            "open": [10.] * 3,
            "high": [15] * 3,
            "low": [9] * 3,
            "close": [12.5] * 3,
            "volume": [1000] * 3,
        })
        self._writer.insert_bulk_minute_data(df=df)
        self._check_saved_data(table="MinuteData",
                               test_id_field="trade_symbol_id",
                               test_id_type=int)

    def test_insert_minute_data1(self) -> None:
        self._prepare_test(insert_symbol=True,
                           insert_exchange=True,
                           insert_trade_symbol=True)
        self._writer.insert_minute_data(
            trade_symbol_id=self._get_test_number(),
            date_time="2021-02-10T13:50:00Z",
            open_val=10.,
            high_val=15,
            low_val=9,
            close_val=12.5,
            volume_val=1000,
        )
        self._check_saved_data(table="MinuteData",
                               test_id_field="trade_symbol_id",
                               test_id_type=int)

    def test_insert_tick_data1(self) -> None:
        self._prepare_test(insert_symbol=True,
                           insert_exchange=True,
                           insert_trade_symbol=True)
        self._writer.insert_tick_data(
            trade_symbol_id=self._get_test_number(),
            date_time="2021-02-10T13:50:00Z",
            price_val=10.,
            size_val=15,
        )
        self._check_saved_data(table="TickData",
                               test_id_field="trade_symbol_id",
                               test_id_type=int)
