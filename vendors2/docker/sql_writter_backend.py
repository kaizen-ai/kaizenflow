import psycopg2
import psycopg2.extensions

import helpers.dbg as dbg
import vendors2.kibot.data.types as vkdtyp


class SQLWriterBackend:
    conn: psycopg2.extensions.connection

    def __init__(self, dbname: str, user: str, password: str, host: str):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
        )

    def get_exchange_id(
        self,
        exchange: str,
    ) -> int:
        exchange_id = -1
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "SELECT id FROM Exchange WHERE name = %s", [exchange]
                )
                if curs.rowcount:
                    (_exchange_id,) = curs.fetchone()
                    exchange_id = _exchange_id
        if exchange_id == 0:
            dbg.dfatal(f"Could not find Exchange ${exchange}")
        return exchange_id

    def ensure_symbol_exists(
        self,
        symbol: str,
        asset_class: vkdtyp.AssetClass,
    ) -> None:
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO Symbol (code, asset_class) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [symbol, asset_class.value],
                )

    def get_symbol_id(
        self,
        symbol: str,
    ) -> int:
        symbol_id = -1
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute("SELECT id FROM Symbol WHERE code = %s", [symbol])
                if curs.rowcount:
                    (_symbol_id,) = curs.fetchone()
                    symbol_id = _symbol_id
        if symbol_id == -1:
            dbg.dfatal(f"Could not find Symbol ${symbol}")
        return symbol_id

    def ensure_trade_symbol_exists(
        self,
        symbol_id: int,
        exchange_id: int,
    ) -> None:
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO TradeSymbol (symbol_id, exchange_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    [symbol_id, exchange_id],
                )

    def get_trade_symbol_id(
        self,
        symbol_id: int,
        exchange_id: int,
    ) -> int:
        trade_symbol_id = -1
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "SELECT id FROM TradeSymbol "
                    "WHERE symbol_id = %s AND exchange_id = %s",
                    [symbol_id, exchange_id],
                )
                if curs.rowcount:
                    (_trade_symbol_id,) = curs.fetchone()
                    trade_symbol_id = _trade_symbol_id
        if trade_symbol_id == -1:
            dbg.dfatal(
                f"Could not find Trade Symbol with "
                f"symbol_id=${symbol_id} and exchange_id=${exchange_id}"
            )
        return trade_symbol_id

    def insert_minute_data(
        self,
        trade_symbol_id: int,
        datetime: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
    ) -> None:
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO MinuteData "
                    "(trade_symbol_id, datetime, open, high, low, close, volume) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    [
                        trade_symbol_id,
                        datetime,
                        open_val,
                        high_val,
                        low_val,
                        close_val,
                        volume_val,
                    ],
                )

    def close(self) -> None:
        self.conn.close()
