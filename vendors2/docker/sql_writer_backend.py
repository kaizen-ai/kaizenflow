import psycopg2
import psycopg2.extensions

import helpers.dbg as dbg
import vendors2.kibot.data.types as vkdtyp


class SQLWriterBackend:
    """
    Manager of CRUD operations on a database defined in db.sql.
    """

    def __init__(self, dbname: str, user: str, password: str, host: str):
        self.conn: psycopg2.extensions.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
        )

    def get_exchange_id(
        self,
        exchange: str,
    ) -> int:
        """
        Get primary key (id) of the Exchange entry by its name.

        :param exchange: Name of the Exchange entry as defined in DB.
        :return: primary key (id)
        """
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
        """
        Insert new Symbol entry if it does not exist.

        :param symbol: cymbol code
        :param asset_class: asset class of the Symbol
        """
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
        """
        Get primary key (id) of the Symbol entry by its symbol code.

        :param symbol: symbol code, e.g. GOOGL
        :return: primary key (id)
        """
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
        """
        Insert new TradeSymbol entry if it does not exist.

        :param symbol_id: id of Symbol
        :param exchange_id: id of Exchange
        """
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
        """
        Get primary key (id) of the TradeSymbol entry by its respective Symbol
        and Exchange ids.

        :param symbol_id: id of Symbol
        :param exchange_id: id of Exchange
        :return: primary key (id)
        """
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

    def insert_daily_data(
        self,
        trade_symbol_id: int,
        date: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
    ) -> None:
        """
        Insert daily data for a particular TradeSymbol entry.

        :param trade_symbol_id: id of TradeSymbol
        :param date: date string
        :param open_val: open price
        :param high_val: high price
        :param low_val: low price
        :param close_val: close price
        :param volume_val: volume
        """
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO DailyData "
                    "(trade_symbol_id, date, open, high, low, close, volume) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    [
                        trade_symbol_id,
                        date,
                        open_val,
                        high_val,
                        low_val,
                        close_val,
                        volume_val,
                    ],
                )

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
        """
        Insert minute data for a particular TradeSymbol entry.

        :param trade_symbol_id: id of TradeSymbol
        :param datetime: date and time string
        :param open_val: open price
        :param high_val: high price
        :param low_val: low price
        :param close_val: close price
        :param volume_val: volume
        """
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

    def insert_tick_data(
        self,
        trade_symbol_id: int,
        datetime: str,
        price_val: float,
        size_val: int,
    ) -> None:
        """
        Insert tick data for a particular TradeSymbol entry.

        :param trade_symbol_id: id of TradeSymbol
        :param datetime: date and time string
        :param price_val: price of the transaction
        :param size_val: size of the transaction
        """
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO TickData "
                    "(trade_symbol_id, datetime, price, size) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    [
                        trade_symbol_id,
                        datetime,
                        price_val,
                        size_val,
                    ],
                )

    def close(self) -> None:
        self.conn.close()
