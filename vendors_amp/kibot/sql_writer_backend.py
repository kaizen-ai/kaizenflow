from typing import Optional

import pandas as pd
import psycopg2.extras as pextra

import vendors_amp.common.data.types as vcdtyp
import vendors_amp.common.sql_writer_backend as vcsqlw


class SQLWriterKibotBackend(vcsqlw.AbstractSQLWriterBackend):
    """
    Manager of CRUD operations on a database defined in db.sql.
    """

    FREQ_ATTR_MAPPING = {
        vcdtyp.Frequency.Daily: {
            "table_name": "DailyData",
            "datetime_field_name": "date",
        },
        vcdtyp.Frequency.Minutely: {
            "table_name": "MinuteData",
            "datetime_field_name": "datetime",
        },
        vcdtyp.Frequency.Tick: {
            "table_name": "TickData",
            "datetime_field_name": "datetime",
        },
    }

    def get_remains_data_to_load(
        self, trade_symbol_id: int, df: pd.DataFrame, frequency: vcdtyp.Frequency
    ) -> pd.DataFrame:
        """
        Find the maximum date(time) for trade_symbol_id in a certain frequency
        that already loaded and return a slice of data from a pandas Dataframe
        where datetime > given maximum.

        :param trade_symbol_id: id of TradeSymbol.
        :param df: Pandas Dataframe to load.
        :param frequency: frequency of the data.
        :return: Slice of Pandas Dataframe to load.
        """
        datetime_field_name = self.FREQ_ATTR_MAPPING[frequency][
            "datetime_field_name"
        ]
        table_name = self.FREQ_ATTR_MAPPING[frequency]["table_name"]
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX({datetime_field_name}) "
                    f"FROM {table_name} WHERE "
                    f"trade_symbol_id = {trade_symbol_id}"
                )
                max_datetime = cur.fetchone()[0]
        df[datetime_field_name] = pd.to_datetime(df[datetime_field_name])
        if max_datetime is not None:
            df = df[df[datetime_field_name] > pd.to_datetime(max_datetime)]
        return df

    def delete_data_by_trade_symbol_id(
        self, trade_symbol_id: int, frequency: vcdtyp.Frequency
    ) -> None:
        """
        Delete all data from table by given frequency and trade_symbol_id.

        :param trade_symbol_id: id of TradeSymbol.
        :param frequency: frequency of the data.
        :return:
        """
        table_name = self.FREQ_ATTR_MAPPING[frequency]["table_name"]
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {table_name} "
                    f"WHERE trade_symbol_id = {trade_symbol_id}"
                )

    def insert_bulk_daily_data(
        self,
        df: pd.DataFrame,
    ) -> None:
        """
        Insert daily data for a particular TradeSymbol entry in bulk.

        :param df: a dataframe from s3
        """
        with self.conn:
            with self.conn.cursor() as curs:
                pextra.execute_values(
                    curs,
                    "INSERT INTO DailyData "
                    "(trade_symbol_id, date, open, high, low, close, volume) "
                    "VALUES %s ON CONFLICT DO NOTHING",
                    df.to_dict("records"),
                    template="(%(trade_symbol_id)s, %(date)s, %(open)s,"
                    " %(high)s, %(low)s, %(close)s, %(volume)s)",
                )

    def insert_daily_data(
        self,
        trade_symbol_id: int,
        date: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
        average_val: Optional[float] = None,
        bar_count_val: Optional[int] = None,
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
        :param average_val: average
        :param bar_count_val: bar count
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

    def insert_bulk_minute_data(
        self,
        df: pd.DataFrame,
    ) -> None:
        """
        Insert minute data for a particular TradeSymbol entry in bulk.

        :param df: a dataframe from s3
        """
        with self.conn:
            with self.conn.cursor() as curs:
                pextra.execute_values(
                    curs,
                    "INSERT INTO MinuteData "
                    "(trade_symbol_id, datetime, open, high, low, close, volume) "
                    "VALUES %s ON CONFLICT DO NOTHING",
                    df.to_dict("records"),
                    template="(%(trade_symbol_id)s, %(datetime)s, %(open)s,"
                    " %(high)s, %(low)s, %(close)s, %(volume)s)",
                )

    def insert_minute_data(
        self,
        trade_symbol_id: int,
        date_time: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
        average_val: Optional[float] = None,
        bar_count_val: Optional[int] = None,
    ) -> None:
        """
        Insert minute data for a particular TradeSymbol entry.

        :param trade_symbol_id: id of TradeSymbol
        :param date_time: date and time string
        :param open_val: open price
        :param high_val: high price
        :param low_val: low price
        :param close_val: close price
        :param volume_val: volume
        :param average_val: average
        :param bar_count_val: bar count
        """
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO MinuteData "
                    "(trade_symbol_id, datetime, open, high, low, close, volume) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    [
                        trade_symbol_id,
                        date_time,
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
        date_time: str,
        price_val: float,
        size_val: int,
    ) -> None:
        """
        Insert tick data for a particular TradeSymbol entry.

        :param trade_symbol_id: id of TradeSymbol
        :param date_time: date and time string
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
                        date_time,
                        price_val,
                        size_val,
                    ],
                )
