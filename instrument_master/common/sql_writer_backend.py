import abc
from typing import Dict, Optional

import pandas as pd
import psycopg2
import psycopg2.extensions as pexten

import instrument_master.common.data.types as vcdtyp


class AbstractSqlWriterBackend(abc.ABC):
    """
    Interface for manager of CRUD operations on a database defined in db.sql.
    """

    # Provider-specific constant. Map frequency to table name and datetime field.
    # E.g.:
    # {
    #      vcdtyp.Frequency.Daily: {
    #         "table_name": "KibotDailyData",
    #         "datetime_field_name": "date",
    #     }
    # }
    FREQ_ATTR_MAPPING: Dict[vcdtyp.Frequency, Dict[str, str]]

    def __init__(
        self, dbname: str, user: str, password: str, host: str, port: int
    ):
        self.conn: pexten.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

    def ensure_symbol_exists(
        self,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
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

    def ensure_exchange_exists(
        self,
        exchange: str,
    ) -> None:
        """
        Insert new Exchange entry if it does not exist.

        :param exchange: exchange code
        """
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "INSERT INTO Exchange (name) "
                    "VALUES (%s) ON CONFLICT DO NOTHING",
                    [exchange],
                )

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

    @abc.abstractmethod
    def insert_bulk_daily_data(
        self,
        df: pd.DataFrame,
    ) -> None:
        """
        Insert daily data for a particular TradeSymbol entry in bulk.

        :param df: a dataframe from s3
        """

    @abc.abstractmethod
    def insert_daily_data(
        self,
        trade_symbol_id: int,
        date: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
        average_val: Optional[float],
        bar_count_val: Optional[int],
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

    @abc.abstractmethod
    def insert_bulk_minute_data(
        self,
        df: pd.DataFrame,
    ) -> None:
        """
        Insert minute data for a particular TradeSymbol entry in bulk.

        :param df: a dataframe from s3
        """

    @abc.abstractmethod
    def insert_minute_data(
        self,
        trade_symbol_id: int,
        date_time: str,
        open_val: float,
        high_val: float,
        low_val: float,
        close_val: float,
        volume_val: int,
        average_val: Optional[float],
        bar_count_val: Optional[int],
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

    @abc.abstractmethod
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

    def close(self) -> None:
        self.conn.close()

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
