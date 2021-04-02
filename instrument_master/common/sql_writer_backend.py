import abc
from typing import Dict, Optional

import pandas as pd
import psycopg2
import psycopg2.extensions as pexten

import instrument_master.common.data.types as vcdtyp


# TODO: -> Why not AbstractSqlWriter?
class AbstractSqlWriterBackend(abc.ABC):
    """
    Interface for manager of CRUD operations on a database defined in db/sql.
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
        Insert new symbol entry, if it does not exist.
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
        Insert new exchange entry, if it does not exist.
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
        Insert new (`symbol_id`, `exchange_id`) entry, if it does not exist.
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
        Insert daily data in bulk for the given `trade_symbol_id`.

        :param df: a dataframe with the data to insert (e.g., from S3)
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
        Insert daily data for the given `trade_symbol_id`.
        """

    @abc.abstractmethod
    def insert_bulk_minute_data(
        self,
        df: pd.DataFrame,
    ) -> None:
        """
        Insert minute data in bulk for the given `trade_symbol_id`.

        :param df: a dataframe with the data to insert (e.g., from S3)
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
        Insert minute data for the given `trade_symbol_id`.
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
        Insert tick data for the given `trade_symbol_id` entry.
        """

    def close(self) -> None:
        self.conn.close()

    # TODO(*): -> remaining
    # TODO(*): Move df before
    def get_remains_data_to_load(
        self, trade_symbol_id: int, df: pd.DataFrame, frequency: vcdtyp.Frequency
    ) -> pd.DataFrame:
        """
        Return the slice of pandas Dataframe for `trade_symbol_id` and `frequency`
        that still needs to be loaded in the table.

        :return: slice of pandas df to load
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
        # Trim the df, if needed.
        df[datetime_field_name] = pd.to_datetime(df[datetime_field_name])
        if max_datetime is not None:
            df = df[df[datetime_field_name] > pd.to_datetime(max_datetime)]
        return df

    def delete_data_by_trade_symbol_id(
        self, trade_symbol_id: int, frequency: vcdtyp.Frequency
    ) -> None:
        """
        Delete all data from table corresponding to `trade_symbol_id` and `frequency`.
        """
        table_name = self.FREQ_ATTR_MAPPING[frequency]["table_name"]
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {table_name} "
                    f"WHERE trade_symbol_id = {trade_symbol_id}"
                )
