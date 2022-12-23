"""
Import as:

import surrentum_infra_sandbox.examples.binance.db as sisebidb
"""

from typing import Any

import pandas as pd

import psycopg2 as psycop
import psycopg2.extras as extras
import surrentum_infra_sandbox.download as sinsadow
import surrentum_infra_sandbox.save as sinsasav


def get_ohlcv_spot_downloaded_1min_create_table_query() -> str:
    """
    Get SQL query to create Binance OHLCV table.
    """
    query = """
    CREATE TABLE IF NOT EXISTS binance_ohlcv_spot_downloaded_1min(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            end_download_timestamp TIMESTAMP WITH TIME ZONE,
            knowledge_timestamp TIMESTAMP WITH TIME ZONE default CURRENT_TIMESTAMP,
            UNIQUE(timestamp, currency_pair, open, high, low, close, volume)
            )
            """
    return query


def get_ohlcv_spot_resampled_5min_create_table_query() -> str:
    """
    Get SQL query to create Binance OHLCV resampeld model table.
    """
    query = """
    CREATE TABLE IF NOT EXISTS binance_ohlcv_spot_resampled_5min(
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            currency_pair VARCHAR(255) NOT NULL,
            end_download_timestamp TIMESTAMP WITH TIME ZONE,
            knowledge_timestamp TIMESTAMP WITH TIME ZONE default CURRENT_TIMESTAMP,
            UNIQUE(timestamp, currency_pair, open, high, low, close, volume)
            )
            """
    return query


def get_db_connection():
    """
    Retrieve connection based on hardcoded values.

    The parameters must match the parameters set up in the surrentum
    data note docker-compose.
    """
    connection = psycop.connect(
        host="host.docker.internal",
        dbname="airflow",
        port=5532,
        user="postgres",
        password="postgres",
    )
    connection.autocommit = True
    return connection


class PostgresDataFrameSaver(sinsasav.DataSaver):
    """
    Class for saving pandas DataFrame to a postgres DB using a provided DB
    connection.
    """

    def __init__(self, db_connection) -> None:
        """
        Constructor.

        :param db_conn: path to save data to.
        """
        self.db_conn = db_connection
        self._create_tables()

    def save(
        self, data: sinsadow.RawData, db_table: str, *args: Any, **kwargs: Any
    ) -> None:
        """
        Save RawData storing a DataFrame to a specified DB table.

        :param data: data to persists into DB.
        :param db_table: table to save data to.
        """
        if not isinstance(data.get_data(), pd.DataFrame):
            raise ValueError("Only DataFrame is supported.")
        # Transform dataframe into list of tuples.
        df = data.get_data()
        values = [tuple(v) for v in df.to_numpy()]
        # Generate a query for multiple rows.
        query = self._create_insert_query(df, db_table)
        # Execute query for each provided row.
        cursor = self.db_conn.cursor()
        extras.execute_values(cursor, query, values)
        self.db_conn.commit()
        
    def _create_insert_query(self, df: pd.DataFrame, db_table: str) -> str:
        """
        Create an INSERT query to insert data into a DB.

        :param df: data to insert into DB
        :param table_name: name of the table for insertion
        :return: sql query, e.g.,
                ```
                INSERT INTO ccxt_ohlcv(timestamp,open,high,low,close) VALUES %s
                ```
        """             
        columns = ",".join(list(df.columns))
        query = f"INSERT INTO {db_table}({columns}) VALUES %s"
        return query

    def _create_tables(self) -> None:
        """
        Create DB data tables used in this example.

        Normally table creation would be handled elsewhere, as an
        example this suffices.
        """
        cursor = self.db_conn.cursor()
        query = get_ohlcv_spot_downloaded_1min_create_table_query()
        cursor.execute(query)
        query = get_ohlcv_spot_resampled_5min_create_table_query()
        cursor.execute(query)
