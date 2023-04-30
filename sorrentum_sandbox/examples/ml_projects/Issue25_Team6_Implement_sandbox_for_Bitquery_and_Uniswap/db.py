"""
Implementation of load part of the ETL and QA pipeline.

Import as:

import sorrentum_sandbox.examples.ml_projects.Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap.db as sisebidb

"""

from typing import Any, Optional

import pandas as pd
import psycopg2 as psycop
import psycopg2.extras as extras

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import sorrentum_sandbox.common.client as sinsacli
import sorrentum_sandbox.common.download as sinsadow
import sorrentum_sandbox.common.save as sinsasav


def get_uniswap_table_query() -> str:
    """
    Get SQL query to create GraphQL API table.

    This table contains the data as it is downloaded.
    """
    query = """
    CREATE TABLE IF NOT EXISTS uniswap_table(
            tradeIndex INT,
            block_timestamp_time TIMESTAMP,
            block_height INT,
            exchange_fullName VARCHAR(255) NOT NULL,
            protocol VARCHAR(255) NOT NULL,
            timeInterval_minute TIMESTAMP,
            baseAmount DECIMAL NOT NULL,
            quoteAmount DECIMAL NOT NULL,
            trades INT,
            quotePrice DECIMAL NOT NULL,
            maximum_price DECIMAL NOT NULL,
            minimum_price DECIMAL NOT NULL,
            open_price DECIMAL NOT NULL,
            close_price DECIMAL NOT NULL,
            baseCurrency_symbol VARCHAR(255) NOT NULL,
            baseCurrency_address VARCHAR(255) NOT NULL,
            quoteCurrency_symbol VARCHAR(255) NOT NULL,
            quoteCurrency_address VARCHAR(255) NOT NULL,
            transaction_hash VARCHAR(255) NOT NULL,
            transaction_gas INT,
            transaction_to_address VARCHAR(255) NOT NULL,
            transaction_txFrom_address VARCHAR(255) NOT NULL
            )
            """
    return query



def get_db_connection() -> Any:
    """
    Retrieve connection to the Postgres DB inside the Sorrentum data node.

    The parameters must match the parameters set up in the Sorrentum
    data node docker-compose.
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


# #############################################################################
# PostgresDataFrameSaver
# #############################################################################


class PostgresDataFrameSaver(sinsasav.DataSaver):
    """
    Save Pandas DataFrame to a PostgreSQL using a provided DB connection.
    """

    def __init__(self, db_connection: str) -> None:
        """
        Constructor.

        :param db_conn: DB connection
        """
        self.db_conn = db_connection
        self._create_tables()

    def save(
        self, data: sinsadow.RawData, db_table: str, *args: Any, **kwargs: Any
    ) -> None:
        """
        Save RawData storing a DataFrame to a specified DB table.

        :param data: data to persists into DB
        :param db_table: table to save data to
        """
        hdbg.dassert_isinstance(data.get_data(), pd.DataFrame, "Only DataFrame is supported.")
        # Transform dataframe into list of tuples.
        df = data.get_data()
        values = [tuple(v) for v in df.to_numpy()]
        # Generate a query for multiple rows.
        query = self._create_insert_query(df, db_table)
        # Execute query for each provided row.
        cursor = self.db_conn.cursor()
        extras.execute_values(cursor, query, values)
        self.db_conn.commit()

    @staticmethod
    def _create_insert_query(df: pd.DataFrame, db_table: str) -> str:
        """
        Create an INSERT query to insert data into a DB.

        :param df: data to insert into DB
        :param table_name: name of the table for insertion
        :return: SQL query, e.g.,
            ```
            INSERT INTO bitquery_uniswap(timestamp,open,high,low,close) VALUES %s  
        ```
        """
        columns = ",".join(list(df.columns))
        query = f"INSERT INTO {db_table}({columns}) VALUES %s"
        return query

    def _create_tables(self) -> None:
        """
        Create DB data tables to store data.

        Note that typically table creation would not be handled in the same place
        as downloading the data, but as an example this suffices.
        """
        cursor = self.db_conn.cursor()
        #
        query = get_uniswap_table_query()
        cursor.execute(query)
        


# #############################################################################
# PostgresClient
# #############################################################################


class PostgresClient(sinsacli.DataClient):
    """
    Load PostgreSQL data.
    """

    def __init__(self, db_connection: str) -> None:
        """
        Constructor.

        :param db_conn: DB connection
        """
        self.db_conn = db_connection

    def load(
        self,
        dataset_signature: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Load CSV data specified by a unique signature from a desired source
        directory for a specified time period.

        The method assumes data having a `timestamp` column.
        """
        select_query = f"SELECT * FROM {dataset_signature}"
        # Filter data.
        if start_timestamp:
            hdateti.dassert_has_tz(start_timestamp)
            start_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
                start_timestamp
            )
            select_query += f" WHERE timestamp >= {start_timestamp_as_unix}"
        if end_timestamp:
            hdateti.dassert_has_tz(end_timestamp)
            end_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
                end_timestamp
            )
            if start_timestamp:
                select_query += " AND "
            else:
                select_query += " WHERE "
            select_query += f" timestamp < {end_timestamp_as_unix}"
        # Read data.
        data = pd.read_sql_query(select_query, self.db_conn)
        return data
