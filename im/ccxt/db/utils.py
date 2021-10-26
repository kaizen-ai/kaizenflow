"""
Utilities for working with CCXT database.

Import as:

import im.ccxt.db.insert_data as imccdbuti
"""

import io
import logging

import pandas as pd
import psycopg2.extras as extras

import helpers.dbg as hdbg
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


def get_ccxt_ohlcv_create_table_query() -> str:
    """
    Get SQL query to create CCXT OHLCV table.
    """
    query = """CREATE TABLE ccxt_ohlcv(
                id SERIAL PRIMARY KEY,
                timestamp BIGINT NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                currency_pair VARCHAR(255) NOT NULL,
                exchange_id VARCHAR(255) NOT NULL
                )
                """
    return query


def get_exchange_name_create_table_query() -> str:
    """
    Get SQL query to define CCXT crypto exchange names.
    """
    query = """CREATE TABLE exchange_name(
            exchange_id SERIAL PRIMARY KEY,
            exchange_name VARCHAR(255) NOT NULL
            )
            """
    return query


def get_currency_pair_create_table_query() -> str:
    """
    Get SQL query to define CCXT currency pairs.
    """
    query = """CREATE TABLE currency_pair(
            currency_pair_id SERIAL PRIMARY KEY,
            currency_pair VARCHAR(255) NOT NULL
            )
            """
    return query


def copy_rows_with_copy_from(
    connection: hsql.DbConnection, df: pd.DataFrame, table_name: str
) -> None:
    """
    Copy dataframe contents into DB directly from buffer.

    This function works much faster for large dataframes (>10000 rows).

    :param connection: DB connection
    :param df: data to insert
    :param table_name: name of the table for insertion
    """
    hdbg.dassert_in(
        table_name, hsql.get_table_names(connection)
    )
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur = connection.cursor()
    cur.copy_from(buffer, table_name, header=False)
    connection.commit()


def populate_exchange_currency_tables(conn: hsql.DbConnection) -> None:
    """
    Populate exchange name and currency pair tables with data.

    :param conn: DB connection
    """
    # Extract a list of all CCXT exchange names.
    all_exchange_names = pd.Series(ccxt.exchanges)
    # Create a dataframe with exchange names and ids.
    df_exchange_names = all_exchange_names.reset_index()
    df_exchange_names.columns = ["exchange_id", "exchange_name"]
    # Insert exchange names dataframe in DB.
    execute_insert_query(conn, df_exchange_names, "exchange_name")
    # Create an empty list for currency pairs.
    currency_pairs = []
    # Extract all the currency pairs for each exchange and append them.
    # to the currency pairs list.
    for exchange_name in all_exchange_names:
        # Some few exchanges require credentials for this info so we omit them.
        try:
            exchange_class = getattr(ccxt, exchange_name)()
            exchange_currency_pairs = list(exchange_class.load_markets().keys())
            currency_pairs.extend(exchange_currency_pairs)
        # Continue cycle if some of the following errors appear since all of
        # them are related to denied access to the requested data for only 6
        # exchanges that are far from our scope.
        except(ccxt.AuthenticationError, ccxt.NetworkError, TypeError) as e:
            continue
    # Create a dataframe with currency pairs and ids.
    currency_pairs_srs = pd.Series(sorted(list(set(currency_pairs))))
    df_currency_pairs = currency_pairs_srs.reset_index()
    df_currency_pairs.columns = ["currency_pair_id", "currency_pair"]
    # Insert currency pairs dataframe in DB.
    execute_insert_query(conn, df_currency_pairs, "currency_pair")


def execute_insert_query(
    connection: hsql.DbConnection, df: pd.DataFrame, table_name: str
) -> None:
    """
    Insert multiple rows into the database.

    :param connection: connection to the DB
    :param df: data to insert
    :param table_name: name of the table for insertion
    """
    hdbg.dassert_in(
        table_name, hsql.get_table_names(connection))
    # Transform dataframe into list of tuples.
    values = [tuple(v) for v in df.to_numpy()]
    # Generate a query for multiple rows.
    query = _create_insert_query(df, table_name)
    # Execute query for each provided row.
    cur = connection.cursor()
    extras.execute_values(cur, query, values)
    connection.commit()


def _create_insert_query(df: pd.DataFrame, table_name: str) -> str:
    """
    Create an INSERT query.

    Example:

    INSERT INTO ccxt_ohlcv(timestamp,open,high,low,close) VALUES %s

    :param df: data to insert into DB
    :param table_name: name of the table for insertion
    :return: INSERT command
    """
    columns = ",".join(list(df.columns))
    query = f"INSERT INTO {table_name}({columns}) VALUES %s"
    _LOG.debug("query=%s", query)
    return query
