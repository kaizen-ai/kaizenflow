"""
Utilities for working with CCXT database.

Import as:

import im.ccxt.db.utils as imccdbuti
"""

import io
import logging

import pandas as pd
import psycopg2.extras as extras

import helpers.dbg as hdbg
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


# TODO(gp): -> get_create_ccxt_ohlcv_table_query()
def get_ccxt_ohlcv_create_table_query() -> str:
    """
    Get SQL query to create CCXT OHLCV table.
    """
    query = """
    CREATE TABLE IF NOT EXISTS ccxt_ohlcv(
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
    query = """
    CREATE TABLE IF NOT EXISTS exchange_name(
            exchange_id SERIAL PRIMARY KEY,
            exchange_name VARCHAR(255) NOT NULL
            )
            """
    return query


def get_currency_pair_create_table_query() -> str:
    """
    Get SQL query to define CCXT currency pairs.
    """
    query = """
    CREATE TABLE IF NOT EXISTS currency_pair(
            currency_pair_id SERIAL PRIMARY KEY,
            currency_pair VARCHAR(255) NOT NULL
            )
            """
    return query


# #############################################################################


def populate_exchange_currency_tables(conn: hsql.DbConnection) -> None:
    """
    Populate exchange name and currency pair tables with data from CCXT.

    :param conn: DB connection
    """
    # Extract the list of all CCXT exchange names.
    all_exchange_names = pd.Series(ccxt.exchanges)
    # Create a dataframe with exchange ids and names.
    df_exchange_names = all_exchange_names.reset_index()
    df_exchange_names.columns = ["exchange_id", "exchange_name"]
    # Insert exchange names dataframe in DB.
    hsql.execute_insert_query(conn, df_exchange_names, "exchange_name")
    # Create an empty list for currency pairs.
    currency_pairs = []
    # Extract all the currency pairs for each exchange and append them to the
    # currency pairs list.
    for exchange_name in all_exchange_names:
        # Some few exchanges require credentials for this info so we omit them.
        try:
            exchange_class = getattr(ccxt, exchange_name)()
            exchange_currency_pairs = list(exchange_class.load_markets().keys())
            currency_pairs.extend(exchange_currency_pairs)
        except (ccxt.AuthenticationError, ccxt.NetworkError, TypeError) as e:
            # Continue since these errors are related to denied access for 6
            # exchanges that we ignore.
            _LOG.warning("Skipping exchange_name='%s'", exchange_name)
            continue
    # Create a dataframe with currency pairs and ids.
    currency_pairs_srs = pd.Series(sorted(list(set(currency_pairs))))
    df_currency_pairs = currency_pairs_srs.reset_index()
    df_currency_pairs.columns = ["currency_pair_id", "currency_pair"]
    # Insert currency pairs dataframe in DB.
    hsql.execute_insert_query(conn, df_currency_pairs, "currency_pair")
