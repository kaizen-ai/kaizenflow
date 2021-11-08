"""
Create and handle the Postgres DB.

Import as:

import im.common.db.create_db as imcodbcrdb
"""

import logging
from typing import Optional

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.dbg as hdbg
import helpers.sql as hsql
import im.ccxt.db.utils as imccdbuti
import im.ib.sql_writer as imibsqwri
import im.kibot.sql_writer as imkisqwri

_LOG = logging.getLogger(__name__)


def get_common_create_table_query() -> str:
    """
    Get SQL query that is used to create tables for common usage.
    """
    sql_query = """
    CREATE TABLE IF NOT EXISTS Exchange (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        name text UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Symbol (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        code text UNIQUE,
        description text,
        asset_class AssetClass,
        start_date date DEFAULT CURRENT_DATE,
        symbol_base text
    );

    CREATE TABLE IF NOT EXISTS TRADE_SYMBOL (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        exchange_id integer REFERENCES Exchange,
        symbol_id integer REFERENCES Symbol,
        UNIQUE (exchange_id, symbol_id)
    );
    """
    return sql_query


def get_data_types_query() -> str:
    """
    Define custom data types inside a database.
    """
    # Define data types.
    # TODO(Grisha): Futures -> futures.
    # TODO(Grisha): T -> minute, D -> daily.
    query = """
    CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
    CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
    CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
    CREATE SEQUENCE serial START 1;
    """
    return query


def create_all_tables(connection: hsql.DbConnection) -> None:
    """
    Create tables inside a database.

    :param connection: a database connection
    """
    queries = [
        get_data_types_query(),
        get_common_create_table_query(),
        imibsqwri.get_create_table_query(),
        imkisqwri.get_create_table_query(),
        imccdbuti.get_ccxt_ohlcv_create_table_query(),
        imccdbuti.get_exchange_name_create_table_query(),
        imccdbuti.get_currency_pair_create_table_query(),
    ]
    # Create tables.
    for query in queries:
        _LOG.debug("Executing query %s", query)
        try:
            cursor = connection.cursor()
            cursor.execute(query)
        except psycop.errors.DuplicateObject:
            _LOG.warning("Duplicate table created, skipping.")


# TODO(Grisha): convert it to test.
def test_tables(
    connection: hsql.DbConnection,
) -> None:
    """
    Test that tables are created.

    :param connection: a database connection
    """
    _LOG.info("Testing created tables...")
    # Check tables list.
    actual_tables = hsql.get_table_names(connection)
    expected_tables = [
        "ccxt_ohlcv",
        "currency_pair",
        "exchange",
        "exchange_name",
        "ib_daily_data",
        "ib_minute_data",
        "ib_tick_bid_ask_data",
        "ib_tick_data",
        "kibot_daily_data",
        "kibot_minute_data",
        "kibot_tick_bid_ask_data",
        "kibot_tick_data",
        "symbol",
        "trade_symbol",
    ]
    hdbg.dassert_set_eq(actual_tables, expected_tables)
    # Execute the test query.
    test_query = "INSERT INTO Exchange (name) VALUES ('TestExchange');"
    cursor = connection.cursor()
    cursor.execute(test_query)


def create_database(
    connection: hsql.DbConnection,
    new_db: str,
    force: Optional[bool] = None,
) -> None:
    """
    Create database and SQL schema inside it.

    :param connection: a database connection
    :param new_db: name of database to connect to, e.g. `im_db_local`
    :param force: overwrite existing database
    """
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=new_db, force=force)
    # Create SQL schema.
    create_all_tables(connection)


def remove_database(connection: hsql.DbConnection, db_to_drop: str) -> None:
    """
    Remove database in current environment.

    :param connection: a database connection
    :param db_to_drop: database name to drop, e.g. `im_db_local`
    """
    # Drop database.
    connection.cursor().execute(
        psql.SQL("DROP DATABASE {};").format(psql.Identifier(db_to_drop))
    )
