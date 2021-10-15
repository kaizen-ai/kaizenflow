"""
Create and handle the Postgres DB.

Import as:

import im.common.db.create_db as imcodbcrdb
"""

import logging
import os
from typing import Optional

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.dbg as hdbg
import helpers.sql as hsql
import helpers.system_interaction as hsyint
import im.common.db.utils as imcodbuti
import im.kibot.sql_writer as imkkisqwribac


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


def get_ib_create_table_query() -> str:
    """
    Get SQL query that is used to create tables for `ib`.
    """
    sql_query = """
    CREATE TABLE IF NOT EXISTS IB_DAILY_DATA (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        date date,
        open numeric,
        high numeric,
        low numeric,
        close numeric,
        volume bigint,
        average numeric,
        -- TODO(*): barCount -> bar_count
        barCount integer,
        UNIQUE (trade_symbol_id, date)
    );

    CREATE TABLE IF NOT EXISTS IB_MINUTE_DATA (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamptz,
        open numeric,
        high numeric,
        low numeric,
        close numeric,
        volume bigint,
        average numeric,
        barCount integer,
        UNIQUE (trade_symbol_id, datetime)
    );

    CREATE TABLE IF NOT EXISTS IB_TICK_BID_ASK_DATA (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        bid numeric,
        ask numeric,
        volume bigint
    );

    CREATE TABLE IF NOT EXISTS IB_TICK_DATA (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        price numeric,
        size bigint
    );
    """
    return sql_query


def get_data_types_query() -> str:
    """
    Define custom data types inside a database.
    """
    # Define data types.
    query = """
    /* TODO: Futures -> futures */
    CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
    /* TODO: T -> minute, D -> daily */
    CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
    CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
    CREATE SEQUENCE serial START 1;
    """
    return query


def create_all_tables(
    cursor: psycop.extensions.cursor,
) -> None:
    """
    Create tables inside a database.

    :param cursor: a database cursor
    """
    queries = [
        get_data_types_query(),
        get_common_create_table_query(),
        get_ib_create_table_query(),
        imkkisqwribac.get_create_table_query(),
    ]

    # Create tables.
    for query in queries:
        try:
            cursor.execute(query)
        except psycop.errors.DuplicateObject:
            _LOG.warning("Duplicate table created, skipping.")


def test_tables(
    connection: hsql.DbConnection,
    cursor: psycop.extensions.cursor,
) -> None:
    """
    Test that tables are created.

    :param connection: a database connection
    :param cursor: a database cursor
    """
    _LOG.info("Testing created tables...")
    # Check tables list.
    actual_tables = hsql.get_table_names(connection)
    expected_tables = [
        "exchange",
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
    cursor.execute(test_query)


def create_database(
    new_db: str,
    conn_db: str,
    host: str,
    user: str,
    port: int,
    password: str,
    force: Optional[bool] = None,
) -> None:
    """
    Create database and SQL schema inside it.

    :param new_db: name of database to connect to, e.g. `im_db_local`
    :param conn_db: name of database to create, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    :param force: overwrite existing database
    """
    # Initialize connection.
    connection, cursor = hsql.get_connection(
        dbname=conn_db, host=host, user=user, port=port, password=password
    )
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=new_db, force=force)
    connection.close()
    # Create SQL schema.
    # TODO(Danya): remove cursor and pass connection (#169).
    create_all_tables(cursor)


def remove_database(
    db_to_drop: str,
    conn_db: str,
    host: str,
    user: str,
    port: int,
    password: str,
) -> None:
    """
    Remove database in current environment.

    :param db_to_drop: database name to drop, e.g. `im_db_local`
    :param conn_db: name of database to connect, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    """
    # Initialize connection.
    connection, cursor = hsql.get_connection(
        dbname=conn_db, host=host, user=user, port=port, password=password
    )
    # Drop database.
    cursor.execute(
        psql.SQL("DROP DATABASE {};").format(psql.Identifier(db_to_drop))
    )
    # Close connection.
    connection.close()
