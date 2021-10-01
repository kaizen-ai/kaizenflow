"""
Create and handle the Postgres DB.

Import as:

import im.common.db.create_schema as icdcrsch
"""

import logging
import os
from typing import Optional, Tuple

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.sql as hsql
import helpers.system_interaction as sysint

_LOG = logging.getLogger(__name__)


# TODO(Grisha): convert the code into a class.

def get_db_connection_from_environment() -> Tuple[
    hsql.DbConnection, psycop.extensions.cursor
]:
    """
    Get connection and cursor for a SQL database using environment variables.

    Environment variables include:
        - Database name
        - Host
        - Port
        - Username
        - Password

    :return: connection and cursor for a SQL database
    """
    connection, cursor = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    return connection, cursor


def get_db_connection_details_from_environment() -> str:
    """
    Get database connection details using environment variables.

    Connection details include:
        - Database name
        - Host
        - Port
        - Username
        - Password

    :return: database connection details
    """
    txt = []
    txt.append("dbname='%s'" % os.environ["POSTGRES_DB"])
    txt.append("host='%s'" % os.environ["POSTGRES_HOST"])
    txt.append("port='%s'" % os.environ["POSTGRES_PORT"])
    txt.append("user='%s'" % os.environ["POSTGRES_USER"])
    txt.append("password='%s'" % os.environ["POSTGRES_PASSWORD"])
    txt = "\n".join(txt)
    return txt


def check_db_connection() -> None:
    """
    Verify that the database is available.
    """
    _LOG.info("Checking the database connection...")
    cmd = """
    postgres_ready() {
      pg_isready -d $POSTGRES_DB -p $POSTGRES_PORT -h $POSTGRES_HOST
    }
    
    echo "STAGE: $STAGE"
    echo "POSTGRES_HOST: $POSTGRES_HOST"
    echo "POSTGRES_PORT: $POSTGRES_PORT"
    
    until postgres_ready; do
      >&2 echo 'Waiting for PostgreSQL to become available...'
      sleep 1
    done
    >&2 echo 'PostgreSQL is available'
    """
    sysint.system(cmd, suppress_output=False)


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
    
    CREATE TABLE IF NOT EXISTS TradeSymbol (
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
    CREATE TABLE IF NOT EXISTS IbDailyData (
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
    
    CREATE TABLE IF NOT EXISTS IbMinuteData (
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
    
    CREATE TABLE IF NOT EXISTS IbTickBidAskData (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        bid numeric,
        ask numeric,
        volume bigint
    );
    
    CREATE TABLE IF NOT EXISTS IbTickData (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        price numeric,
        size bigint
    );
    """
    return sql_query


def get_kibot_create_table_query() -> str:
    """
    Get SQL query that is used to create tables for `kibot`.
    """
    sql_query = """
    CREATE TABLE IF NOT EXISTS KibotDailyData (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        date date,
        open numeric,
        high numeric,
        low numeric,
        close numeric,
        volume bigint,
        UNIQUE (trade_symbol_id, date)
    );
    
    CREATE TABLE IF NOT EXISTS KibotMinuteData (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        open numeric,
        high numeric,
        low numeric,
        close numeric,
        volume bigint,
        UNIQUE (trade_symbol_id, datetime)
    );
    
    CREATE TABLE IF NOT EXISTS KibotTickBidAskData (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        bid numeric,
        ask numeric,
        volume bigint
    );
    
    CREATE TABLE IF NOT EXISTS KibotTickData (
        id integer PRIMARY KEY DEFAULT nextval('serial'),
        trade_symbol_id integer REFERENCES TradeSymbol,
        datetime timestamp,
        price numeric,
        size bigint
    );
    """
    return sql_query


def define_data_types(cursor: psycop.extensions.cursor) -> None:
    """
    Define custom data types inside a database.

    :param cursor: a database cursor
    """
    _LOG.info("Defining data types...")
    # Define data types.
    define_types_query = """
    /* TODO: Futures -> futures */
    CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
    /* TODO: T -> minute, D -> daily */
    CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
    CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
    CREATE SEQUENCE serial START 1;
    """
    try:
        cursor.execute(define_types_query)
    except psycop.errors.DuplicateObject:
        _LOG.warning("Specified data types already exist: skipping.")


def create_tables(
    cursor: psycop.extensions.cursor,
) -> None:
    """
    Create tables inside a database.

    :param cursor: a database cursor
    """
    # Get SQL query to create the common tables.
    common_query = get_common_create_table_query()
    # Get SQL query to create the `kibot` tables.
    kibot_query = get_kibot_create_table_query()
    # Get SQL query to create the `ib` tables.
    ib_query = get_ib_create_table_query()
    # Collect the queries.
    provider_to_query = {
        "common": common_query,
        "kibot": kibot_query,
        "ib": ib_query,
    }
    # Create tables.
    for provider, query in provider_to_query:
        try:
            _LOG.info("Creating `%s` tables...", )
            cursor.execute(query)
        except psycop.errors.DuplicateObject:
            _LOG.warning("The `%s` tables are already created: skipping.", provider)


def test_tables(cursor: psycop.extensions.cursor) -> None:
    """
    Test that tables are created.

    :param cursor: a database cursor
    """
    _LOG.info("Testing created tables...")
    test_query = "INSERT INTO Exchange (name) VALUES ('TestExchange');"
    cursor.execute(test_query)


def create_schema() -> None:
    """
    Create SQL schema.

    Creating schema includes:
        - Defining custom data types
        - Creating new tables
        - Testing that tables are created
    """
    _LOG.info("DB connection:\n%s", get_db_connection_details_from_environment())
    # Get database connection and cursor.
    connection, cursor = get_db_connection_from_environment()
    # Define data types.
    define_data_types(cursor)
    # Create tables.
    create_tables(cursor)
    # Test the db.
    test_tables(cursor)
    # Close connection.
    connection.close()


def create_database(
    dbname: str,
    force: Optional[bool] = None,
) -> None:
    """
    Create database and SQL schema inside it.

    :param dbname: database name, e.g. `im_db_local`
    :param force: overwrite existing database
    """
    # Initialize connection.
    connection, _ = get_db_connection_from_environment()
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=dbname, force=force)
    connection.close()
    # Create SQL schema.
    create_schema()


def remove_database(dbname: str) -> None:
    """
    Remove database in current environment.

    :param dbname: database name, e.g. `im_db_local`
    """
    # Initialize connection.
    connection, cursor = get_db_connection_from_environment()
    # Drop database.
    cursor.execute(psql.SQL("DROP DATABASE {};").format(psql.Identifier(dbname)))
    # Close connection.
    connection.close()


# TODO(*): Move it to common/utils.py
def is_inside_im_container() -> bool:
    """
    Return whether we are running inside IM app.

    :return: True if running inside the IM app, False otherwise
    """
    # TODO(*): Why not testing only STAGE?
    condition = (
        os.environ.get("STAGE") == "TEST"
        and os.environ.get("POSTGRES_HOST") == "im_postgres_test"
    ) or (
        os.environ.get("STAGE") == "LOCAL"
        and os.environ.get("POSTGRES_HOST") == "im_postgres_local"
    )
    return condition
