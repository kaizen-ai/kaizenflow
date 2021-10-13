"""
Create and manage the Postgres DB.

Since there is coupling between the data providers (e.g., Kibot and IB share some
tables) we keep all the schema initialized here.

Import as:

import im.common.db.create_schema as imcodbcrsch
"""

# TODO(gp): This file should become `create_db` since it creates the DB and its
#  schema.

import logging
import os
import time
from typing import Optional

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.dbg as hdbg
import helpers.sql as hsql
import helpers.system_interaction as hsyint

_LOG = logging.getLogger(__name__)


# TODO(Grisha): convert the code into a class.


# TODO(gp): -> db_connection_to_str() and move to utils.py
# TODO(gp): It should get a DbConnection.
def get_db_connection_details(
    db_name: str, host: str, user: str, port: int, password: str
) -> str:
    """
    Get database connection details using environment variables.

    Connection details include:
        - Database name
        - Host
        - Port
        - Username
        - Password

    :param db_name: name of database to connect to, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    :return: database connection details
    """
    txt = []
    txt.append("dbname='%s'" % db_name)
    txt.append("host='%s'" % host)
    txt.append("port='%s'" % port)
    txt.append("user='%s'" % user)
    txt.append("password='%s'" % password)
    txt = "\n".join(txt)
    return txt


# TODO(gp): Move to utils.py
# TODO(gp): It should get a DbConnection.
def check_db_connection(
    db_name: str,
    host: str,
    user: str,
    port: int,
    password: str,
) -> None:
    """
    Verify that the database is available.

    :param db_name: name of database to connect to, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    """
    _LOG.info(
        "Checking the database connection:\n%s",
        get_db_connection_details(
            db_name=db_name, host=host, user=user, port=port, password=password
        ),
    )
    while True:
        _LOG.info("Waiting for PostgreSQL to become available...")
        cmd = "pg_isready -d %s -p %s -h %s"
        rc = hsyint.system(
            cmd
            % (
                db_name,
                port,
                host,
            )
        )
        time.sleep(1)
        if rc == 0:
            _LOG.info("PostgreSQL is available")
            break


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


# TODO(gp): This should go in IB.
def get_ib_create_table_query() -> str:
    """
    Get SQL query that is used to create tables for IB.
    """
    # TODO(*): barCount -> bar_count
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


# TODO(gp): This should go in Kibot.
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


def get_data_types_query() -> str:
    """
    Define custom data types inside a database.
    """
    sql_query = """
    /* TODO: Futures -> futures */
    CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
    /* TODO: T -> minute, D -> daily */
    CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
    CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
    CREATE SEQUENCE serial START 1;
    """
    return sql_query


# TODO(gp): -> create_all_tables
# TODO(gp): It should get a DbConnection
def create_tables(
    cursor: psycop.extensions.cursor,
) -> None:
    """
    Create tables inside a database.

    :param cursor: a database cursor
    """
    queries = [
        get_data_types_query(),
        get_common_create_table_query(),
        get_kibot_create_table_query(),
        get_ib_create_table_query(),
    ]
    # Create tables.
    for query in queries:
        try:
            cursor.execute(query)
        except psycop.errors.DuplicateObject:
            _LOG.warning(
                "The `%s` tables are already created: skipping.", provider
            )


# TODO(gp): This is a unit test and should be split into IB and Kibot at some point.
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
    # TODO(gp): These names are difficult to read: they should be like "ib_minute_data".
    expected_tables = [
        "exchange",
        "ibdailydata",
        "ibminutedata",
        "ibtickbidaskdata",
        "ibtickdata",
        "kibotdailydata",
        "kibotminutedata",
        "kibottickbidaskdata",
        "kibottickdata",
        "symbol",
        "tradesymbol",
    ]
    hdbg.dassert_set_eq(actual_tables, expected_tables)
    # Execute the test query.
    test_query = "INSERT INTO Exchange (name) VALUES ('TestExchange');"
    cursor.execute(test_query)


# TODO(gp): This should get a DbConnection object.
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
    connection, _ = hsql.get_connection(
        dbname=conn_db, host=host, user=user, port=port, password=password
    )
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=new_db, force=force)
    connection.close()
    # Create SQL schema.
    create_tables()


# TODO(gp): This should get a DbConnection object.
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
