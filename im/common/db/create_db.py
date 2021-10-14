"""
Create and handle the Postgres DB.

Import as:

import im.common.db.create_schema as imcodbcrsch
"""

import logging
import os
from typing import Optional

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.dbg as hdbg
import helpers.sql as hsql
import im.common.db.utils as imcodbuti
import im.ib.sql_writer as imibsqwri
import im.kibot.sql_writer as imkisqwri

_LOG = logging.getLogger(__name__)


# TODO(Grisha): convert the code into a class.


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


def get_data_types_query(cursor: psycop.extensions.cursor) -> None:
    """
    Define custom data types inside a database.

    :param cursor: a database cursor
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
    try:
        cursor.execute(query)
    except psycop.errors.DuplicateObject:
        _LOG.warning("Specified data types already exist: skipping.")


def create_all_tables(
    cursor: psycop.extensions.cursor,
) -> None:
    """
    Create tables inside a database.

    :param cursor: a database cursor
    """
    queries = [
        get_data_types_query(cursor),
        get_common_create_table_query(),
        imkisqwri.get_create_table_query(),
        imibsqwri.get_create_table_query()
    ]
    for query in queries:
        cursor.execute(query)


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


def create_schema(
    db_name: str,
    host: str,
    user: str,
    port: int,
    password: str,
) -> None:
    """
    Create SQL schema.

    Creating schema includes:
        - Defining custom data types
        - Creating new tables
        - Testing that tables are created

    :param db_name: name of database to connect to, e.g. `im_db_local`
    :param host: host name to connect to db
    :param user: user name to connect to db
    :param port: port to connect to db
    :param password: password to connect to db
    """
    _LOG.info(
        "DB connection:\n%s",
        imcodbuti.db_connection_to_str(
            db_name=db_name, host=host, user=user, port=port, password=password
        ),
    )
    # Get database connection and cursor.
    connection, cursor = hsql.get_connection(
        dbname=db_name,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Define data types.
    get_data_types_query(cursor)
    # Create tables.
    create_all_tables(cursor)
    # Test the db.
    test_tables(connection, cursor)
    # Close connection.
    connection.close()


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
    create_schema(
        db_name=conn_db, host=host, user=user, port=port, password=password
    )


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
