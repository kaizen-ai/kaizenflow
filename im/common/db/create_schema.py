"""
Create and handle the Postgres DB.

Import as:

import im.common.db.create_schema as icdcrsch
"""

import logging
import os
from typing import List, Optional, Tuple

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.io_ as hio
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


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


def get_sql_files(custom_files: Optional[List[str]] = None) -> List[str]:
    """
    Get SQL files with `CREATE TABLE` instructions.

    :param custom_files: provider-specific sql files
    :return: SQL files with `CREATE TABLE` instructions
    """
    # Common files.
    files = [
        os.path.join(os.path.dirname(__file__), "../db/sql", filename)
        for filename in (
            "static.sql",
            "kibot.sql",
            "ib.sql",
        )
    ]
    # Extend with custom files, if needed.
    if custom_files:
        files.extend(custom_files)
    return files


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
    _LOG.debug("Executing query '%s'...", define_types_query)
    try:
        cursor.execute(define_types_query)
    except psycop.errors.DuplicateObject:
        _LOG.warning("Specified data types already exist: skipping.")


def create_tables(
    cursor: psycop.extensions.cursor,
    custom_files: Optional[List[str]] = None,
) -> None:
    """
    Create tables inside a database.

    :param cursor: a database cursor
    :param custom_files: provider-specific sql files
    """
    _LOG.info("Creating tables...")
    sql_files = get_sql_files(custom_files)
    # Create tables.
    for sql_file in sql_files:
        _LOG.debug("Creating tables from '%s'...", sql_file)
        sql_query = hio.from_file(sql_file)
        try:
            cursor.execute(sql_query)
        except psycop.errors.DuplicateObject:
            _LOG.warning("Schemas are already created: skipping.")
            break


def test_tables(cursor: psycop.extensions.cursor) -> None:
    """
    Test that tables are created.

    :param cursor: a database cursor
    """
    _LOG.info("Testing created tables...")
    test_query = "INSERT INTO Exchange (name) VALUES ('TestExchange');"
    _LOG.debug("Executing query '%s'...", test_query)
    cursor.execute(test_query)


def create_schema(custom_files: Optional[List[str]] = None) -> None:
    """
    Create SQL schema.

    Creating schema includes:
        - Defining custom data types
        - Creating new tables
        - Testing that tables are created

    :param custom_files: provider-specific sql files
    """
    _LOG.info("DB connection:\n%s", get_db_connection_details_from_environment())
    # Get database connection and cursor.
    connection, cursor = get_db_connection_from_environment()
    # Define data types.
    define_data_types(cursor)
    # Create tables.
    create_tables(cursor, custom_files)
    # Test the db.
    test_tables(cursor)
    # Close connection.
    connection.close()


def create_database(
    dbname: str,
    custom_files: Optional[List[str]] = None,
    force: Optional[bool] = None,
) -> None:
    """
    Create database and SQL schema inside it.

    :param dbname: database name, e.g. `im_db_local`
    :param custom_files: provider-specific sql files
    :param force: overwrite existing database
    """
    # Initialize connection.
    connection, _ = get_db_connection_from_environment()
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=dbname, force=force)
    connection.close()
    # Create SQL schema.
    create_schema(custom_files)


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
