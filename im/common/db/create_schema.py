"""
Creates and handles the Postgres DB.

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


def get_db_connection_from_environment() -> Tuple[hsql.DbConnection, psycop.extensions.cursor]:
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


def get_sql_schema_files(custom_files: Optional[List[str]] = None) -> List[str]:
    """
    Return the PostgreSQL initialization scripts in proper execution order.

    :param custom_files: provider related init files
    :return: all files needed to initialize the database
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


def create_database(
    dbname: str, sql_schemas: List[str], force: Optional[bool] = None
) -> None:
    """
    Create database in current environment.

    :param dbname: database name, e.g. `im_db_local`
    :param sql_schemas:
    :param force: overwrite existing database
    """
    # Initialize connection.
    connection, _ = get_db_connection_from_environment()
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=dbname, force=force)
    connection.close()
    # Initialize database.
    initialize_database(dbname, sql_schemas)


def define_data_types(db_connection: hsql.DbConnection) -> None:
    # Define data types.
    define_types_query = """
    /* TODO: Futures -> futures */
    CREATE TYPE AssetClass AS ENUM ('Futures', 'etfs', 'forex', 'stocks', 'sp_500');
    /* TODO: T -> minute, D -> daily */
    CREATE TYPE Frequency AS ENUM ('T', 'D', 'tick');
    CREATE TYPE ContractType AS ENUM ('continuous', 'expiry');
    CREATE SEQUENCE serial START 1;
    """
    _LOG.info("Defining data types with query %s.", define_types_query)
    try:
        hsql.execute_query(db_connection, define_types_query)
    except psycop.errors.DuplicateObject:
        _LOG.warning("Specified data types already exist. Terminated.")


def create_schemas(
    db_connection: hsql.DbConnection,
    custom_files: Optional[List[str]] = None,
) -> None:
    schema_files = get_sql_schema_files(custom_files)
    # Create schemas.
    for schema_file in schema_files:
        _LOG.info("Executing %s...", schema_file)
        sql_query = hio.from_file(schema_file)
        try:
            hsql.execute_query(db_connection, sql_query)
        except psycop.errors.DuplicateObject:
            _LOG.warning(
                "Schemas are already created. Terminated."
            )
            break


def test_db(db_connection: hsql.DbConnection) -> None:
    test_query = "INSERT INTO Exchange (name) VALUES ('TestExchange');"
    _LOG.info("Testing db by executing query %s", test_query)
    try:
        hsql.execute_query(db_connection, test_query)
    except psycop.Error:
        _LOG.warning("Test failed with error %s", psycop.Error)


def initialize_database(dbname: str, sql_schemas: List[str]) -> None:
    """
    Execute init scripts on database.

    :param dbname: database name, e.g. `im_db_local`
    :param sql_schemas: init
    """
    _LOG.info(
        "DB connection:\n%s",
        get_db_connection_details_from_environment()
    )
    # Connect to recently created database.
    connection, _ = get_db_connection_from_environment()
    # Define data types.
    define_data_types(connection)
    # Create schemas.
    create_schemas(connection, sql_schemas)
    # Test the db.
    test_db(connection)
    # Close connection.
    connection.close()


def remove_database(dbname: str) -> None:
    """
    Remove database in current environment.

    :param dbname: database name, e.g. `im_db_local`
    :return:
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
