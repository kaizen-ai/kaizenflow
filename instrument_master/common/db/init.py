"""
Creates and handles the Postgres DB.

Import as:

import instrument_master.common.db.init as init
"""

import logging
import os
from typing import List, Optional

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.io_ as hio
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


def get_db_connection() -> psycop.extensions.connection:
    conn, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    return conn


def get_connection_details() -> str:
    txt = []
    txt.append("dbname='%s'" % os.environ["POSTGRES_DB"])
    txt.append("host='%s'" % os.environ["POSTGRES_HOST"])
    txt.append("port='%s'" % os.environ["POSTGRES_PORT"])
    txt.append("user='%s'" % os.environ["POSTGRES_USER"])
    txt.append("password='%s'" % os.environ["POSTGRES_PASSWORD"])
    txt = "\n".join(txt)
    return txt


def get_init_sql_files(custom_files: Optional[List[str]] = None) -> List[str]:
    """
    Return the PostgreSQL initialization scripts in proper execution order.

    :param custom_files: provider related init files
    :return: all files needed to initialize the database
    """
    # Common files.
    files = [
        os.path.join(os.path.dirname(__file__), "../db/sql", filename)
        for filename in (
            "types.sql",
            "static.sql",
            "kibot.sql",
            "ib.sql",
            "test.sql",
        )
    ]
    # Extend with custom files, if needed.
    if custom_files:
        files.extend(custom_files)
    return files


def create_database(
    dbname: str, init_sql_files: List[str], force: Optional[bool] = None
) -> None:
    """
    Create database in current environment.

    :param dbname: database to create
    :param init_sql_files: files to run to init database
    :param force: overwrite existing database
    """
    # Initialize connection.
    # TODO(*): Factor out this common part.
    connection, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    _LOG.debug("connection=%s", connection)
    # Create database.
    hsql.create_database(connection, db=dbname, force=force)
    connection.close()
    # Initialize database.
    initialize_database(dbname, init_sql_files)


def initialize_database(dbname: str, init_sql_files: List[str]) -> None:
    """
    Execute init scripts on database.
    """
    _LOG.info("DB connection:\n%s", get_connection_details())
    # Connect to recently created database.
    connection, cursor = hsql.get_connection(
        dbname=dbname,
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # Execute the init scripts.
    for sql_file in init_sql_files:
        _LOG.info("Executing %s...", sql_file)
        try:
            cursor.execute(hio.from_file(sql_file))
        except psycop.errors.DuplicateObject:
            _LOG.warning(
                "Database %s already initialized. Initialization stopped.", dbname
            )
            break
    # Close connection.
    connection.close()


def remove_database(dbname: str) -> None:
    """
    Remove database in current environment.
    """
    # Initialize connection.
    connection, cursor = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # Drop database.
    cursor.execute(psql.SQL("DROP DATABASE {};").format(psql.Identifier(dbname)))
    # Close connection.
    connection.close()


# TODO(*): Move it to common/utils.py
def is_inside_im_container() -> bool:
    """
    Return whether we are running inside IM app.
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
