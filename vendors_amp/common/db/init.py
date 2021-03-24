import logging
import os
from typing import List, Optional

import psycopg2
import psycopg2.sql as psql

import helpers.io_ as hio
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


def get_init_sql_files(custom_files: Optional[List[str]] = None) -> List[str]:
    """
    Return list of PostgreSQL initialization files in order of running.

    :param custom_fields: provider related init files
    :return: all files to init database
    """
    # Common files.
    files = [
        os.path.join(os.path.dirname(__file__), "../db/sql", filename)
        for filename in ("types.sql", "static.sql", "kibot.sql")
    ]
    # Extend with custom.
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
    """
    # Initialize connection.
    admin_connection, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # Create a database from scratch.
    hsql.create_database(admin_connection, db=dbname, force=force)
    # Close connection.
    admin_connection.close()
    # Initialize database.
    initialize_database(dbname, init_sql_files)


def initialize_database(dbname: str, init_sql_files: List[str]) -> None:
    # Connect to recently created database.
    connection, cursor = hsql.get_connection(
        dbname=dbname,
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # Make this database the copy of default.
    for sql_file in init_sql_files:
        _LOG.info("Executing %s...", sql_file)
        try:
            cursor.execute(hio.from_file(sql_file))
        except psycopg2.errors.DuplicateObject:
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
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # Drop database.
    cursor.execute(psql.SQL("DROP DATABASE {};").format(psql.Identifier(dbname)))
    # Close connection.
    connection.close()


def is_inside_im_container() -> bool:
    """
    Define if IM app is started.
    """
    condition = (
        (
            os.environ.get("STAGE") == "TEST"
            and os.environ.get("POSTGRES_HOST") == "im_postgres_test"
        )
        or (
            os.environ.get("STAGE") == "LOCAL"
            and os.environ.get("POSTGRES_HOST") == "im_postgres_local"
        )
    )
    return condition
