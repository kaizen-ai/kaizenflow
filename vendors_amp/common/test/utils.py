import datetime
import os
import time
import pandas as pd
import psycopg2
import psycopg2.sql as psql
import pytest

from typing import List
import helpers.io_ as hio
import helpers.unit_test as hut
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.kibot.data.load.sql_data_loader as vkdlsq
import vendors_amp.kibot.sql_writer_backend as vksqlw


def get_init_sql_files(custom_files: List[str]) -> List[str]:
    """
    Return list of PostgreSQL initialization files in order of running.

    :param custom_fields: provider related init files
    :return: all files to init database
    """
    # Common files.
    files = [os.path.join(os.path.dirname(__file__), "../compose/init_sql", filename) for filename in ("types.sql", "static.sql")]
    # Extend with custom.
    files.extend(custom_files)
    return files

# TODO(plyq): Move it to common place, e.g. helpers.
def create_database(dbname: str, init_sql_files: List[str]) -> None:
    """
    Create database in current environment.

    :param dbname: database to create
    :param init_sql_files: files to run to init database
    """
    # Initialize connection.
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    default_dbname = os.environ["POSTGRES_DB"]
    admin_connection = psycopg2.connect(
        dbname=default_dbname,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Make DROP/CREATE DATABASE executable from transaction block.
    admin_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Create a database from scratch.
    with admin_connection:
        with admin_connection.cursor() as cursor:
            cursor.execute(
                psql.SQL("DROP DATABASE IF EXISTS {};").format(
                    psql.Identifier(dbname)
                )
            )
            cursor.execute(
                psql.SQL("CREATE DATABASE {};").format(psql.Identifier(dbname))
            )
    # Close connection.
    admin_connection.close()
    # Connect to recently created database.
    connection = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Make this database the copy of default.
    with connection:
        with connection.cursor() as cursor:
            print(init_sql_files)
            for sql_file in init_sql_files:
                cursor.execute(hio.from_file(sql_file))
    # Close connection.
    connection.close()


# TODO(plyq): Move it to common place, e.g. helpers.
def remove_database(dbname: str) -> None:
    """
    Remove database in current environment.
    """
    # Initialize connection.
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    default_dbname = os.environ["POSTGRES_DB"]
    connection = psycopg2.connect(
        dbname=default_dbname,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Make DROP DATABASE executable from transaction block.
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Drop database.
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                psql.SQL("DROP DATABASE {};").format(psql.Identifier(dbname))
            )
    # Close connection.
    connection.close()
