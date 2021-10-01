#!/usr/bin/env python
"""
Import as:

import im.devops.insert_db as imdeindb
"""

import argparse
import logging
import os

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql

_LOG = logging.getLogger(__name__)


def _get_create_table_command(table_name: str) -> str:
    """
    Build a CREATE TABLE command based on table name.

    Currently available tables:
        - 'ccxtohlcv': OHLCV table with CCXT data

    :param table_name: name of the table
    :return: CREATE TABLE command
    """
    if table_name == "ccxtohlcv":
        command = """
                CREATE TABLE ccxtohlcv (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                open NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                epoch INTEGER NOT NULL,
                currency_pair VARCHAR(255) NOT NULL,
                exchange_id VARCHAR(255) NOT NULL
                )
                """
    else:
        hdbg.dfatal("Table %s is not available for creation", table_name)
    return command


def create_table(conn: hsql.DbConnection, table_name: str) -> None:
    """
    Create a new table in the database.

    Accepts a table name and creates a table
    with preset schema.

    Currently available tables:
        - 'ccxtohlcv': OHLCV table with CCXT data

    #TODO (Danya): Add support for custom schemas (e.g. from file)

    :param conn: DB connection
    :param table_name: name of the table
    """
    hdbg.dassert_not_in(
        table_name,
        hsql.get_table_names(conn),
        msg="Table %s already exists!" % table_name,
    )
    cursor = conn.cursor()
    # Build a CREATE TABLE command from name.
    command = _get_create_table_command(table_name)
    cursor.execute(command)
    print(hsql.get_table_names(conn))
    cursor.close()
    conn.commit()
    return None


# +
def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--table_name",
        action="store",
        required=True,
        type=str,
        help="Name of table to create",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    conn, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    create_table(conn, args.table_name)
    return None
