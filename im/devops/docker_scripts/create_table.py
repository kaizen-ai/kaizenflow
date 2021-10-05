#!/usr/bin/env python
"""
Insert a table into the local database.

Use example:

> python create_table.py \
    --table_name 'ccxt_ohlcv'

Import as:

import im.devops.create_table as imdecrtab
"""

import argparse
import logging

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import im.common.db.create_schema as imcodbcrsch

_LOG = logging.getLogger(__name__)


def _get_create_table_sql_command(table_name: str) -> str:
    """
    Build a CREATE TABLE command based on table name.

    :param table_name: name of the table
    :return: CREATE TABLE command
    """
    if table_name == "ccxt_ohlcv":
        command = """CREATE TABLE ccxt_ohlcv(
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
        raise ValueError("Table '%s' is not valid" % table_name)
    _LOG.debug("command=%s", command)
    return command


def create_table(conn: hsql.DbConnection, table_name: str) -> None:
    """
    Create a new table in the database.

    Accepts a table name and creates a table
    with preset schema.

    Currently available tables:
        - 'ccxt_ohlcv': OHLCV table with CCXT data

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
    command = _get_create_table_sql_command(table_name)
    cursor.execute(command)
    cursor.close()
    # TODO (Danya): Remove cloing from function
    conn.close()


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
    conn, _ = imcodbcrsch.get_db_connection_from_environment()
    create_table(conn, args.table_name)


if __name__ == "__main__":
    _main(_parse())
