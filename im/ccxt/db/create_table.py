#!/usr/bin/env python
"""
Insert a table into the database.

Use example:

> create_table.py \
    --table_name 'ccxt_ohlcv'

Import as:

import im.ccxt.db.create_table as imccdbcrtab
"""

import argparse
import logging

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import os

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
                timestamp INTEGER NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
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

    Example of OHLCV data from CCXT:
    timestamp,open,high,low,close,volume,currency_pair,exchange
    1632745560000,43480.71,43489.87,43435.0,43460.31,18.85418,BTC/USDT,binance
    1632745620000,43460.32,43467.68,43420.59,43445.21,39.4702,BTC/USDT,binance

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
    # TODO(Danya): Remove closing from function.
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
    # Get connection using env variables.
    conn, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    create_table(conn, args.table_name)


if __name__ == "__main__":
    _main(_parse())
