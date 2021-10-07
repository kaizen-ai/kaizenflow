"""
Utilities for inserting data into DB table.

Import as:

import im.ccxt.db.insert_data as imccdbindat
"""

import io
import logging

import pandas as pd

import helpers.parser as hparser
import helpers.dbg as hdbg
import helpers.sql as hsql
import argparse
import im.common.db.create_schema as imcodbcrsch

_LOG = logging.getLogger(__name__)


def copy_rows_from_buffer(
    connection: hsql.DbConnection, rows: pd.DataFrame, table_name: str
) -> None:
    """
    Copy dataframe contents into DB directly from buffer.

    Works much faster for larger dataframes (>10000 rows).

    :param connection: DB connection
    :param rows: data to insert
    :param table_name: name of the table for insertion
    """
    hdbg.dassert(
        table_name in hsql.get_table_names(connection),
        "Table %s is not present in the database!",
        table_name,
    )
    buffer = io.StringIO()
    rows.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur = connection.cursor()
    cur.copy_from(buffer, table_name, header=False)
    connection.commit()


def create_insert_query(rows: pd.DataFrame, table_name: str) -> str:
    """
    Create an INSERT query.

    :param rows: rows to insert into DB
    :param table_name: name of the table for insertion
    :return: INSERT command
    """
    columns = ",".join(list(rows.columns))
    _LOG.info("%s columns found", len(columns))
    query = f"INSERT INTO {table_name}({columns}) VALUES({'%%s'*len(columns)})"
    _LOG.info("Executing %s", query)
    return query


def execute_insert_query(
    connection: hsql.DbConnection, rows: pd.DataFrame, table_name: str
) -> None:
    """
    Insert multiple rows into the database.

    :param connection: connection to the DB
    :param rows: data to insert
    :param table_name: name of the table for insertion
    """
    hdbg.dassert(
        table_name in hsql.get_table_names(connection),
        "Table %s is not present in the database!",
        table_name,
    )
    # Transform dataframe into list of tuples.
    values = [tuple(v) for v in rows.to_numpy()]
    # Generate a query for multiple rows.
    query = create_insert_query(rows, table_name)
    # Execute query for each provided row.
    cur = connection.cursor()
    cur.executemany(query, values)
    connection.commit()


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
    rows = pd.read_csv("binance_ETH_USDT_20210927-083608.csv")
    execute_insert_query(conn, rows, table_name=args.table_name)


if __name__ == "__main__":
    _main(_parse())
