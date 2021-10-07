"""
Utilities for inserting data into DB table.

Import as:

import im.ccxt.db.insert_data as imccdbindat
"""

import io
import logging

import pandas as pd

import helpers.dbg as hdbg
import helpers.sql as hsql

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
    query = f"INSERT INTO {table_name}({columns}) VALUES({'%%s'*len(columns)})"
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
