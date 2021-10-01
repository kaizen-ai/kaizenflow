#!/usr/bin/env python
import os

import helpers.sql as hsql
import helpers.dbg as dbg


def get_db_connection() -> hsql.DbConnection:
    conn, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    return conn


def _get_create_table_command(table_name: str) -> str:
    """
    Build a CREATE TABLE command based on table name.

    Currently available tables:
        - 'ccxtohlcv': OHLCV table with CCXT data

    :param table_name:
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
        dbg.dfatal("Table %s is not available for creation", table_name)
    return command


def create_table(conn: hsql.DbConnection, table_name: str):
    """

    :param conn:
    :param table_name:
    :return:
    """
    cursor = conn.cursor()
    # Build a CREATE TABLE command from name.
    command = _get_create_table_command(table_name)
    cursor.execute(command)
    cursor.close()
    conn.commit()
    return None


# -

conn = get_db_connection()

print(conn)

print(hsql.get_db_names(conn))
print(f"Table names: \n{','.join(hsql.get_table_names(conn))}")
print(f"Column names for exchange: \n{hsql.get_columns(conn, 'exchange')}")
print(f"Column names for symbol: \n{hsql.get_columns(conn, 'symbol')}")
print(f"Column names for kibotminutedata: \n{hsql.get_columns(conn, 'kibotminutedata')}")
print(f"Column names for kibotminutedata: \n{hsql.get_columns(conn, 'kibotminutedata')}")
print(f"Column names for kibotminutedata: \n{hsql.get_columns(conn, 'kibottickbidaskdata')}")
