#!/usr/bin/env python
import os

import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.io_ as hio
import helpers.sql as hsql

def get_db_connection() -> psycop.extensions.connection:
    conn, _ = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    return conn


def create_table(conn):
    cursor = conn.cursor()
    command = """
    CREATE TABLE ccxohlcv (
    id SERIAL PRIMARY KEY,
    timestamp ,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    epoch INT,
    currency_pair FOREIGN_KEY,
    exchange_id FOREIGN_KEY
    )
    """
    return None

conn = get_db_connection()

print(conn)

print(hsql.get_db_names(conn))
print(f"Table names: \n{','.join(hsql.get_table_names(conn))}")
print(f"Column names for exchange: \n{hsql.get_columns(conn, 'exchange')}")
print(f"Column names for symbol: \n{hsql.get_columns(conn, 'symbol')}")
print(f"Column names for kibotminutedata: \n{hsql.get_columns(conn, 'kibotminutedata')}")
print(f"Column names for kibotminutedata: \n{hsql.get_columns(conn, 'kibotminutedata')}")
print(f"Column names for kibotminutedata: \n{hsql.get_columns(conn, 'kibottickbidaskdata')}")
