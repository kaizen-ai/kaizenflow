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

conn = get_db_connection()

print(conn)

print(hsql.get_db_names(conn))
print(hsql.get_table_names(conn))
