"""
Import as:

import helpers.sql as hsql
"""

import collections
import io
import logging
import os
import time
from typing import List, NamedTuple, Optional, Tuple, Union

import pandas as pd
import psycopg2 as psycop
import psycopg2.extras as extras
import psycopg2.sql as psql

import helpers.dbg as hdbg
import helpers.printing as hprint
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)

# #############################################################################
# Connection
# #############################################################################

# TODO(gp): mypy doesn't like this. Understand why and / or inline.
DbConnection = psycop.extensions.connection


# Invariant: keep the arguments in the interface in the same order as:
# host, dbname, port, user, password
DbConnectionInfo = collections.namedtuple(
    "DbConnectionInfo", ["host", "dbname", "port", "user", "password"]
)


DbConnection = psycop.extensions.connection


# TODO(gp): mypy doesn't like this. Understand why and / or inline.
DbConnection = psycop.extensions.connection


DbConnectionInfo = collections.namedtuple(
    "DbConnectionInfo", ["host", "dbname", "port", "user", "password"]
)


# TODO(gp): Return only the connection (CmampTask441).
def get_connection(
    host: str,
    dbname: str,
    port: int,
    user: str,
    password: str,
    autocommit: bool = True,
) -> DbConnection:
    """
    Create a connection and cursor for a SQL database.
    """
    _LOG.debug(hprint.to_str("host dbname port user"))
    connection = psycop.connect(
        host=host, dbname=dbname, port=port, user=user, password=password
    )
    if autocommit:
        connection.autocommit = True
    return connection


def get_connection_from_env_vars() -> Tuple[
    DbConnection, psycop.extensions.cursor
]:
    """
    Create a SQL connection with the information from the environment
    variables.
    """
    # Get values from the environment variables.
    # TODO(gp): -> POSTGRES_DBNAME
    host = os.environ["POSTGRES_HOST"]
    dbname = os.environ["POSTGRES_DB"]
    port = int(os.environ["POSTGRES_PORT"])
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    # Build the
    connection = get_connection(
        host=host,
        dbname=dbname,
        port=port,
        user=user,
        password=password,
    )
    return connection


# TODO(gp): Return only the connection (CmampTask441).
def get_connection_from_env_vars() -> Tuple[
    DbConnection, psycop.extensions.cursor
]:
    """
    Create a SQL connection with the information from the environment
    variables.
    """
    # Get values from the environment variables.
    # TODO(gp): -> POSTGRES_DBNAME
    host = os.environ["POSTGRES_HOST"]
    dbname = os.environ["POSTGRES_DB"]
    port = int(os.environ["POSTGRES_PORT"])
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    # Build the
    connection = get_connection(
        host=host,
        dbname=dbname,
        port=port,
        user=user,
        password=password,
    )
    return connection


def get_connection_from_string(
    conn_as_str: str,
    autocommit: bool = True,
) -> Tuple[DbConnection, psycop.extensions.cursor]:
    """
    Create a connection from a string.

    TODO(gp): E.g., add example
    """
    connection = psycop.connect(conn_as_str)
    cursor = connection.cursor()
    if autocommit:
        connection.autocommit = True
    return connection, cursor


def check_db_connection(
    host: str,
    dbname: str,
    port: int,
    user: str,
    password: str,
) -> Tuple[bool, Optional[psycop.OperationalError]]:
    """
    Check whether a connection to a DB exists, in a non-blocking way.
    """
    try:
        get_connection(
            host=host, dbname=dbname, port=port, user=user, password=password
        )
        connection_exist = True
        error = None
    except psycop.OperationalError as e:
        connection_exist = False
        error = e
    return connection_exist, error


def wait_db_connection(
    host: str,
    dbname: str,
    port: int,
    user: str,
    password: str,
    timeout_in_secs: int = 10,
) -> None:
    """
    Wait until the database is available.

    :param timeout_in_secs: secs before timing out with `RuntimeError`.
    """
    hdbg.dassert_lte(1, timeout_in_secs)
    _LOG.debug("dbname=%s, port=%s, host=%s", dbname, port, host)
    elapsed_secs = 0
    while True:
        _LOG.info("Waiting for PostgreSQL to become available...")
        conn_exists = check_db_connection(host, dbname, port, user, password)
        if conn_exists[0]:
            _LOG.info("PostgreSQL is available (after %s seconds)", elapsed_secs)
            break
        if elapsed_secs > timeout_in_secs:
            raise psycop.OperationalError(
                f"Cannot connect to db host={host} dbname={dbname} port={port}"
                f"\n{conn_exists[1]}"
            )
        elapsed_secs += 1
        time.sleep(1)


def db_connection_to_tuple(connection: DbConnection) -> NamedTuple:
    """
    Get database connection details using connection. Connection details
    include:

        - Host
        - Database name
        - Port
        - Username
        - Password

    :param connection: a database connection
    :return: database connection details
    """
    info = connection.info
    det = DbConnectionInfo(
        host=info.host,
        dbname=info.dbname,
        port=info.port,
        user=info.user,
        password=info.password,
    )
    return det


# #############################################################################
# State of the whole DB
# #############################################################################


def get_engine_version(connection: DbConnection) -> str:
    """
    Report information on the SQL engine.

    E.g., ``` PostgreSQL 11.5 on x86_64-pc-linux-gnu compiled by gcc
    (GCC) 4.8.3 20140911 (Red Hat 4.8.3-9), 64-bit ```
    """
    query = "SELECT version();"
    df = pd.read_sql_query(query, connection)
    # pylint: disable=no-member
    info: str = df.iloc[0, 0]
    return info


# TODO(gp): Test / fix this.
def get_indexes(connection: DbConnection) -> pd.DataFrame:
    res = []
    tables = get_table_names(connection)
    cursor = connection.cursor()
    for table in tables:
        query = (
            """SELECT * FROM pg_indexes WHERE tablename = '{table}' """.format(
                table=table
            )
        )
        cursor.execute(query)
        z = cursor.fetchall()
        res.append(pd.DataFrame(z))
    tmp: pd.DataFrame = pd.concat(res)
    tmp["index_type"] = tmp[4].apply(
        lambda w: w.split("USING")[1].lstrip().split(" ")[0]
    )
    tmp.columns = [
        "type: public/private",
        "table_name",
        "key_name",
        "None",
        "Statement",
        "index_type",
    ]
    tmp["columns"] = tmp["Statement"].apply(lambda w: w.split("(")[1][:-1])

    return tmp


def disconnect_all_clients(dbname: str):
    # From https://stackoverflow.com/questions/36502401
    # Not sure this will work in our case, since it might kill our own connection.
    cmd = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{dbname}';"


# #############################################################################
# Database
# #############################################################################


def get_db_names(connection: DbConnection) -> List[str]:
    """
    Return the names of the available DBs.

    E.g., ['postgres', 'rdsadmin', 'template0', 'template1']
    """
    query = "SELECT datname FROM pg_database;"
    cursor = connection.cursor()
    cursor.execute(query)
    dbs = list(zip(*cursor.fetchall()))[0]
    dbs = sorted(dbs)
    return dbs


def create_database(
    connection: DbConnection,
    dbname: str,
    overwrite: Optional[bool] = None,
) -> None:
    """
    Create empty database.

    :param connection: database connection
    :param dbname: database to create
    :param overwrite: overwrite existing database
    """
    _LOG.debug("connection=%s", connection)
    with connection.cursor() as cursor:
        if overwrite:
            cursor.execute(
                psql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE);").format(
                    psql.Identifier(dbname)
                )
            )
        else:
            if dbname in get_table_names(connection):
                raise ValueError(f"Database {dbname} already exists")
        cursor.execute(
            psql.SQL("CREATE DATABASE {};").format(psql.Identifier(dbname))
        )


def remove_database(connection: DbConnection, dbname: str) -> None:
    """
    Remove database in current environment.

    :param connection: a database connection
    :param dbname: database name to drop, e.g. `im_db_local`
    """
    # Drop database.
    # From https://stackoverflow.com/questions/36502401
    connection.cursor().execute(
        psql.SQL("DROP DATABASE {} WITH (FORCE);").format(psql.Identifier(dbname))
    )


# #############################################################################
# Tables
# #############################################################################


def get_table_names(connection: DbConnection) -> List[str]:
    """
    Report the name of the tables.

    E.g., tables=['entities', 'events', 'stories', 'taxonomy']
    """
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema = 'public'
    """
    cursor = connection.cursor()
    cursor.execute(query)
    tables = [x[0] for x in cursor.fetchall()]
    return tables


def get_tables_size(
    connection: DbConnection,
    only_public: bool = True,
    summary: bool = True,
) -> pd.DataFrame:
    """
    Report the size of each table.

    E.g.,

      table_name  row_estimate   total    index      toast  table
    0     events           0.0   26 GB  0 bytes  192 bytes  26 GB
    1    stories           0.0   15 GB    43 GB  192 bytes  12 GB
    2   entities    10823400.0   76 MB  0 bytes  192 bytes  76 MB
    3   taxonomy       20691.0  690 kB  0 bytes  192 bytes 652 kB
    """
    q = """SELECT *, pg_size_pretty(total_bytes) AS total
        , pg_size_pretty(index_bytes) AS INDEX
        , pg_size_pretty(toast_bytes) AS toast
        , pg_size_pretty(table_bytes) AS TABLE
      FROM (
      SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
          SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
                  , c.reltuples AS row_estimate
                  , pg_total_relation_size(c.oid) AS total_bytes
                  , pg_indexes_size(c.oid) AS index_bytes
                  , pg_total_relation_size(reltoastrelid) AS toast_bytes
              FROM pg_class c
              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE relkind = 'r'
      ) a
    ) a
    ORDER by total_bytes DESC"""
    df = pd.read_sql_query(q, connection)
    if only_public:
        df = df[df["table_schema"] == "public"]
    if summary:
        cols = "table_name row_estimate total index toast table".split()
        df = df[cols]
    return df


def head_table(
    connection: DbConnection,
    table: str,
    limit: int = 5,
) -> str:
    """
    Report the head of the table as str.
    """
    txt = []
    query = "SELECT * FROM %s LIMIT %s " % (table, limit)
    df = execute_query_to_df(connection, query)
    # pd.options.display.max_columns = 1000
    # pd.options.display.width = 130
    txt.append(str(df))
    txt = "\n".join(txt)
    return txt


def head_tables(
    connection: DbConnection,
    tables: Optional[List[str]] = None,
    limit: int = 5,
) -> str:
    txt = []
    if tables is None:
        tables = get_table_names(connection)
    for table in tables:
        txt.append("\n" + "#" * 80 + "\n" + table + "\n" + "#" * 80)
        txt_tmp = head_table(connection, table, limit=limit)
        txt.append(txt_tmp)
    txt = "\n".join(txt)
    return txt


def get_table_columns(connection: DbConnection, table_name: str) -> List[str]:
    """
    Get column names for given table.
    """
    query = f"""
        SELECT column_name
            FROM information_schema.columns
            WHERE TABLE_NAME = '{table_name}'"""
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [x[0] for x in cursor.fetchall()]
    return columns


def find_tables_common_columns(
    connection: DbConnection,
    tables: List[str],
    as_df: bool = False,
) -> Union[None, pd.DataFrame]:
    limit = 5
    df = []
    for i, table in enumerate(tables):
        table = tables[i]
        query = "SELECT * FROM %s LIMIT %s " % (table, limit)
        df1 = execute_query_to_df(connection, query, verbose=False)
        if df1 is None:
            continue
        for j in range(i + 1, len(tables)):
            table = tables[j]
            query = "SELECT * FROM %s LIMIT %s " % (table, limit)
            df2 = execute_query_to_df(connection, query, verbose=False)
            if df2 is None:
                continue
            common_cols = [c for c in df1 if c in df2]
            if as_df:
                df.append(
                    (
                        tables[i],
                        tables[j],
                        len(common_cols),
                        " ".join(common_cols),
                    )
                )
            else:
                print(("'%s' vs '%s'" % (tables[i], tables[j])))
                print(
                    ("    (%s): %s" % (len(common_cols), " ".join(common_cols)))
                )
    obj = None
    if as_df:
        obj = pd.DataFrame(
            df, columns=["table1", "table2", "num_comm_cols", "common_cols"]
        )
    return obj


def remove_table(connection: DbConnection, table_name: str) -> None:
    query = f"DROP TABLE IF EXISTS {table_name}"
    connection.cursor().execute(query)


# #############################################################################
# Query
# #############################################################################


def execute_query_to_df(
    connection: DbConnection,
    query: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    use_timer: bool = False,
    profile: bool = False,
    verbose: bool = False,
) -> Union[None, pd.DataFrame]:
    """
    Execute a query.
    """
    if limit is not None:
        query += " LIMIT %s" % limit
    if offset is not None:
        query += " OFFSET %s" % offset
    if profile:
        query = "EXPLAIN ANALYZE " + query
    if verbose:
        print(("> " + query))
    # Compute.
    if use_timer:
        idx = htimer.dtimer_start(0, "Sql time")
    df = None
    cursor = connection.cursor()
    try:
        df = pd.read_sql_query(query, connection)
    except psycop.OperationalError:
        # Catch error and execute query directly to print error.
        try:
            cursor.execute(query)
        except psycop.Error as e:
            print(e.pgerror)
            raise psycop.Error
    if use_timer:
        htimer.dtimer_stop(idx)
    if profile:
        print(df)
        return None
    return df


# #############################################################################
# Insert
# #############################################################################


def copy_rows_with_copy_from(
    connection: DbConnection, df: pd.DataFrame, table_name: str
) -> None:
    """
    Copy dataframe contents into DB directly from buffer.

    This function works much faster for large dataframes (>10000 rows).

    :param connection: DB connection
    :param df: data to insert
    :param table_name: name of the table for insertion
    """
    # The target table needs to exist.
    hdbg.dassert_in(table_name, get_table_names(connection))
    # Read the data.
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    # Copy the data to the DB.
    cur = connection.cursor()
    cur.copy_from(buffer, table_name, sep=",")
    # TODO(gp): CmampTask413, is this still needed because the autocommit.
    connection.commit()


def _create_insert_query(df: pd.DataFrame, table_name: str) -> str:
    """
    Create an INSERT query to insert data into a DB.

    :param df: data to insert into DB
    :param table_name: name of the table for insertion
    :return: sql query, e.g.,
        ```
        INSERT INTO ccxt_ohlcv(timestamp,open,high,low,close) VALUES %s
        ```
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    columns = ",".join(list(df.columns))
    query = f"INSERT INTO {table_name}({columns}) VALUES %s"
    _LOG.debug("query=%s", query)
    return query


def execute_insert_query(
    connection: DbConnection, obj: Union[pd.DataFrame, pd.Series], table_name: str
) -> None:
    """
    Insert a DB as multiple rows into the database.

    :param connection: connection to the DB
    :param obj: data to insert
    :param table_name: name of the table for insertion
    """
    if isinstance(obj, pd.Series):
        df = obj.to_frame().T
    else:
        df = obj
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_in(table_name, get_table_names(connection))
    _LOG.debug("df=\n%s", hprint.dataframe_to_str(df, use_tabulate=False))
    # Transform dataframe into list of tuples.
    values = [tuple(v) for v in df.to_numpy()]
    # Generate a query for multiple rows.
    query = _create_insert_query(df, table_name)
    # Execute query for each provided row.
    cur = connection.cursor()
    extras.execute_values(cur, query, values)
    connection.commit()


def get_remove_duplicates_query(
    table_name: str, id_col_name: str, column_names: List[str]
) -> str:
    """
    Get a query to remove duplicates from table, keeping last duplicated row.

    :param table_name: name of table
    :param id_col_name: name of unique id column
    :param column_names: names of columns to compare on
    :return: query to execute duplicate removal
    """
    # TODO(*): Add a "limit" parameter if possible, to check only in top N rows.
    remove_statement = []
    remove_statement.append(f"DELETE FROM {table_name} a USING {table_name} b")
    remove_statement.append(f"WHERE a.{id_col_name} < b.{id_col_name}")
    for c in column_names:
        remove_statement.append(f"AND a.{c} = b.{c}")
    remove_statement = " ".join(remove_statement)
    return remove_statement
