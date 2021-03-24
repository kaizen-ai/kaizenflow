import logging
from typing import List, Optional, Tuple, Union

import pandas as pd
import psycopg2 as psycop
import psycopg2.sql as psql

import helpers.timer as htimer

_log = logging.getLogger(__name__)


def get_connection(
    dbname: str,
    host: str,
    user: str,
    port: int,
    password: str,
    autocommit: bool = True,
) -> Tuple[psycop.extensions.connection, psycop.extensions.cursor]:
    connection = psycop.connect(
        dbname=dbname, host=host, user=user, port=port, password=password
    )
    cursor = connection.cursor()
    if autocommit:
        connection.autocommit = True
    return connection, cursor


def get_engine_version(connection: psycop.extensions.connection) -> str:
    """
    Report information on the SQL engine.

    E.g., ``` PostgreSQL 11.5 on x86_64-pc-linux-gnu     compiled by gcc
    (GCC) 4.8.3 20140911 (Red Hat 4.8.3-9), 64-bit ```
    """
    query = "SELECT version();"
    df = pd.read_sql_query(query, connection)
    # pylint: disable=no-member
    info: str = df.iloc[0, 0]
    return info


def get_db_names(connection: psycop.extensions.connection) -> List[str]:
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


def get_table_size(
    connection: psycop.extensions.connection,
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


def get_table_names(connection: psycop.extensions.connection) -> List[str]:
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


# TODO(gp): Test / fix this.
def get_indexes(connection: psycop.extensions.connection) -> pd.DataFrame:
    res = []
    tables = get_table_names(connection)
    cursor = connection.cursor()
    for table in tables:
        query = (
            """select * from pg_indexes where tablename = '{table}' """.format(
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


def get_columns(
    connection: psycop.extensions.connection, table_name: str
) -> list:
    query = (
        """SELECT column_name
            FROM information_schema.columns
            WHERE TABLE_NAME = '%s' """
        % table_name
    )
    cursor = connection.get_cursor()
    cursor.execute(query)
    columns = [x[0] for x in cursor.fetchall()]
    return columns


def execute_query(
    connection: psycop.extensions.connection,
    query: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    use_timer: bool = False,
    profile: bool = False,
    verbose: bool = True,
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
            print((e.pgerror))
            raise psycop.Error
    if use_timer:
        htimer.dtimer_stop(idx)
    if profile:
        print(df)
        return None
    return df


def head_table(
    connection: psycop.extensions.connection,
    table: str,
    limit: int = 5,
    as_txt: bool = False,
) -> None:
    query = "SELECT * FROM %s LIMIT %s " % (table, limit)
    df = execute_query(connection, query)
    if as_txt:
        # pd.options.display.max_columns = 1000
        # pd.options.display.width = 130
        print(df)
    else:
        import IPython

        IPython.core.display.display(df)


def head_tables(
    connection: psycop.extensions.connection,
    tables: Optional[List[str]] = None,
    limit: int = 5,
    as_txt: bool = False,
) -> None:
    if tables is None:
        tables = get_table_names(connection)
    for table in tables:
        print(("\n" + "#" * 80 + "\n" + table + "\n" + "#" * 80))
        head_table(connection, table, limit=limit, as_txt=as_txt)


def find_common_columns(
    connection: psycop.extensions.connection,
    tables: List[str],
    as_df: bool = False,
) -> Union[None, pd.DataFrame]:
    limit = 5
    df = []
    for i, table in enumerate(tables):
        table = tables[i]
        query = "SELECT * FROM %s LIMIT %s " % (table, limit)
        df1 = execute_query(connection, query, verbose=False)
        if df1 is None:
            continue
        for j in range(i + 1, len(tables)):
            table = tables[j]
            query = "SELECT * FROM %s LIMIT %s " % (table, limit)
            df2 = execute_query(connection, query, verbose=False)
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


# TODO(plyq): Tests.
def create_database(
    connection: psycop.extensions.connection,
    db: str,
    force: Optional[bool] = None,
) -> None:
    """
    Create empty database.

    :param connection: database connection
    :param db: database to create
    :param force: overwrite existing database
    """
    with connection.cursor() as cursor:
        if force:
            cursor.execute(
                psql.SQL("DROP DATABASE IF EXISTS {};").format(
                    psql.Identifier(db)
                )
            )
        else:
            raise ValueError("Database %s already exists" % db)
        cursor.execute(
            psql.SQL("CREATE DATABASE {};").format(psql.Identifier(db))
        )
