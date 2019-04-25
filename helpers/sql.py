import configparser
import copy
import getpass
import logging
import os
import pickle
import socket
import sys

import pandas as pd
import psycopg2 as pg

import helpers.dbg as dbg
import helpers.timer as timer

_log = logging.getLogger(__name__)

# TODO(gp): This code needs to be broken in pieces and moved in different
# locations.

# #############################################################################
# UI.
# #############################################################################


def read_config(config_file):
    print(("Using config_file %s" % config_file))
    assert os.path.exists(config_file)
    # Read config.
    config = configparser.ConfigParser()
    config.readfp(open(config_file))
    return config


# #############################################################################
# Sql.
# #############################################################################


def to_sql_conn_string(host, user, database="postgres", password=None):
    conn = "host='%s' user='%s' dbname='%s'" % (host, user, database)
    if password:
        conn += ' password="%s"' % password
    return conn


def sql_execute(conn_string, qq, autocommit=False):
    with pg.connect(conn_string) as conn:
        if autocommit:
            conn.autocommit = True
        # Catch error and execute query directly to print error.
        cur = conn.cursor()
        try:
            cur.execute(qq)
        except pg.Error as e:
            print((e.pgerror))
            raise pg.Error


def sql_execute_query(conn_string, qq):
    with pg.connect(conn_string) as conn:
        try:
            df = pd.read_sql_query(qq, conn)
        except:
            # Catch error and execute query directly to print error.
            cur = conn.cursor()
            try:
                cur.execute(qq)
            except pg.Error as e:
                print((e.pgerror))
                raise pg.Error
    return df


_sql_cache = {}


def query(conn_string,
          qq,
          limit=None,
          use_timer=False,
          use_cache=True,
          profile=False,
          verbose=True):
    global _sql_cache
    if limit is not None:
        qq += " LIMIT %s" % limit
    if profile:
        qq = "EXPLAIN ANALYZE " + qq
    if verbose:
        print(("> " + qq))
    #
    df = None
    #
    key = conn_string, qq
    if use_cache and (key in _sql_cache):
        _log.debug("Getting cache value for '%s'", key)
        df = copy.deepcopy(_sql_cache[key])
    else:
        # Compute.
        if not use_cache and use_timer:
            idx = timer.dtimer_start(0, "Sql time")
        with pg.connect(conn_string) as conn:
            try:
                df = pd.read_sql_query(qq, conn)
            except:
                # Catch error and execute query directly to print error.
                with pg.connect(conn_string) as conn:
                    cur = conn.cursor()
                try:
                    cur.execute(qq)
                except pg.Error as e:
                    print((e.pgerror))
                    raise pg.Error
        if use_cache:
            dbg.dassert_not_in(key, _sql_cache)
            _sql_cache[key] = df
            df = copy.deepcopy(_sql_cache[key])
        if not use_cache and use_timer:
            timer.dtimer_stop(idx)
    if profile:
        print(df)
        return None
    return df


def get_sql_dbs(conn_string):
    _log.debug("conn_string=%s", conn_string)
    conn = pg.connect(conn_string)
    string = "SELECT datname FROM pg_database;"
    cursor = conn.cursor()
    cursor.execute(string)
    dbs = zip(*cursor.fetchall())[0]
    dbs = sorted(dbs)
    return dbs


def get_all_tables(conn_string):
    _log.debug("conn_string=%s", conn_string)
    conn = pg.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' and "
                   "relname !~ '^(pg_|sql_)';")
    tables = zip(*cursor.fetchall())[0]
    tables = sorted(tables)
    return tables


def print_db_table_size(conn_string):
    cmd = """
SELECT d.datname AS Name, pg_catalog.pg_get_userbyid(d.datdba) AS Owner,
CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))
ELSE 'No Access'
END AS SIZE
FROM pg_catalog.pg_database d
ORDER BY
CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
THEN pg_catalog.pg_database_size(d.datname)
ELSE NULL
END DESC -- nulls first
LIMIT 20;
"""
    df = sql_execute_query(conn_string, cmd)
    return df


def show_table(conn_string, table, limit=5, as_txt=False):
    qq = "SELECT * FROM %s LIMIT %s " % (table, limit)
    df = query(conn_string, qq)
    if as_txt:
        #pd.options.display.max_columns = 1000
        #pd.options.display.width = 130
        print(df)
    else:
        display(df, as_txt=as_txt)


def show_tables(conn_string, tables=None, limit=5, as_txt=False):
    if tables is None:
        tables = get_all_tables(conn_string)
    for table in tables:
        print(("\n" + "#" * 80 + "\n" + table + "\n" + "#" * 80))
        show_table(conn_string, table, limit=limit, as_txt=as_txt)


def find_common_columns(conn_string, tables, as_df=False):
    limit = 5
    df = []
    for i in range(len(tables)):
        table = tables[i]
        qq = "SELECT * FROM %s LIMIT %s " % (table, limit)
        df1 = query(conn_string, qq, verbose=False)
        for j in range(i + 1, len(tables)):
            table = tables[j]
            qq = "SELECT * FROM %s LIMIT %s " % (table, limit)
            df2 = query(conn_string, qq, verbose=False)
            common_cols = [c for c in df1 if c in df2]
            if as_df:
                df.append((tables[i], tables[j], len(common_cols),
                           " ".join(common_cols)))
            else:
                print(("'%s' vs '%s'" % (tables[i], tables[j])))
                print(("    (%s): %s" % (len(common_cols), " ".join(common_cols))))
    if as_df:
        df = pd.DataFrame(
            df, columns=["table1", "table2", "num_comm_cols", "common_cols"])
        return df


def create_sql_pickle(conn, sql_query, file_name, abort_on_file_exists):
    file_name = os.path.abspath(file_name)
    _log.info("file_name='%s'", file_name)
    if os.path.exists(file_name):
        if abort_on_file_exists:
            raise RuntimeError("File %s already exists" % file_name)
    data = {
        "sql_query": sql_query,
        "username": getpass.getuser(),
        "datetime": pd.Timestamp.utcnow().tz_convert("US/Eastern"),
        "server": socket.gethostname(),
        "file_name": file_name,
    }
    _log.info("sql_query=%s", sql_query)
    with timer.TimedScope(0, "sql query"):
        df = pd.read_sql_query(sql_query, conn)
    data["df"] = df
    #
    f = open(file_name, 'w')
    pickle.dump(data, f)
    f.close()
    _log.info("Created file='%s'", file_name)
    return file_name


def read_sql_pickle(file_name):
    f = open(file_name, 'r')
    data = pickle.load(f)
    f.close()
    return data["df"]


# #############################################################################
# TR.
# #############################################################################


def normalize_code(code):
    # Remove quotes.
    code = code.rstrip("\"").lstrip("\"")
    # Remove \\ (see "NI:ATTACK/01\   instancesOf" in bug #6).
    if code.endswith("\\"):
        code2 = code.rstrip("""\\""")
        _log.warning("Found code '%s': using code '%s'", code, code2)
        code = code2
    # TODO(gp): Remove this and increase the max length of the code row.
    if len(code) > 20:
        code2 = code[:20]
        _log.warning("Truncating '%s' to '%s'", code, code2)
        code = code2
    return code