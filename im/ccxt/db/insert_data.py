"""
Utilities for inserting data into DB table.


"""

import logging

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import im.common.db.create_schema as imcodbcrsch
import psycopg2
from typing import List, Tuple


def get_insert_rows_sql_command(values: List[Tuple], table_name: str, connection: hsql.DbConnection):
    """
    Get command to insert given values into CCXT table.

    :param values:
    :param table_name:
    :return:
    """
    cur = connection.cursor()
    args_str = ", ".join(cur.mogrify(""))
    command = f"{}"
    return command


_LOG = logging.getLogger(__name__)