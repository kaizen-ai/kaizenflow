#!/usr/bin/env python
"""
Insert a table into the database.

Use example:

> create_foreign_keys_tables.py

Import as:

import im.ccxt.db.create_foreign_keys_tables as imcdcfkta
"""


import argparse
import logging

import ccxt
import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import os

import pandas as pd

import im.ccxt.data.load.loader as cdlloa
import im.ccxt.db.insert_data as imccdbindat
import im.ccxt.db.create_table as imccdbcrtab

_LOG = logging.getLogger(__name__)


def create_empty_table(conn: hsql.DbConnection, table_name: str) -> None:
    """
    Create empty table in the database.

    :param conn: DB connection
    :param table_name: name of the table
    """
    cursor = conn.cursor()
    # Extract all table names.
    all_table_names = hsql.get_table_names(conn)
    if table_name in all_table_names:
        # Clear table content if it is already in DB.
        delete_query = "DELETE FROM %s" % table_name
        cursor.execute(delete_query)
    else:
        # Create an empty table if it is not present in DB.
        imccdbcrtab.create_table(conn, table_name)


def populate_exchange_currency_tables(conn: hsql.DbConnection) -> None:
    """
    Populate exchange name and currency pair tables with data.

    :param conn: DB connection
    """
    # Extract a list of all CCXT exchange names.
    all_exchange_names = pd.Series(ccxt.exchanges)
    # Create a dataframe with exchange names and ids.
    df_exchange_names = all_exchange_names.reset_index()
    df_exchange_names.columns = ["exchange_id", "exchange_name"]
    # Insert exchange names dataframe in DB.
    imccdbindat.execute_insert_query(conn, df_exchange_names, "exchange_name")
    # Create an empty list for currency pairs.
    currency_pairs = []
    # Extract all the currency pairs for each exchange and append them.
    # to the currency pairs list.
    for exchange_name in all_exchange_names:
        # Some few exchanges require credentials for this info so we omit them.
        try:
            exchange_class = getattr(ccxt, exchange_name)()
            exchange_currency_pairs = list(exchange_class.load_markets().keys())
            currency_pairs.extend(exchange_currency_pairs)
        except:
            continue
    # Create a dataframe with currency pairs and ids.
    currency_pairs_srs = pd.Series(sorted(list(set(currency_pairs))))
    df_currency_pairs = currency_pairs_srs.reset_index()
    df_currency_pairs.columns = ["currency_pair_id", "currency_pair"]
    # Insert currency pairs dataframe in DB.
    imccdbindat.execute_insert_query(conn, df_currency_pairs, "currency_pair")


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get connection using env variables.
    conn, _ = hsql.get_connection_from_env_vars()
    # Create new or clear existing required tables.
    for table_name in ["exchange_name", "currency_pair"]:
        create_empty_table(conn, table_name)
    # Populate tables with data.
    populate_exchange_currency_tables(conn)


if __name__ == "__main__":
    _main(_parse())
