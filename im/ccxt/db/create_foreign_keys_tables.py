#!/usr/bin/env python
"""
Insert a table into the database.

Use example:

> create_foreign_keys_tables.py

Import as:

import im.ccxt.db.create_foreign_keys_tables as imcdbcrfokeytab
"""


import argparse
import logging

import pandas as pd
import psycopg2 as psycop

import ccxt
import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import im.ccxt.db.insert_data as imccdbindat
import im.common.db.create_db as imcodbcrdb

_LOG = logging.getLogger(__name__)


# TODO(Dan): Move this in #220.
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
    # TODO(Dan): Remove this script completely and use `create_im_schema` instead.
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get connection using env variables.
    conn, _ = hsql.get_connection_from_env_vars()
    queries = [
        imcodbcrdb.get_exchange_name_create_table_query,
        imcodbcrdb.get_currency_pair_create_table_query,
    ]
    for query in queries:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
        except psycop.errors.DuplicateObject:
            _LOG.warning("Duplicate table created, skipping.")
    # Populate tables with data.
    populate_exchange_currency_tables(conn)


if __name__ == "__main__":
    _main(_parse())
