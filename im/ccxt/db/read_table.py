#!/usr/bin/env python

# docker-compose --file compose/docker-compose.yml --env-file env/local.im_db_config.env run --rm app bash


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
    conn, cur = hsql.get_connection(
        dbname=os.environ["POSTGRES_DB"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # Extract all table names.
    all_table_names = hsql.get_table_names(conn)
    # Set necessary empty tables in DB.
    if "exchange_name" in all_table_names:
        # Clear 'exchange_name' table if it is already in DB.
        delete_query = """DELETE FROM exchange_name"""
        cur.execute(delete_query)
        conn.commit()
    else:
        # Create an empty 'exchange_name` table if it is not present in DB.
        imccdbcrtab.create_table(conn, "exchange_name")
    if "currency_pair" in all_table_names:
        # Clear 'currency_pair' table if it is already in DB.
        delete_query = """DELETE FROM currency_pair"""
        cur.execute(delete_query)
        conn.commit()
    else:
        # Create an empty 'currency_pair` table if it is not present in DB.
        imccdbcrtab.create_table(conn, "currency_pair")
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
    #
    # Uncomment to see the result in the script run.
    # ccxt_loader = cdlloa.CcxtLoader(conn)
    # table_exchange_names = ccxt_loader.read_db_data("exchange_name")
    # table_currency_pairs = ccxt_loader.read_db_data("currency_pair")
    # print(len(table_exchange_names))
    # print(table_exchange_names.head()
    # print(len(table_currency_pairs))
    # print(table_currency_pairs.head()


if __name__ == "__main__":
    _main(_parse())
