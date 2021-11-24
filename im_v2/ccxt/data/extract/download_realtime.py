#!/usr/bin/env python
"""
Script to download OHLCV data from CCXT in real-time.

Use as:

# Download OHLCV data for universe 'v03', saving only on disk:
> python im_v2/ccxt/data/extract/download_realtime.py \
    --db_connection 'none' \
    --dst_dir 'test_ohlcv_rt' \
    --data_type 'ohlcv' \
    --universe 'v03'

# Download order book data for universe 'v03', saving only on disk:
> python im_v2/ccxt/data/extract/download_realtime.py \
    --db_connection 'none' \
    --dst_dir 'test_orderbook_rt' \
    --data_type 'orderbook' \
    --universe 'v03'

Import as:

import im_v2.ccxt.data.extract.download_realtime as imvcdedore
"""
import argparse
import collections
import logging
import os
import time
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import ccxt
import pandas as pd

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.s3 as hs3
import helpers.sql as hsql
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.common.universe.universe as imvcounun

_LOG = logging.getLogger(__name__)

# TODO(Danya): Create a type and move outside.
def _instantiate_exchange(
    exchange_id: str,
    ccxt_universe: Dict[str, List[str]],
    api_keys: Optional[str] = None,
) -> NamedTuple:
    """
    Create a tuple with exchange id, its class instance and currency pairs.

    :param exchange_id: CCXT exchange id
    :param ccxt_universe: CCXT trade universe
    :return: named tuple with exchange id and currencies
    """
    exchange_to_currency = collections.namedtuple(
        "ExchangeToCurrency", ["id", "instance", "pairs"]
    )
    exchange_to_currency.id = exchange_id
    exchange_to_currency.instance = imvcdeexcl.CcxtExchange(exchange_id, api_keys)
    exchange_to_currency.pairs = ccxt_universe[exchange_id]
    return exchange_to_currency


def _download_data(
    data_type: str, exchange: NamedTuple, pair: str
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Download order book or OHLCV data.

    :param data_type: 'ohlcv' or 'orderbook'
    :param exchange: exchange instance
    :param pair: currency pair, e.g. 'BTC/USDT'
    :return: downloaded data
    """
    # Download 5 latest OHLCV candles.
    if data_type == "ohlcv":
        pair_data = exchange.instance.download_ohlcv_data(
            curr_symbol=pair, step=5
        )
        # Assign pair and exchange columns.
        pair_data["currency_pair"] = pair
        pair_data["exchange_id"] = exchange.id
    elif data_type == "orderbook":
        # Download current state of the orderbook.
        pair_data = exchange.instance.download_order_book(pair)
    else:
        hdbg.dfatal(
            "'%s' data type is not supported. Supported data types: 'ohlcv', 'orderbook'",
            data_type,
        )
    return pair_data


def _save_data(
    fs_dir: str,
    s3_dir: str,
    s3fs_,
    data_type: str,
    pair_data: Union[pd.DataFrame, Dict[str, Any]],
    exchange: NamedTuple,
    pair: str,
) -> None:
    """
    Save downloaded data to disk and S3.

    The file name is constructed from provided params, e.g.
    'kucoin_BTC_USDT_20210922_140312.csv.gz'

    :param data_type: 'ohlcv' or 'orderbook'
    :param fs_dir: FS directory to save to
    :param s3_dir: S3 location to save to
    :param s3fs_: S3 filesystem object
    :param pair_data: downloaded data
    :param exchange: exchange instance
    :param pair: currency pair, e.g. 'BTC/USDT'
    """
    current_datetime = hdateti.get_current_time("ET")
    if data_type == "ohlcv":
        file_name = (
            f"{exchange.id}_{pair.replace('/', '_')}_{current_datetime}.csv.gz"
        )
        # Save to filesystem.
        fs_path = os.path.join(fs_dir, file_name)
        pair_data.to_csv(fs_path, index=False, compression="gzip")
        # Save to S3.
        s3_path = os.path.join(s3_dir, file_name)
        s3fs_.put(fs_path, s3_path)
    elif data_type == "orderbook":
        file_name = (
            f"{exchange.id}_"
            f"{pair.replace('/', '_')}_"
            f"{hdateti.get_timestamp('ET')}.json"
        )
        # Save to filesystem.
        fs_path = os.path.join(fs_dir, file_name)
        hio.to_json(fs_path, pair_data)
        # Save to S3.
        s3_path = os.path.join(s3_dir, file_name)
        s3fs_.put(fs_path, s3_path)
    else:
        hdbg.dfatal(
            "'%s' data type is not supported. Supported data types: 'ohlcv', 'orderbook'",
            data_type,
        )


def _get_rt_paths(dst_dir: str) -> Tuple[str, str]:
    """
    Get paths to FS shared and S3 directories.

    E.g, "/data/shared/data/ohlcv_test/", "s3://alphamatic-data/data/ohlcv_test/"
    :param dst_dir: download directory
    :return: local and S3 paths
    """
    rt_fs_path = os.path.join("/data/shared/", dst_dir)
    rt_s3_path = hs3.get_path() + dst_dir
    return rt_fs_path, rt_s3_path


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--db_connection",
        action="store",
        default="from_env",
        type=str,
        help="Connection to database to upload to",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        help="Folder to save copies of data to",
    )
    parser.add_argument(
        "--data_type",
        action="store",
        required=True,
        type=str,
        help="Type of data to load, 'ohlcv' or 'orderbook'",
    )
    parser.add_argument(
        "--table_name",
        action="store",
        type=str,
        help="Name of the table to upload to",
    )
    parser.add_argument(
        "--api_keys",
        action="store",
        type=str,
        default=imvcdeexcl.API_KEYS_PATH,
        help="Path to JSON file that contains API keys for exchange access",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe to download data for",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Construct destination paths.
    #  Note: data is always saved to S3 and "/data/shared/".
    fs_path, s3_path = _get_rt_paths(args.dst_dir)
    # Create local dir.
    hio.create_dir(fs_path, incremental=args.incremental)
    # Connect to database.
    if args.db_connection == "from_env":
        connection = hsql.get_connection_from_env_vars()
    elif args.db_connection == "none":
        connection = None
    else:
        hdbg.dfatal("Unknown db connection: %s" % args.db_connection)
    # Load universe.
    universe = imvcounun.get_trade_universe(args.universe)
    exchange_ids = universe["CCXT"].keys()
    # Build mappings from exchange ids to classes and currencies.
    exchanges = []
    for exchange_id in exchange_ids:
        exchanges.append(
            _instantiate_exchange(exchange_id, universe["CCXT"], args.api_keys)
        )
    # Generate a query to remove duplicates.
    dup_query = hsql.get_remove_duplicates_query(
        table_name=args.table_name,
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    # Get an S3FS object to save RT data.
    rt_s3fs = hs3.get_s3fs()
    # Launch an infinite loop.
    while True:
        for exchange in exchanges:
            for pair in exchange.pairs:
                try:
                    # Download latest data.
                    pair_data = _download_data(args.data_type, exchange, pair)
                except (
                    ccxt.ExchangeError,
                    ccxt.NetworkError,
                    ccxt.base.errors.RequestTimeout,
                    ccxt.base.errors.RateLimitExceeded,
                ) as e:
                    # Continue the loop if could not connect to exchange.
                    _LOG.warning("Got an error: %s", type(e).__name__, e.args)
                    continue
                # Save to disk.
                _save_data(
                    fs_path,
                    s3_path,
                    rt_s3fs,
                    data_type=args.data_type,
                    pair_data=pair_data,
                    exchange=exchange,
                    pair=pair,
                )
                if connection:
                    # Insert into database.
                    hsql.execute_insert_query(
                        connection=connection,
                        obj=pair_data,
                        table_name=args.table_name,
                    )
                    # Drop duplicates inside the table.
                    connection.cursor().execute(dup_query)
        time.sleep(60)


if __name__ == "__main__":
    _main(_parse())
