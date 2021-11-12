#!/usr/bin/env python
"""
Script to download OHLCV data from CCXT in real-time.

Use as:

# Download OHLCV data for universe '01', saving only on disk:
> python im/ccxt/data/extract/download_realtime.py \
    --db_connection 'none' \
    --dst_dir 'test_ohlcv_rt' \
    --data_type 'ohlcv' \
    --universe '01'

# Download order book data for universe '01', saving only on disk:
> python im/ccxt/data/extract/download_realtime.py \
    --db_connection 'none' \
    --dst_dir 'test_orderbook_rt' \
    --data_type 'orderbook' \
    --universe '01'

Import as:

import im.ccxt.data.extract.download_realtime as imcdaexdowrea
"""
import argparse
import collections
import logging
import os
import time
from typing import Any, Dict, List, NamedTuple, Optional, Union

import ccxt
import pandas as pd

import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.sql as hsql
import im.ccxt.data.extract.exchange_class as imcdaexexccla
import im.ccxt.db.utils as imccdbuti
import im.data.universe as imdauni

_LOG = logging.getLogger(__name__)

# TODO(Danya): Merge with `download_realtime_orderbook.py`


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
    exchange_to_currency.instance = imcdaexexccla.CcxtExchange(
        exchange_id, api_keys
    )
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


def _save_data_on_disk(
    data_type: str,
    dst_dir: str,
    pair_data: Union[pd.DataFrame, Dict[str, Any]],
    exchange: NamedTuple,
    pair: str,
) -> None:
    """
    Save downloaded data to disk.

    :param data_type: 'ohlcv' or 'orderbook'
    :param dst_dir: directory to save to
    :param pair_data: downloaded data
    :param exchange: exchange instance
    :param pair: currency pair, e.g. 'BTC/USDT'
    """
    current_datetime = hdatetim.get_current_time("ET")
    if data_type == "ohlcv":
        file_name = (
            f"{exchange.id}_{pair.replace('/', '_')}_{current_datetime}.csv.gz"
        )
        full_path = os.path.join(dst_dir, file_name)
        pair_data.to_csv(full_path, index=False, compression="gzip")
    elif data_type == "orderbook":
        file_name = (
            f"orderbook_{exchange.id}_"
            f"{pair.replace('/', '_')}_"
            f"{hdatetim.get_timestamp('ET')}.json"
        )
        full_path = os.path.join(dst_dir, file_name)
        hio.to_json(full_path, pair_data)
    else:
        hdbg.dfatal(
            "'%s' data type is not supported. Supported data types: 'ohlcv', 'orderbook'",
            data_type,
        )


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
        required=True,
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
        default=imcdaexexccla.API_KEYS_PATH,
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
    # Create the directory.
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # Connect to database.
    if args.db_connection == "from_env":
        connection, _ = hsql.get_connection_from_env_vars()
    elif args.db_connection == "none":
        connection = None
    else:
        hdbg.dfatal("Unknown db connection: %s" % args.db_connection)
    # Load universe.
    universe = imdauni.get_trade_universe(args.universe)
    exchange_ids = universe["CCXT"].keys()
    # Build mappings from exchange ids to classes and currencies.
    exchanges = []
    for exchange_id in exchange_ids:
        exchanges.append(
            _instantiate_exchange(exchange_id, universe["CCXT"], args.api_keys)
        )
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
                ) as e:
                    # Continue the loop if could not connect to exchange.
                    _LOG.warning("Got an error: %s", type(e).__name__, e.args)
                    continue
                except ccxt.base.errors.RateLimitExceeded as e:
                    _LOG.warning(
                        "Got an Exceeded limit error: %s",
                        type(e).__name__,
                        e.args,
                    )
                    sleep(60)
                    continue
                # Save to disk.
                _save_data_on_disk(
                    args.data_type, args.dst_dir, pair_data, exchange, pair
                )
                if connection:
                    # Insert into database.
                    imccdbuti.execute_insert_query(
                        connection=connection,
                        df=pair_data,
                        table_name=args.table_name,
                    )
        time.sleep(60)


if __name__ == "__main__":
    _main(_parse())
