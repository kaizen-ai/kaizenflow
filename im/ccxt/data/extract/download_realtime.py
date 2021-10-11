#!/usr/bin/env python
"""
Script to download data from CCXT in real-time.

Use as:

- Download all currency pairs for Binance, Kucoin,
  FTX exchanges:
> python im/ccxt/data/extract/download_realtime.py \
    --dst_dir test1 \
    --exchange_ids 'binance kucoin ftx' \
    --currency_pairs 'all'

Import as:

import im.ccxt.data.extract.download_realtime as imcdaexdowrea
"""
import argparse
import collections
import logging
import os
import time
from typing import NamedTuple, Optional

import helpers.datetime_ as hdatetim
import helpers.io_ as hio
import helpers.parser as hparser
import im.ccxt.data.extract.exchange_class as imcdaexexccla
from helpers import dbg

_LOG = logging.getLogger(__name__)


def _instantiate_exchange(
    exchange_id: str, currency_pairs: str, api_keys: Optional[str] = None
) -> NamedTuple:
    """
    Create a tuple with exchange id, its class instance and currency pairs.

    :param exchange_id: CCXT exchange id
    :param currency_pairs: space-delimited currencies, e.g. 'BTC/USDT ETH/USDT'
    :return: named tuple with exchange id and currencies
    """
    exchange_to_currency = collections.namedtuple(
        "ExchangeToCurrency", ["id", "instance", "pairs"]
    )
    exchange_to_currency.id = exchange_id
    exchange_to_currency.instance = imcdaexexccla.CcxtExchange(
        exchange_id, api_keys
    )
    if currency_pairs == "all":
        # Store all currency pairs for each exchange.
        currency_pairs = exchange_to_currency.instance.currency_pairs
        exchange_to_currency.pairs = currency_pairs
    else:
        # Store currency pairs present in provided exchanges.
        provided_pairs = currency_pairs.split()
        exchange_to_currency.pairs = [
            curr
            for curr in provided_pairs
            if curr in exchange_to_currency.instance.currency_pairs
        ]
    return exchange_to_currency


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        # TODO(Danya): replace dst_dir with SQL connection.
        "--dst_dir",
        action="store",
        required=True,
        type=str,
        help="Folder to download files to",
    )
    parser.add_argument(
        "--api_keys",
        action="store",
        type=str,
        default=imcdaexexccla.API_KEYS_PATH,
        help="Path to JSON file that contains API keys for exchange access",
    )
    parser.add_argument(
        "--exchange_ids",
        action="store",
        required=True,
        type=str,
        help="CCXT names of exchanges to download from, separated by spaces, e.g. 'binance gemini',"
        "'all' for each exchange (currently includes Binance and Kucoin by default)",
    )
    parser.add_argument(
        "--currency_pairs",
        action="store",
        required=True,
        type=str,
        help="Currency pairs to download data for, separated by spaces, e.g. 'BTC/USD ETH/USD',"
        " 'all' for each currency pair in exchange",
    )
    parser.add_argument(
        # TODO(Danya): remove after adding the SQL connection.
        "--incremental",
        action="store_true",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # Get exchange ids.
    if args.exchange_ids == "all":
        exchange_ids = ["binance", "kucoin"]
    else:
        # Get exchanges provided by the user.
        exchange_ids = args.exchange_ids.split()
    # Build mappings from exchange ids to classes and currencies.
    exchanges = []
    for exchange_id in exchange_ids:
        exchanges.append(
            _instantiate_exchange(exchange_id, args.currency_pairs, args.api_keys)
        )
    # Launch an infinite loop.
    while True:
        for exchange in exchanges:
            for pair in exchange.pairs:
                # Download latest 5 minutes for the currency pair and exchange.
                pair_data = exchange.instance.download_ohlcv_data(
                    curr_symbol=pair, step=5
                )
                # Save data with timestamp.
                # TODO(Danya): replace saving with DB update.
                file_name = (
                    f"{exchange.id}_"
                    f"{pair.replace('/', '_')}_"
                    f"{hdatetim.get_timestamp('ET')}"
                    f".csv.gz"
                )
                file_path = os.path.join(args.dst_dir, file_name)
                pair_data.to_csv(file_path, index=False, compression="gzip")
        time.sleep(60)


if __name__ == "__main__":
    _main(_parse())
