#!/usr/bin/env python
"""
Script to download data from CCXT in real-time.
"""
import argparse
import logging
import os
import time

import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import im.ccxt.data.extract.exchange_class as deecla
import helpers.datetime_ as hdt

_LOG = logging.getLogger(__name__)


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
        default=deecla.API_KEYS_PATH,
        help="Path to JSON file that contains API keys for exchange access",
    )
    parser.add_argument(
        "--exchange_ids",
        action="store",
        required=True,
        type=str,
        help="CCXT names of exchanges to download data for, separated by spaces, e.g. 'binance gemini',"
        "'all' for each exchange (currently includes Binance and Kucoin by default)",
    )
    parser.add_argument(
        "--currency_pairs",
        action="store",
        required=True,
        type=str,
        help="Name of the currency pair to download data for, separated by spaces, e.g. 'BTC/USD ETH/USD',"
        " 'all' for each currency pair in exchange",
    )
    parser.add_argument(
        # TODO(Danya): remove after adding the SQL connection.
        "--incremental",
        action="store_true"
    )
    parser = hparse.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the directory.
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # If end_date is not provided, get current time.
    if args.exchange_ids == "all":
        # Iterate over all available exchanges.
        exchange_ids = ["binance", "kucoin"]
    else:
        # Get provided exchanges.
        exchange_ids = args.exchange_ids.split()
    exchanges = dict()
    currency_pairs = dict()
    for exchange_id in exchange_ids:
        # Initialize a class instance for each provided exchange.
        exchange_class = deecla.CcxtExchange(exchange_id, api_keys_path=args.api_keys)
        # Store the exchange class instance.
        exchanges[exchange_id] = exchange_class
        if args.currency_pairs == "all":
            # Store all currency pairs for each exchange.
            currency_pairs[exchange_id] = exchange_class.currency_pairs
        else:
            # Iterate over provided currencies.
            currency_pairs = args.currency_pairs.split()
            # Store currency pairs present in each exchange.
            currency_pairs[exchange_id] = [
                curr for curr in currency_pairs if curr in exchange_class.currency_pairs
            ]
    # Launch an infinite loop.
    while True:
        for exchange_id in exchange_ids:
            for pair in currency_pairs[exchange_id]:
                # Download latest 5 minutes for the currency pair and exchange.
                pair_data = exchanges[exchange_id].download_ohlcv_data(curr_symbol=pair, step=5)
                # Save data with timestamp.
                # TODO (Danya): replace saving with DB update.
                file_name = f"{exchange_id}_{pair.replace('/', '_')}_{hdt.get_timestamp('ET')}"
                file_path = os.path.join(args.dst_dir, file_name)
                _LOG.warning("Saved to %s", file_path)
                pair_data.to_csv(file_path)
                time.sleep(60)


if __name__ == "__main__":
    _main(_parse())
