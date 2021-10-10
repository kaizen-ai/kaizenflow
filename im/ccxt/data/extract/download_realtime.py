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
"""
import argparse
import logging
import os
import time

import helpers.datetime_ as hdt
import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import im.ccxt.data.extract.exchange_class as deecla

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--db",
        action="store",
        # TODO(Danya): set db connections, if needed, as labels that are used to
        #  set db connection afterwards. Includes DB name and stage.
        default="from_env",
        type=str,
        help="db to connect to."
    )
    parser.add_argument(
        "--table_name",
        action="store",
        required=True,
        type=str,
        help="Name of the table to update",
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
        action="store_true",
    )
    parser = hparse.add_verbosity_arg(parser)
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
    exchange_id_to_class = dict()
    exchange_id_to_currency_pairs = dict()
    for exchange_id in exchange_ids:
        # Initialize a class instance for each provided exchange.
        exchange_class = deecla.CcxtExchange(
            exchange_id, api_keys_path=args.api_keys
        )
        # Store the exchange class instance.
        exchange_id_to_class[exchange_id] = exchange_class
        if args.currency_pairs == "all":
            # Store all currency pairs for each exchange.
            exchange_id_to_currency_pairs[
                exchange_id
            ] = exchange_class.currency_pairs
        else:
            # Store currency pairs present in provided exchanges.
            provided_pairs = args.currency_pairs.split()
            exchange_id_to_currency_pairs[exchange_id] = [
                curr
                for curr in provided_pairs
                if curr in exchange_class.currency_pairs
            ]
    # Launch an infinite loop.
    while True:
        for exchange_id in exchange_ids:
            for pair in exchange_id_to_currency_pairs[exchange_id]:
                # Download latest 5 minutes for the currency pair and exchange.
                pair_data = exchange_id_to_class[exchange_id].download_ohlcv_data(
                    curr_symbol=pair, step=5
                )
                # Save data with timestamp.
                # TODO(Danya): replace saving with DB update.
                file_name = f"{exchange_id}_{pair.replace('/', '_')}_{hdt.get_timestamp('ET')}.csv.gz"
                file_path = os.path.join(args.dst_dir, file_name)
                pair_data.to_csv(file_path, index=False, compression="gzip")
        time.sleep(60)


if __name__ == "__main__":
    _main(_parse())
