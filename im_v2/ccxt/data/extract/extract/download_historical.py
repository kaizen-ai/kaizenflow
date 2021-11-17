#!/usr/bin/env python
"""
Script to download historical data from CCXT.

Use as:

# Download data from 2019-01-01 to now, for latest trade universe:
> download_historical.py \
     --dst_dir 'test' \
     --universe 'v03' \
     --start_datetime '2019-01-01' \

Import as:

import im.ccxt.data.extract.download_historical as imcdedohi
"""

import argparse
import logging
import os
import time

import pandas as pd

import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import im_v2.ccxt.data.extract.exchange_class as imcdeexcl
import im.data.universe as imdatuniv

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
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
        default=imcdeexcl.API_KEYS_PATH,
        help="Path to JSON file that contains API keys for exchange access",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe for which to download data, e.g. 'latest', '01'",
    )
    parser.add_argument(
        "--start_datetime",
        action="store",
        required=True,
        type=str,
        help="Start date of download in iso8601 format, e.g. '2021-09-08T00:00:00.000Z'",
    )
    parser.add_argument(
        "--end_datetime",
        action="store",
        type=str,
        default=None,
        help="End date of download in iso8601 format, e.g. '2021-10-08T00:00:00.000Z'."
        "None means datetime.now())",
    )
    parser.add_argument(
        "--step",
        action="store",
        type=int,
        default=None,
        help="Size of each API request per iteration",
    )
    parser.add_argument(
        "--sleep_time",
        action="store",
        type=int,
        default=60,
        help="Sleep time between currency pair downloads (in seconds).",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the directory.
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # Pick start and end datetime.
    start_datetime = pd.Timestamp(args.start_datetime)
    if not args.end_datetime:
        # If end datetime is not provided, get current time.
        end_datetime = pd.Timestamp.now()
    else:
        end_datetime = pd.Timestamp(args.end_datetime)
    # Load trading universe.
    if args.universe == "latest":
        trade_universe = imdatuniv.get_trade_universe()["CCXT"]
    else:
        trade_universe = imdatuniv.get_trade_universe(args.universe)["CCXT"]
    _LOG.info("Getting data for exchanges %s", ", ".join(trade_universe.keys()))
    for exchange_id in trade_universe:
        # Initialize the exchange class.
        exchange = imcdeexcl.CcxtExchange(
            exchange_id, api_keys_path=args.api_keys
        )
        for pair in trade_universe[exchange_id]:
            _LOG.info(pair)
            # Download OHLCV data.
            pair_data = exchange.download_ohlcv_data(
                curr_symbol=pair,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                step=args.step,
            )
            # Sleep between iterations.
            time.sleep(args.sleep_time)
            # Create file name based on exchange and pair.
            # E.g. 'binance_BTC_USDT.csv.gz'
            file_name = f"{exchange_id}_{pair.replace('/', '_')}.csv.gz"
            full_path = os.path.join(args.dst_dir, file_name)
            # Save file.
            pair_data.to_csv(
                full_path,
                index=False,
                compression="gzip",
            )
            _LOG.debug("Saved data to %s", file_name)


if __name__ == "__main__":
    _main(_parse())
