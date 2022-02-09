#!/usr/bin/env python
"""
Script to download historical data from CCXT.

Use as:

# Download data for CCXT for trading universe `v03` from 2019-01-01 to now:
> download_historical_data.py \
     --dst_dir 'test' \
     --universe 'v03' \
     --start_datetime '2019-01-01'

Import as:

import im_v2.ccxt.data.extract.download_historical_data as imvcdedhda
"""

import argparse
import logging
import os
import time

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.ccxt.universe.universe as imvccunun

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start_datetime",
        action="store",
        required=True,
        type=str,
        help="Start date of download to parse with pd.Timestamp",
    )
    parser.add_argument(
        "--end_datetime",
        action="store",
        required=True,
        type=str,
        help="End date of download to parse with pd.Timestamp. "
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of exchange to download data from",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe to download data for"
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
        default=10,
        help="Sleep time between currency pair downloads (in seconds).",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = hs3.add_s3_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Connect to S3 filesystem, if provided.
    if args.aws_profile:
        fs = hs3.get_s3fs(args.aws_profile)
    # Load trading universe.
    universe = imvccunun.get_trade_universe(args.universe)
    # Load a list of currency pars.
    currency_pairs = universe["CCXT"][args.exchange_id]
    # Convert timestamps.
    end = pd.Timestamp(args.to_datetime)
    start = pd.Timestamp(args.from_datetime)
    # TODO(Danya): Only 1 exchange id.
    for exchange_id in trade_universe:
        # Initialize the exchange class.
        exchange = imvcdeexcl.CcxtExchange(exchange_id)
        for currency_pair in trade_universe[exchange_id]:
            _LOG.info("Downloading currency pair '%s'", currency_pair)
            # Download OHLCV data.
            currency_pair_data = exchange.download_ohlcv_data(
                currency_pair,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                bar_per_iteration=args.step,
            )
            # Sleep between iterations.
            time.sleep(args.sleep_time)
            # Create file name based on exchange and currency pair.
            # E.g. 'binance_BTC_USDT.csv.gz'
            # TODO(Danya): Filename = currency_pair_timestamp.
            file_name = f"{exchange_id}-{currency_pair}.csv.gz"
            full_path = os.path.join(args.dst_dir, file_name)
            # Save file.
            currency_pair_data.to_csv(
                full_path,
                index=False,
                compression="gzip",
            )
            _LOG.debug("Saved data to %s", file_name)


if __name__ == "__main__":
    _main(_parse())
