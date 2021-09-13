"""
Script to download historical data from CCXT.

Use as:

- Download Binance BTC/USDT data from 2019-01-01 to 2019-01-02:
> download_historical.py \
     --dst_dir test \
     --exchange_id "binance" \
     --currency_pair "BTC/USDT" \
     --start_datetime "2019-01-01" \
     --end_datetime "2019-01-02"

- Download Binance data from 2019-01-01 to now,
  for all currency pairs:
> download_historical.py \
     --dst_dir test \
     --exchange_id "binance" \
     --currency_pair "all" \
     --start_datetime "2019-01-01" \

- Download data for all exchanges, BTC/USDT currency pair,
from 2019-01-01 to now:
> download_historical.py \
     --dst_dir test \
     --exchange_id "all" \
     --currency_pair "BTC/USDT" \
     --start_datetime "2019-01-01" \

- Download data for all exchanges and all pairs,
from 2019-01-01 to 2019-01-02:
> download_historical.py \
     --dst_dir test \
     --exchange_id "all" \
     --currency_pair "all" \
     --start_datetime "2019-01-01" \
     --start_endtime "2019-01-02"
"""

import argparse
import logging
import os

import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as prsr
import im.ccxt.exchange_class as icec

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
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="CCXT name of the exchange to download data for, e.g. 'binance'",
    )
    parser.add_argument(
        "--currency_pair",
        action="store",
        required=True,
        type=str,
        help="Name of the currency pair to download data for, e.g. 'BTC/USD',"
        " 'all' for each currency pair in exchange",
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
        "Optional, defaults to datetime.now())",
    )
    parser.add_argument(
        "--step",
        action="store",
        type=int,
        default=None,
        help="Size of each API request per iteration",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = prsr.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the directory.
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    start_datetime = pd.Timestamp(args.start_datetime)
    # If end_date is not provided, get current time.
    if not args.end_datetime:
        end_datetime = pd.Timestamp.now()
    else:
        end_datetime = pd.Timestamp(args.end_datetime)
    if args.exchange_id == "all":
        # Iterate over all available exchanges.
        exchange_ids = ["binance", "kucoin"]
    else:
        # Get a single exchange.
        exchange_ids = [args.exchange_id]
    for exchange_id in exchange_ids:
        ohlcv_df = []
        # Initialize the exchange class.
        exchange = icec.CCXTExchange(exchange_id)
        if args.currency_pair == "all":
            # Iterate over all currencies available for exchange.
            currency_pairs = exchange.currency_pairs
        else:
            # Iterate over single provided currency.
            currency_pairs = [args.currency_pair]
        for pair in currency_pairs:
            # Download OHLCV data.
            pair_data = exchange.download_ohlcv_data(
                start_datetime, end_datetime, curr_symbol=pair, step=args.step
            )
            # Save file.
            pair_data.to_csv(os.path.join(args.dst_dir, f"{exchange_id}_{pair}.csv.gz"), index=False, compression="gzip")
    _LOG.info("Saved to %s" % args.file_name)
    return None


if __name__ == "__main__":
    _main(_parse())
