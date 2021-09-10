"""
Script to download historical data from CCXT.
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
        "--file_name",
        action="store",
        required=True,
        type=str,
        help="Full path to the output file",
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
    dbg.dassert_file_extension(args.file_name, ".csv.gz")
    # Create the dst dir.
    hio.create_enclosing_dir(args.file_name, incremental=args.incremental)
    # Initialize the exchange class.
    exchange = icec.CCXTExchange(args.exchange_id)
    # Download OHLCV.
    start_date = args.start_datetime
    # If end_date is not provided, get current time in ms.
    #  Note: utilizes CCXT's method that provides data in correct format.
    end_date = args.end_datetime or exchange._exchange.milliseconds()
    ohlcv_data = exchange.download_ohlcv_data(
        start_date, end_date, curr_symbol=args.currency_pair, step=args.step
    )
    # Transform to dataframe.
    ohlcv_df = pd.DataFrame(
        ohlcv_data,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    # Save as single .csv file.
    full_path = os.path.join(args.dst_dir, args.file_name)
    ohlcv_df.to_csv(full_path, index=False, compression="gzip")
    _LOG.info("Saved to %s" % full_path)
    return None


if __name__ == "__main__":
    _main(_parse())
