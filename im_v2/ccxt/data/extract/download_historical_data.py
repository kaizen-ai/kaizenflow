#!/usr/bin/env python
"""
Download historical data from CCXT and save to S3. The script is meant to run
daily for reconciliation with realtime data.

Use as:

# Download data for CCXT for binance from 2022-02-08 to 2022-02-09:
> im_v2/ccxt/data/extract/download_historical_data.py \
     --to_datetime '2022-02-09' \
     --from_datetime '2022-02-08' \
     --exchange_id 'binance' \
     --universe 'v03' \
     --aws_profile 'ck' \
     --s3_path 's3://cryptokaizen-data/historical/'
"""

import argparse
import logging
import os
import time

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
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
        "--to_datetime",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded period",
    )
    parser.add_argument(
        "--from_datetime",
        action="store",
        required=True,
        type=str,
        help="Beginning of the downloaded period",
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
        help="Trade universe to download data for",
    )
    parser.add_argument(
        "--sleep_time",
        action="store",
        type=int,
        default=5,
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
    exchange = imvcdeexcl.CcxtExchange(args.exchange_id)
    # Load trading universe.
    universe = imvccunun.get_trade_universe(args.universe)
    # Load a list of currency pars.
    currency_pairs = universe["CCXT"][args.exchange_id]
    # Convert timestamps.
    end_datetime = pd.Timestamp(args.to_datetime)
    start_datetime = pd.Timestamp(args.from_datetime)
    for currency_pair in currency_pairs:
        # Download OHLCV data.
        data = exchange.download_ohlcv_data(
            currency_pair.replace("_", "/"),
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        data["currency_pair"] = currency_pair
        # Get datetime of push to s3 in UTC.
        knowledge_timestamp = hdateti.get_current_timestamp_as_string("UTC")
        data["knowledge_timestamp"] = knowledge_timestamp
        # Get file name.
        file_name = (
            currency_pair
            + "_"
            + knowledge_timestamp
            + ".csv"
        )
        path_to_file = os.path.join(args.s3_path, args.exchange_id, file_name)
        # Save data to S3 filesystem.
        with fs.open(path_to_file, "w") as f:
            data.to_csv(f, index=False)
        # Sleep between iterations.
        time.sleep(args.sleep_time)


if __name__ == "__main__":
    _main(_parse())
