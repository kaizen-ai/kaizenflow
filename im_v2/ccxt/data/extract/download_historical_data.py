#!/usr/bin/env python
"""
Download historical data from CCXT and save to S3. The script is meant to run
daily for reconciliation with realtime data.

Use as:

# Download data for CCXT for binance from 2022-02-08 to 2022-02-09:
> im_v2/ccxt/data/extract/download_historical_data.py \
     --end_timestamp '2022-02-09' \
     --start_timestamp '2022-02-08' \
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
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--sleep_time",
        action="store",
        type=int,
        default=5,
        help="Sleep time between currency pair downloads (in seconds).",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = hs3.add_s3_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _run(args: argparse.Namespace) -> None:
    # Initialize exchange class.
    exchange = imvcdeexcl.CcxtExchange(args.exchange_id)
    # Load trading universe.
    universe = imvccunun.get_trade_universe(args.universe)
    # Load a list of currency pars.
    currency_pairs = universe["CCXT"][args.exchange_id]
    # Convert timestamps.
    end_timestamp = pd.Timestamp(args.end_timestamp)
    start_timestamp = pd.Timestamp(args.start_timestamp)
    path_to_exchange = os.path.join(args.s3_path, args.exchange_id)
    for currency_pair in currency_pairs:
        # Download OHLCV data.
        data = exchange.download_ohlcv_data(
            currency_pair.replace("_", "/"),
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        # Assign pair and exchange columns.
        # TODO(Nikola): Exchange id was missing and it is added additionally to
        #  match signature of other scripts.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args.exchange_id
        # Change index to allow calling add_date_partition_cols function on the dataframe.
        data = imvcdttrut.reindex_on_datetime(data, "timestamp")
        data, partition_cols = hparque.add_date_partition_columns(
            data, "by_year_month"
        )
        # Get current time of push to s3 in UTC.
        knowledge_timestamp = hdateti.get_current_time("UTC")
        data["knowledge_timestamp"] = knowledge_timestamp
        # Save data to S3 filesystem.
        # Saves filename as `uuid`.
        hparque.to_partitioned_parquet(
            data,
            ["currency_pair"] + partition_cols,
            path_to_exchange,
            partition_filename=None,
            aws_profile=args.aws_profile,
        )
        # Sleep between iterations.
        time.sleep(args.sleep_time)
    # Merge all new parquet into a single `data.parquet`.
    hparque.list_and_merge_pq_files(
        path_to_exchange, aws_profile=args.aws_profile
    )


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
