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
import re

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hparquet as hparque
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.transform.transform_utils as imvctrtrul
from typing import List

_LOG = logging.getLogger(__name__)


def list_pq_files(root_dir: str, partition_columns: List[str], start_timestamp: pd.Timestamp,
                   end_timestamp: pd.Timestamp, fs,
                   *, file_name: str = "data.pq"):
    """
    Go through Parquet Dataset folders and merge multiple files in one.
    :param root_dir: root directory of PQ dataset
    :param partition_columns: columns on which the dataset is partitioned
    :param file_name: name of the single resulting file
    """
    all_contents = fs.glob(f"{root_dir}/**.parquet")
    print(all_contents)
    #dataset_folders = [c for c in dataset_folders if not c.endswith(".parquet")]
    #for folder in dataset_folders:
    #    contents = fs.ls(folder)
    #    if len(contents) > 1:



def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start_timestamp",
        action="store",
        required=True,
        type=str,
        help="Beginning of the downloaded period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded period",
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
    end_timestamp = pd.Timestamp(args.end_timestamp)
    start_timestamp = pd.Timestamp(args.start_timestamp)
    for currency_pair in currency_pairs[0:1]:
        # Download OHLCV data.
        data = exchange.download_ohlcv_data(
            currency_pair.replace("_", "/"),
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        data["currency_pair"] = currency_pair
        # Change index to allow calling add_date_partition_cols function on the dataframe.
        data = imvctrtrul.reindex_on_datetime(data, "timestamp")
        #print(data.head())
        data, partition_cols = hparque.add_date_partition_columns(data, "by_year_month")
        path_to_file = os.path.join(args.s3_path, args.exchange_id)
        # Get timestamp of push to s3 in UTC.
        knowledge_timestamp = hdateti.get_current_timestamp_as_string("UTC")
        data["knowledge_timestamp"] = knowledge_timestamp
        # Save data to S3 filesystem.
        #if hs3.dassert_s3_exists(path_to_file, fs):
            # Merge
        #    pass
        #else:
            # Default file_name is data.parquet.
        hparque.to_partitioned_parquet(data, ["currency_pair"] + partition_cols, path_to_file, filesystem=fs)
        # Sleep between iterations.
        time.sleep(args.sleep_time)
    merge_pq_files(path_to_file, ["currency_pair"] + partition_cols, start_timestamp, end_timestamp, fs=fs)


if __name__ == "__main__":
    _main(_parse())
