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
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


def list_and_merge_pq_files(
    root_dir: str, fs: Any, *, file_name: str = "data.parquet"
) -> None:
    """
    Merge all files of the parquet dataset.

    Can be generalized to any used partition.

    The standard partition assumed is:

    ```
    root_dir/
        currency_pair=ADA_USDT/
            year=2021/
                month=12/
                    data.parquet
            year=2022/
                month=01/
                    data.parquet
        ...
        currency_pair=EOS_USDT/
            year=2021/
                month=12/
                    data.parquet
            year=2022/
                month=01/
                    data.parquet
    ```

    :param root_dir: root directory of PQ dataset
    :param fs: S3 filesystem columns on which the dataset is partitioned
    :param file_name: name of the single resulting file
    """
    # TODO(Danya): Expand to local filesystem.
    # Get full paths to each parquet file inside root dir.
    parquet_files = fs.glob(f"{root_dir}/**.parquet")
    _LOG.debug("Parquet files: '%s'", parquet_files)
    # Get paths only to the lowest level of dataset folders.
    dataset_folders = set(f.rsplit("/", 1)[0] for f in parquet_files)
    for folder in dataset_folders:
        # Get files per folder and merge if there are multiple ones.
        folder_files = fs.ls(folder)
        if len(folder_files) > 1:
            # Read all files in target folder.
            data = pq.ParquetDataset(folder_files, filesystem=fs).read()
            # Remove all old files and write new, merged one.
            fs.rm(folder, recursive=True)
            pq.write_table(data, folder + "/" + file_name, filesystem=fs)


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


def _run(args: argparse.Namespace) -> None:
    # Connect to S3 filesystem.
    fs = hs3.get_s3fs(args.aws_profile)
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
            filesystem=fs,
            partition_filename=None,
        )
        # Sleep between iterations.
        time.sleep(args.sleep_time)
    # Merge all new parquet into a single `data.parquet`.
    list_and_merge_pq_files(
        path_to_exchange,
        fs,
        file_name="data.parquet",
    )


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())