#!/usr/bin/env python
"""
Extract RT data from db to daily PQ files.

# Example:
> extract_data_from_db.py \
    --start_date 2021-11-23 \
    --end_date 2021-11-25 \
    --dst_dir im_v2/common/data/transform/test_data_by_date

Import as:

import im_v2.common.data.transform.extract_data_from_db as imvcdtedfd
"""

import argparse
import logging
import os.path

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.ccxt.data.client.ccxt_clients as imvcdccccl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--start_date",
        action="store",
        type=str,
        required=True,
        help="From when is data going to be extracted, including start date",
    )
    parser.add_argument(
        "--end_date",
        action="store",
        type=str,
        required=True,
        help="Until when is data going to be extracted, excluding end date",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Location of daily PQ files",
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        type=str,
        default="local",
        help="Which env is used: local, dev or prod",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    """
    Standard main part of the script that is parsing provided arguments.

    Timespan provided via start and end date, can not start and end on
    the same day. Start date is included in timespan, while end date is
    excluded.
    """
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Generate timespan.
    start_date = args.start_date
    end_date = args.end_date
    hdbg.dassert_lt(start_date, end_date)
    timespan = pd.date_range(start_date, end_date)
    hdbg.dassert_lt(2, len(timespan))
    # Create location of daily PQ files.
    dst_dir = args.dst_dir
    hdbg.dassert_exists(dst_dir)
    # Connect to database.
    db_stage = args.db_stage
    env_file = imvimlita.get_db_env_path(db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Initiate DB client.
    ccxt_db_client = imvcdccccl.CcxtDbClient("ohlcv", connection)
    # Get universe of symbols.
    symbols = imvccunun.get_vendor_universe()
    for date_index in range(len(timespan) - 1):
        _LOG.debug("Checking for RT data on %s.", timespan[date_index])
        # TODO(Nikola): Refactor to use one db call.
        df = ccxt_db_client.read_data(
            symbols,
            start_ts=timespan[date_index],
            end_ts=timespan[date_index + 1],
            normalize=False,
        )
        if df.empty:
            _LOG.info("No RT date in db for %s.", timespan[date_index])
            continue
        try:
            # Check if directory already exists in specified path.
            date_directory = f"date={timespan[date_index].strftime('%Y%m%d')}"
            full_path = os.path.join(dst_dir, date_directory)
            hdbg.dassert_not_exists(full_path)
            # Set datetime index.
            datetime_col_name = "start_time"
            reindexed_df = imvcdttrut.reindex_on_datetime(df, datetime_col_name)
            # Add date partition columns to the dataframe.
            imvcdttrut.add_date_partition_cols(reindexed_df)
            # Partition and write dataset.
            partition_cols = ["date"]
            imvcdttrut.partition_dataset(reindexed_df, partition_cols, dst_dir)
        except AssertionError as ex:
            _LOG.info("Skipping. PQ file already present: %s.", ex)
            continue


if __name__ == "__main__":
    _main(_parse())
