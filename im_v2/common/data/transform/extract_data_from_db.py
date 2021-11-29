#!/usr/bin/env python
"""
Extract RT data from db to daily PQ files.

# Example:
> python im_v2/common/data/transform/extract_data_from_db.py \
    --start_date 2021-11-23 \
    --end_date 2021-11-25 \
    --daily_pq_path im_v2/common/data/transform/test_data_by_date

Import as:

import im_v2.common.data.transform.extract_data_from_db as imvcdtedfd
"""

import argparse
import logging

import pandas as pd

import helpers.dbg as hdbg
import helpers.hparquet as hparque
import helpers.parser as hparser
import helpers.sql as hsql
import im_v2.ccxt.data.client.clients as imvcdclcl

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
        help="From when is data going to be extracted, including start date.",
    )
    parser.add_argument(
        "--end_date",
        action="store",
        type=str,
        required=True,
        help="Until when is data going to be extracted, excluding end date.",
    )
    parser.add_argument(
        "--daily_pq_path",
        action="store",
        type=str,
        required=True,
        help="Location of daily PQ files.",
    )
    # TODO(Nikola): Additional args ?
    #
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Extraction timespan.
    start_date = args.start_date
    end_date = args.end_date
    # TODO(Nikola): Raise exception on date mismatch.
    # Location of daily PQ files.
    daily_pq_path = args.daily_pq_path
    hdbg.dassert_exists(daily_pq_path)

    timespan = pd.date_range(start_date, end_date)
    # TODO(Nikola): Are there any existing custom exceptions ?
    # if len(timespan) < 2:
    #     raise Exception
    # TODO(Nikola): Is connection eventually closed ?
    ccxt_db_client = imvcdclcl.CcxtDbClient(
        "ohlcv", hsql.get_connection_from_env_vars()
    )
    for date_index in range(len(timespan) - 1):
        _LOG.debug("Checking for RT data on %s.", timespan[date_index])
        # TODO(Nikola): Sort by timestamp in df or in SQL query ? Needed ?
        #   Refactor to use one db call.
        rt_df = ccxt_db_client._read_data(
            start_ts=timespan[date_index], end_ts=timespan[date_index + 1]
        )
        if rt_df.empty:
            _LOG.debug("No RT date in db for %s.", timespan[date_index])
            continue
        try:
            date_directory = f"date={timespan[date_index].strftime('%Y%m%d')}"
            full_path = f"{daily_pq_path}/{date_directory}"
            # TODO(Nikola): Use part of _source_parquet_df_generator instead.
            hdbg.dassert_not_exists(full_path)
            # TODO(Nikola): Should id be removed ? Timestamp as index ?
            hparque.save_daily_df_as_pq(rt_df, daily_pq_path)
        except AssertionError as ex:
            _LOG.debug("Skipping. PQ file already present: %s.", ex)
            continue

    # TODO(Nikola): Save to S3 as an extra option ? Or totally different script ?


if __name__ == "__main__":
    _main(_parse())
