#!/usr/bin/env python

"""
Run example:
python im/data_conversion/pq_convert_to_asset.py \
    --src_dir im/data_conversion/test_data_by_date \
    --dst_dir im/data_conversion/test_data_by_asset
"""

import argparse
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.timer as htimer
from helpers.hparquet import from_parquet

_LOG = logging.getLogger(__name__)


def save_data_as_pq_by_asset(df, dst_dir):
    with htimer.TimedScope(logging.DEBUG, "Create partition idxs"):
        df["year"] = df.index.year
    # Save.
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(df)
        partition_cols = ["year"]
        pq.write_to_dataset(table, dst_dir, partition_cols=partition_cols)


def source_parquet_df_generator(src_dir: str) -> pd.DataFrame:
    src_pq_files = hio.find_files(src_dir, "*.parquet")
    for src_pq_file in src_pq_files:
        yield from_parquet(src_pq_file)


def parquet_by_asset_save(dst_dir: str, parquet_df_by_date: pd.DataFrame) -> None:
    with htimer.TimedScope(logging.DEBUG, "Create partition idxs"):
        parquet_df_by_date["year"] = parquet_df_by_date.index.year
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(parquet_df_by_date)
        partition_cols = ["year", "asset"]
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_cols,
            partition_filename_cb=lambda x: 'data.parquet'
        )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # TODO(Nikola): Additional args ?
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Source directory where original pq files are stored."
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination directory where transformed pq files are stored."
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dst_dir = args.dst_dir
    # TODO(Nikola): Double check abort if exists!
    hio.create_dir(dst_dir, incremental=True)
    src_dir = args.src_dir
    from pudb import set_trace; set_trace()
    for source_parquet_df in source_parquet_df_generator(src_dir):
        parquet_by_asset_save(dst_dir, source_parquet_df)


if __name__ == "__main__":
    _main(_parse())
