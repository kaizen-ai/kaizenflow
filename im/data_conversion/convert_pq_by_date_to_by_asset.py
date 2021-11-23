#!/usr/bin/env python
"""
Convert a directory storing Parquet files organized by dates into a Parquet dataset
organized by assets.

A parquet file organized by dates looks like:

src_dir
    date
        data.parquet
    date
        data.parquet

A parquet file organized by assets looks like:

dst_dir
    year
        asset
            data.parquet
        asset
            data.parquet
    year
        asset
            data.parquet
        asset
            data.parquet

# Example:
> python im/data_conversion/convert_pq_by_date_to_by_asset.py \
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
import helpers.hparquet as hparquet

_LOG = logging.getLogger(__name__)


def _source_parquet_df_generator(src_dir: str) -> pd.DataFrame:
    """
    Generator for all the Parquet files in a given dir.
    """
    hdbg.dassert_dir_exists(src_dir)
    src_pq_files = hio.find_files(src_dir, "*.parquet")
    if not src_pq_files:
        src_pq_files = hio.find_files(src_dir, "*.pq")
    _LOG.debug("Found %s pq files in '%s'", len(src_pq_files), src_dir)
    for src_pq_file in src_pq_files:
        yield src_pq_file


def _save_pq_by_asset(parquet_df_by_date: pd.DataFrame, dst_dir: str) -> None:
    """
    Save a dataframe in a Parquet format in `dst_dir` partitioned by year and asset.
    """
    year_col_name = "year"
    asset_col_name = "asset"
    with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
        parquet_df_by_date[year_col_name] = parquet_df_by_date.index.year
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        table = pa.Table.from_pandas(parquet_df_by_date)
        partition_cols = [year_col_name, asset_col_name]
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
        help="Destination directory where transformed pq files will be stored."
    )
    # TODO(Nikola): Additional args ?
    #
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Source directory that contains original parquet files.
    src_dir = args.src_dir
    # We assume that the destination dir doesn't exist so we don't override data.
    dst_dir = args.dst_dir
    hdbg.dassert_not_exists(dst_dir)
    hio.create_dir(dst_dir, incremental=False)
    # Convert the files one at the time.
    all_dfs = []
    for src_pq_file in _source_parquet_df_generator(src_dir):
        all_dfs.append(hparquet.from_parquet(src_pq_file))
    _save_pq_by_asset(pd.concat(all_dfs), dst_dir)


if __name__ == "__main__":
    _main(_parse())
