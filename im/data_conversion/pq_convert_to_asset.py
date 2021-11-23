#!/usr/bin/env python

# TODO(nikolas): Rename the script `convert_pq_by_date_to_by_asset.py`


"""
Convert a directory storing Parquet files organized by dates into a Parquet dataset
organized by assets.

A parquet file organized by dates looks like:
```
TODO(nikola): Complete

A parquet file organized by assets looks like:
...
TODO(nikola): Complete

# Example:
> python im/data_conversion/pq_convert_to_asset.py \
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
# TODO: -> import helpers.hparquet as hparquet
from helpers.hparquet import from_parquet

_LOG = logging.getLogger(__name__)


# -> _source_...
# TODO(gp): Let's split the file generation from reading since this will help
#  later with parallelizing.
def source_parquet_df_generator(src_dir: str) -> pd.DataFrame:
    """
    Generator for all the Parquet files in a given dir.
    """
    hdbg.dassert_dir_exists(src_dir)
    src_pq_files = hio.find_files(src_dir, "*.parquet")
    # TODO(nikolas): If there are no parquet files then look for `*.pq`
    if not src_pq_files:
        src_pq_files = hio.find_files(src_dir, "*.pq")
    _LOG.debug("Found %s pq files in '%s'", len(src_pq_files), src_dir)
    for src_pq_file in src_pq_files:
        yield src_pq_file


# -> _save_pq_by_asset
# TODO: -> We can remove this. Let's always save by asset and by year.
def save_data_as_pq_by_asset(df, dst_dir):
    """
    Save a single dataframe in a Parquet by-asset format in dst_dir.
    """
    col_name = "year"
    hdbg.dassert_not_in(col_name, df)
    # Partition the data by year.
    with htimer.TimedScope(logging.INFO, "Create partition indices"):
        df[col_name] = df.index.year
    # Save the data as PQ partitioned by year.
    with htimer.TimedScope(logging.INFO, "Save data"):
        table = pa.Table.from_pandas(df)
        partition_cols = col_name
        pq.write_to_dataset(table, dst_dir, partition_cols=partition_cols)


# -> _save_pq_by_asset
# TODO: df, dst_dir since df is an input and dst_dir is an "ouput".
def parquet_by_asset_save(dst_dir: str, parquet_df_by_date: pd.DataFrame) -> None:
    """
    Save a dataframe in a Parquet format in `dst_dir` partitioned by year and asset.

    The output layout looks like:
    ```
    ...
    ```
    """
    # TODO: same as before
    year_col_name = ...
    asset_col_name = ...
    with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
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
    #
    src_dir = args.src_dir
    # We assume that the destination dir doesn't exist so we don't override data.
    dst_dir = args.dst_dir
    hdbg.dassert_not_exists(dst_dir)
    hio.create_dir(dst_dir, incremental=False)
    from pudb import set_trace; set_trace()
    # Convert the files one at the time.
    for src_pq_file in source_parquet_df_generator(src_dir):
        df = from_parquet(src_pq_file)
        parquet_by_asset_save(dst_dir, df)


if __name__ == "__main__":
    _main(_parse())
