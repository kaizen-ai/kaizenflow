#!/usr/bin/env python
"""
Convert a directory storing Parquet files organized by dates into a Parquet
dataset organized by assets.

A parquet file organized by dates looks like:

```
src_dir/
    date1/
        data.parquet
    date2/
        data.parquet
```

A parquet file organized by assets looks like:

```
dst_dir
    year1/
        asset1/
            data.parquet
        asset2/
            data.parquet
    year/
        asset1/
            data.parquet
        asset2/
            data.parquet
```

# Example:
> im_v2/common/data/transform/convert_pq_by_date_to_by_asset.py \
    --src_dir im/transform/test_data_by_date \
    --dst_dir im/transform/test_data_by_asset
"""

import argparse
import datetime
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import helpers.dbg as hdbg
import helpers.hparquet as hparque
import helpers.printing as hprint
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)


# TODO(Nikola): Return the list so we can use tqdm as progress bar.
def _source_parquet_df_generator(src_dir: str) -> str:
    """
    Generator for all the Parquet files in a given dir.
    """
    hdbg.dassert_dir_exists(src_dir)
    # Find all the files with extension `.parquet` or `.pq`.
    src_pq_files = hio.find_files(src_dir, "*.parquet")
    if not src_pq_files:
        src_pq_files = hio.find_files(src_dir, "*.pq")
    _LOG.debug("Found %s pq files in '%s'", len(src_pq_files), src_dir)
    hdbg.dassert_lte(1, len(src_pq_files))
    # Return iterator.
    for src_pq_file in src_pq_files:
        yield src_pq_file


def _reindex_on_unix_epoch(df: pd.DataFrame, in_col_name) -> pd.DataFrame:
    """
    Transform the df so that `start_time` is converted into a datetime index.

    `start_time` contains an Unix epoch (e.g., 1638194400) and it's converted into
    a UTC time.
    """
    # Convert.
    hdbg.dassert_in(in_col_name, df.columns)
    srs = df[in_col_name].apply(
        datetime.datetime.fromtimestamp
    )
    srs = srs.dt.tz_localize("UTC")
    # Save.
    out_col_name = in_col_name + "_ts"
    hdbg.dassert_not_in(out_col_name, df.columns)
    df[out_col_name] = srs
    df.set_index(out_col_name, inplace=True, drop=True)
    df.index.name = None
    return df


def _save_pq_by_asset(df_by_date: pd.DataFrame, dst_dir: str) -> None:
    """
    Save a dataframe in a Parquet format in `dst_dir` partitioned by year and
    asset.
    """
    # Re-index by year.
    with htimer.TimedScope(logging.DEBUG, "Create partition indices"):
        year_col_name = "year"
        hdbg.dassert_not_in(year_col_name, df_by_date.columns)
        df_by_date[year_col_name] = df_by_date.index.year
    #
    with htimer.TimedScope(logging.DEBUG, "Save data"):
        # TODO(Nikola): This needs to be passed as param, since it changes.
        asset_col_name = "asset"
        table = pa.Table.from_pandas(df_by_date)
        partition_cols = [year_col_name, asset_col_name]
        pq.write_to_dataset(
            table,
            dst_dir,
            partition_cols=partition_cols,
            partition_filename_cb=lambda x: "data.parquet",
        )


# TODO(gp): We might want to use a config to pass a set of params related to each
#  other (e.g., transform_func, asset_col_name, ...)

def _run(src_dir: str, transform_func: str, dst_dir: str) -> None:
    # Convert the files one at the time.
    all_dfs = []
    for src_pq_file in _source_parquet_df_generator(src_dir):
        _LOG.debug("src_pq_file=%s", src_pq_file)
        # Load data.
        df = hparque.from_parquet(src_pq_file)
        _LOG.debug("before df=\n%s", hprint.dataframe_to_str(df.head(3)))
        # Transform.
        # TODO(gp): Use eval or a more general mechanism.
        if transform_func == "":
            pass
        elif transform_func == "reindex_on_unix_epoch":
            in_col_name = "start_time"
            df = _reindex_on_unix_epoch(df, in_col_name)
            _LOG.debug("after df=\n%s", hprint.dataframe_to_str(df.head(3)))
        else:
            hdbg.dfatal(f"Invalid transform_func='{transform_func}'")
        #
        all_dfs.append(df)
    # Concat df.
    df = pd.concat(all_dfs)
    # Save df after indexing by asset.
    # TODO(Nikola): We need to implement the approach so that it can be parallelized.
    _save_pq_by_asset(df, dst_dir)


# TODO(Nikola): Add support for reading (not writing) to S3. Reuse code from pq_convert.py.
#  When you transplant the code from that file, pls delete it from there so we know
#  it has been updated.

def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Source directory where original PQ files are stored.",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination directory where transformed PQ files will be stored.",
    )
    parser.add_argument("--no_incremental", action="store_true", help="")
    parser.add_argument("--transform_func", action="store", type=str, default="", help="Function to be use for transforming the df")
    #
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Source directory that contains original Parquet files.
    src_dir = args.src_dir
    # We assume that the destination dir doesn't exist so we don't override data.
    dst_dir = args.dst_dir
    if not args.no_incremental:
        # In not incremental mode the dir should already be there.
        hdbg.dassert_not_exists(dst_dir)
    hio.create_dir(dst_dir, incremental=False)
    #
    _run(src_dir, args.transform_func, dst_dir)


if __name__ == "__main__":
    _main(_parse())
