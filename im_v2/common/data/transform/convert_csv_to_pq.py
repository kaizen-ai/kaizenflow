#!/usr/bin/env python
"""
Convert data from CSV to Parquet files and partition dataset by asset.

A Parquet file partitioned by assets looks like:
```
dst_dir/
    year=2021/
        month=12/
           day=11/
               asset=BTC_USDT/
                    data.parquet
               asset=ETH_USDT/
                    data.parquet
```

# Use example:
> csv_to_pq.py \
    --src_dir test/ccxt_test \
    --dst_dir test_pq

Import as:

import im_v2.common.data.transform.convert_csv_to_pq as imvcdtcctp
"""

# TODO(gp): -> transform_csv_to_pq

import argparse
import logging
import os
from typing import List, Tuple

import pyarrow.dataset as ds

import helpers.hcsv as hcsv
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparquet as hparque
import helpers.hparser as hparser
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


def _get_csv_to_pq_file_names(
    src_dir: str, dst_dir: str, incremental: bool
) -> List[Tuple[str, str]]:
    """
    Find all the CSV files in `src_dir` to transform and prepare the
    corresponding destination Parquet files.

    :param incremental: if True, skip CSV files for which the corresponding Parquet
        file already exists
    :return: list of tuples (csv_file, pq_file)
    """
    # Find all the CSV files to convert.
    csv_filenames = []
    for filename in os.listdir(src_dir):
        # Find the CSV files.
        csv_ext = ".csv"
        csv_gz_ext = ".csv.gz"
        csv_filename = filename
        if filename.endswith(csv_ext):
            csv_filename = filename[: -len(csv_ext)]
        elif filename.endswith(csv_gz_ext):
            csv_filename = filename[: -len(csv_gz_ext)]
        else:
            _LOG.warning("Found non CSV file '%s'", filename)
        # Build corresponding Parquet file.
        pq_path = os.path.join(dst_dir, f"{csv_filename}.parquet")
        csv_path = os.path.join(src_dir, filename)
        # Skip CSV files that do not need to be converted.
        # TODO(gp): Try to use hjoblib.apply_incremental_mode
        if incremental and os.path.exists(pq_path):
            _LOG.warning(
                "Skipping conversion of CSV file '%s' since '%s' already exists",
                csv_path,
                pq_path,
            )
        else:
            csv_filenames.append((csv_path, pq_path))
    return csv_filenames


def _run(args: argparse.Namespace) -> None:
    # TODO(gp): @danya use hparser.create_incremental_dir(args.dst_dir, args).
    hio.create_dir(args.dst_dir, args.incremental)
    # Find all CSV and Parquet files.
    files = _get_csv_to_pq_file_names(
        args.src_dir, args.dst_dir, args.incremental
    )
    # Convert CSV files into Parquet files.
    for csv_full_path, pq_full_path in files:
        hcsv.convert_csv_to_pq(csv_full_path, pq_full_path)
    # Convert Parquet files into a different partitioning scheme.
    # TODO(gp): IMO this is a different / optional step.
    dataset = ds.dataset(args.dst_dir, format="parquet", partitioning="hive")
    # TODO(gp): Not sure this can be done since all the CSV files might not fit in
    #  memory.
    df = dataset.to_table().to_pandas()
    # Set datetime index.
    reindexed_df = imvcdttrut.reindex_on_datetime(df, args.datetime_col)
    # Add date partition columns to the dataframe.
    df, partition_cols = hparque.add_date_partition_columns(
        reindexed_df, "by_date"
    )
    hparque.to_partitioned_parquet(df, partition_cols, args.dst_dir)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Dir with input CSV files to convert to Parquet format",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination dir where to save converted Parquet files",
    )
    parser.add_argument(
        "--datetime_col",
        action="store",
        type=str,
        required=True,
        help="Name of column containing datetime information",
    )
    parser.add_argument(
        "--asset_col",
        action="store",
        type=str,
        default=None,
        help="Name of column containing asset name for partitioning by asset",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files that have already been converted",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
