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

Usage sample:

> im_v2/common/data/transform/convert_csv_to_pq.py \
     --src_dir 's3://cryptokaizen-data/historical/binance/' \
     --dst_dir 's3://cryptokaizen-data/historical/binance_parquet/' \
     --datetime_col 'timestamp' \
     --asset_col 'currency_pair' \
     --aws_profile 'ck'
"""

# TODO(gp): -> transform_csv_to_pq

import argparse
import logging
import os
from typing import List, Optional, Tuple

import pyarrow.dataset as ds
import s3fs

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.common.data.transform.transform_utils as imvcdttrut

_LOG = logging.getLogger(__name__)


def _get_csv_to_pq_file_names(
    src_dir: str,
    dst_dir: str,
    incremental: bool,
    *,
    s3fs_: Optional[s3fs.core.S3FileSystem] = None,
) -> List[Tuple[str, str]]:
    """
    Find all the CSV files in `src_dir` to transform and prepare the
    corresponding destination Parquet files.

    :param incremental: if True, skip CSV files for which the corresponding Parquet
        file already exists
    :param s3fs_: S3FS, if not, local FS is assumed
    :return: list of tuples (csv_file, pq_file)
    """
    # Collect the files (on S3 or on the local filesystem) that need to be transformed.
    original_files = []
    # TODO(Nikola): Remove section below after CMTask1440 is done.
    if s3fs_:
        exists_check = s3fs_.exists
        original_files.extend(s3fs_.listdir(src_dir))
        # Get the actual file paths from metadata.
        original_files = [file["Key"] for file in original_files]
    else:
        exists_check = os.path.exists
        original_files.extend(os.listdir(src_dir))
    # Find all the CSV files to convert.
    csv_filenames = []
    for filename in original_files:
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
        if s3fs_:
            # Full path is already present in filenames when iterating in S3.
            pq_path = (
                f"{dst_dir.lstrip('s3://')}{csv_filename.split('/')[-1]}.parquet"
            )
            csv_path = filename
        else:
            pq_path = os.path.join(dst_dir, f"{csv_filename}.parquet")
            csv_path = os.path.join(src_dir, filename)
        # Skip CSV files that do not need to be converted.
        # TODO(gp): Try to use hjoblib.apply_incremental_mode
        if incremental and exists_check(pq_path):
            _LOG.warning(
                "Skipping conversion of CSV file '%s' since '%s' already exists",
                csv_path,
                pq_path,
            )
        else:
            csv_filenames.append((csv_path, pq_path))
    return csv_filenames


def _run(args: argparse.Namespace) -> None:
    if args.aws_profile is not None:
        filesystem = hs3.get_s3fs(args.aws_profile)
    else:
        filesystem = None
        # TODO(gp): @danya use hparser.create_incremental_dir(args.dst_dir, args).
        hio.create_dir(args.dst_dir, args.incremental)
    # Find all CSV and Parquet files.
    files = _get_csv_to_pq_file_names(
        args.src_dir, args.dst_dir, args.incremental, s3fs_=filesystem
    )
    # Convert CSV files into Parquet files.
    for csv_full_path, pq_full_path in files:
        stream, kwargs = hs3.get_local_or_s3_stream(csv_full_path)
        if filesystem:
            kwargs["s3fs"] = filesystem
        df = hpandas.read_csv_to_df(stream, **kwargs)
        hparque.to_parquet(df, pq_full_path, aws_profile=filesystem)
    # Convert Parquet files into a different partitioning scheme.
    # TODO(gp): IMO this is a different / optional step.
    dataset = ds.dataset(
        args.dst_dir, format="parquet", partitioning="hive", filesystem=filesystem
    )
    # TODO(gp): Not sure this can be done since all the CSV files might not fit in
    #  memory.
    df = dataset.to_table().to_pandas()
    # Set datetime index.
    reindexed_df = imvcdttrut.reindex_on_datetime(df, args.datetime_col)
    # Add date partition columns to the dataframe.
    df, partition_cols = hparque.add_date_partition_columns(
        reindexed_df, "by_date"
    )
    hparque.to_partitioned_parquet(
        df, partition_cols, args.dst_dir, aws_profile=args.aws_profile
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
    parser.add_argument(
        "--aws_profile",
        action="store",
        type=str,
        default=None,
        help="The AWS profile to use for `.aws/credentials` or for env vars",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
