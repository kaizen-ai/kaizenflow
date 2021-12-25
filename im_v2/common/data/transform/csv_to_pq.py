#!/usr/bin/env python
"""
Convert data from csv to PQ files and partition dataset by asset.

# A parquet file partitioned by assets:

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
> im_v2/common/data/transform/csv_to_pq.py \
    --src-dir test/ccxt_test \
    --dst-dir test_pq

Import as:

import im_v2.common.data.transform.csv_to_pq as imvcdtctpq
"""

import argparse
import logging
import os
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import helpers.csv_helpers as hcsv
import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.system_interaction as hsysinte
# import im_v2.common.data.transform.transform as imvcdtrtr

_LOG = logging.getLogger(__name__)

# transform = imvcdtrtr.ImTransform()


# TODO(Nikola): Remove in favor of ImTransform class.
def convert_timestamp_column(datetime_col: pd.Series) -> pd.Series:
    """
    Convert datetime as string or int into a timestamp.

    :param datetime_col: Series containing datetime as str or int
    :return: Series containing datetime as `pd.Timestamp`
    """
    # Convert unix epoch into Timestamp.
    if pd.api.types.is_integer_dtype(datetime_col):
        converted_datetime_col = datetime_col.apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
    # Convert string into timestamp.
    elif pd.api.types.is_string_dtype(datetime_col):
        converted_datetime_col = hdateti.to_generalized_datetime(datetime_col)
    else:
        raise ValueError(
            "Incorrect data format. Datetime column should be of integer or string dtype."
        )
    return converted_datetime_col


def _get_csv_to_pq_file_names(
    src_dir: str, dst_dir: str, incremental: bool
) -> List[Tuple[str, str]]:
    """
    Find all the CSV files in `src_dir` to transform and the corresponding
    destination PQ files.

    :param incremental: if True, skip CSV files for which the corresponding PQ file already exists
    :return: list of tuples (csv_file, pq_file)
    """
    hdbg.dassert_ne(len(os.listdir(src_dir)), 0, "No files inside '%s'", src_dir)
    # Find all the CSV files to convert.
    csv_ext, csv_gz_ext = ".csv", ".csv.gz"
    csv_files = []
    for f in os.listdir(src_dir):
        if f.endswith(csv_ext):
            filename = f[: -len(csv_ext)]
        elif f.endswith(csv_gz_ext):
            filename = f[: -len(csv_gz_ext)]
        else:
            _LOG.warning(f"Encountered non CSV file '{f}'")
        pq_path = os.path.join(dst_dir, f"{filename}.parquet")
        # Skip CSV files that do not need to be converted.
        if incremental and os.path.exists(pq_path):
            _LOG.warning(
                "Skipping the conversion of CSV file '%s' since '%s' already exists",
                filename,
                filename,
            )
        else:
            csv_path = os.path.join(src_dir, f)
            csv_files.append((csv_path, pq_path))
    return csv_files


# TODO(Nikola): Remove in favor of ImTransform class.
def _partition_dataset(
    dataset_path: str,
    datetime_col_name: str,
    asset_col_name: str,
    delete_original: bool = True,
) -> None:
    """
    Partition unorganized .parquet files by asset.

    :param dataset_path: location of .parquet files to partition
    :param datetime_col_name: name of datetime column to create index
    :param asset_col_name: name of asset column to partition by
    :param delete_original: whether to delete original .parquet files
    """
    # Read files.
    dataset = ds.dataset(dataset_path, format="parquet", partitioning="hive")
    data = dataset.to_table().to_pandas()
    # Set datetime index.
    datetime_col = data[datetime_col_name]
    indexed_data = data.set_index(convert_timestamp_column(datetime_col))
    # Create date partition columns.
    indexed_data["year"] = indexed_data.index.year
    indexed_data["month"] = indexed_data.index.month
    indexed_data["day"] = indexed_data.index.day
    partition_cols = [asset_col_name, "year", "month", "day"]
    # Save partitioned parquet dataset.
    table = pa.Table.from_pandas(indexed_data)
    pq.write_to_dataset(table, dataset_path, partition_cols=partition_cols)
    # if delete_original:
    # Delete original files.
    hsysinte.system("rm %s/*.parquet" % dataset_path)


# TODO(Danya): Add `by` argument and allow partitioning by date.
def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Location of input CSV to convert to PQ format",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination dir where to save converted PQ files",
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
    # List all original CSV files.
    hio.create_dir(args.dst_dir, args.incremental)
    files = _get_csv_to_pq_file_names(
        args.src_dir, args.dst_dir, args.incremental
    )
    # Transform CSV files.
    for csv_full_path, pq_full_path in files:
        hcsv.convert_csv_to_pq(csv_full_path, pq_full_path)
    # TODO(Nikola): Enable.
    # # Read files.
    # dataset = ds.dataset(args.dst_dir, format="parquet", partitioning="hive")
    # df = dataset.to_table().to_pandas()
    # # Set datetime index.
    # reindexed_df = transform.reindex_on_datetime(df, args.datetime_col)
    # # Add date partition columns to the dataframe.
    # transform.add_date_partition_cols(reindexed_df, "day")
    # # Save partitioned parquet dataset.
    # partition_cols = [args.asset_col, "year", "month", "day"]
    # transform.partition_dataset(reindexed_df, partition_cols, args.dst_dir)
    # TODO(Nikola): Remove in favor of ImTransform class.
    _partition_dataset(
        dataset_path=args.dst_dir,
        datetime_col_name=args.datetime_col,
        asset_col_name=args.asset_col,
    )


if __name__ == "__main__":
    _main(_parse())
