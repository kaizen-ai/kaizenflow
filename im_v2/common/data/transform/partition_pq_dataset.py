#!/usr/bin/env python

"""
# Partition parquet dataset by date or by asset.

# Parquet dataset partitioned by date:
```
src_dir/
    date=20211211/
        data.parquet
    date=20211212/
        data.parquet
```

# A parquet file partitioned by assets:

```
dst_dir/
    year=2021/
        month=12/
                asset=BTC_USDT/
                    data.parquet
                asset=ETH_USDT/
                    data.parquet
```

# Use as:

> im_v2/common/data/transform/partition_pq_dataset.py \
    --src_dir ccxt_raw_pq_example/ \
    --dst_dir ccxt_realtime_partitioned_pq/ \
    --by asset \
    --datetime_col timestamp \
    --asset_col currency_pair
"""

import argparse
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser

_LOG = logging.getLogger(__name__)


# TODO(*): Move as a general transform utility function.
def convert_timestamp_column(datetime_col: pd.Series) -> pd.Series:
    """
    :param datetime_col:
    :return:
    """
    if pd.api.types.is_integer_dtype(datetime_col):
        converted_datetime_col = datetime_col.apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
    elif pd.api.types.is_string_dtype(datetime_col):
        converted_datetime_col = hdateti.to_generalized_datetime(datetime_col)
    else:
        raise ValueError(
            "Incorrect data format. Datetime column should be of integer or string type."
        )
    return converted_datetime_col


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Source directory with unpartitioned .parquet files",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Location to place partitioned parquet dataset",
    )
    parser.add_argument(
        "--by",
        action="store",
        type=str,
        required=True,
        help="Partition dataset by date or by asset",
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
    parser.add_argument("--incremental", action="store_true")
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # Read files.
    dataset = ds.dataset(args.src_dir, format="parquet", partitioning="hive")
    data = dataset.to_table().to_pandas()
    # Set datetime index.
    datetime_col = data[args.datetime_col]
    data = data.set_index(convert_timestamp_column(datetime_col))
    if args.by == "date":
        data["date"] = data.index.strftime("%Y%m%d")
        partition_cols = ["date"]
    elif args.by == "asset":
        hdbg.dassert(
            args.asset_col, msg="Please probide the name of asset column."
        )
        data["year"] = data.index.year
        data["month"] = data.index.month
        partition_cols = ["year", "month", args.asset_col]
    else:
        raise ValueError(f"Partition by {args.by} is not supported.")
    # Save partitioned parquet dataset.
    table = pa.Table.from_pandas(data)
    pq.write_to_dataset(table, args.dst_dir, partition_cols=partition_cols)


if __name__ == "__main__":
    _main(_parse())
