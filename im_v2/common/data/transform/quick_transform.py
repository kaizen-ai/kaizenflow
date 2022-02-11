"""
Convert csv files to a PQ dataset by asset.

Use as:
> im_v2/common/data/transform/quick_transform.py
   --exchange binance

Import as:

import im_v2.common.data.transform.quick_transform as imvcdtqutr
"""
import argparse
import os

import pandas as pd

import helpers.hdbg as hdbg
import im_v2.common.data.transform.transform_utils as imvcdttrut


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--exchange",
        action="store",
        type=str,
        required=True,
        help="Exchange id",
    )
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    directory = "20220210_snapshot/historical/" + args.exchange
    # Read all .csv files for a single exchange.
    dfs = []
    for file in os.listdir(directory):
        df = pd.read_csv(os.path.join(directory, file))
        dfs.append(df)
    df = pd.concat(dfs)
    # Reindex and save as a PQ partitioned by year, month, date and asset.
    df = imvcdttrut.reindex_on_datetime(df, "timestamp")
    df, partition_cols = imvcdttrut.add_date_partition_cols(
        df, partition_mode="by_year_month_day"
    )
    df = df.rename({"currency_pair": "asset"}, axis=1)
    partition_cols.append("asset")
    imvcdttrut.partition_dataset(
        df, partition_cols, "20220210_snapshot_pq/historical/" + args.exchange
    )
