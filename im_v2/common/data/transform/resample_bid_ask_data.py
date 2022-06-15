#!/usr/bin/env python
"""
Load bid/ask parquet data from S3 exchange dir, resample to 1 minute and upload
back.

# Usage sample:
> im_v2/common/data/transform/resample_bid_ask_data.py \
    --src_dir 's3://<ck-data>/bid_ask/crypto_chassis/ftx' \
    --dst_dir 's3://<ck-data>/resampled_bid_ask/ftx' \

Import as:

import im_v2.common.data.transform.resample_bid_ask_data as imvcdtrbad
"""
import argparse
import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm

import core.finance.resampling as cfinresa
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3

_LOG = logging.getLogger(__name__)


def _resample_bid_ask_data(
    data: pd.DataFrame, mode: str = "VWAP"
) -> pd.DataFrame:
    """
    Resample bid/ask data to 1 minute interval.

    :param mode: designate strategy to use, i.e. volume-weighted average
        (VWAP) or time-weighted average price (TWAP)
    """
    resample_rule = "T"
    df = cfinresa.resample(data, rule=resample_rule).agg(
        {
            "bid_price": "last",
            "bid_size": "sum",
            "ask_price": "last",
            "ask_size": "sum",
            "exchange_id": "last",
        }
    )
    if mode == "VWAP":
        bid_price = cfinresa.compute_vwap(
            df, rule=resample_rule, price_col="bid_price", volume_col="bid_size"
        )
        ask_price = cfinresa.compute_vwap(
            df, rule=resample_rule, price_col="ask_price", volume_col="ask_size"
        )
        bid_ask_price_df = pd.DataFrame(
            [bid_price, ask_price], index=["bid_size", "ask_size"]
        ).T
    elif mode == "TWAP":
        bid_ask_price_df = (
            df[["bid_size", "ask_size"]]
            .groupby(pd.Grouper(freq=resample_rule))
            .mean()
        )
    else:
        raise ValueError(f"Invalid mode='{mode}'")
    df["bid_price"] = bid_ask_price_df["bid_size"]
    df["ask_price"] = bid_ask_price_df["ask_size"]
    return df


def _run(args: argparse.Namespace) -> None:
    pattern = "*"
    only_files = True
    use_relative_paths = True
    aws_profile = "ck"
    # Get all files in the root dir.
    files_to_read = hs3.listdir(
        args.src_dir,
        pattern,
        only_files,
        use_relative_paths,
        aws_profile=aws_profile,
    )
    filesystem = hs3.get_s3fs(aws_profile)
    columns = [
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
        "exchange_id",
    ]
    for file in tqdm.tqdm(files_to_read):
        file_path = os.path.join(args.src_dir, file)
        df = hparque.from_parquet(
            file_path, columns=columns, aws_profile=aws_profile
        )
        df = _resample_bid_ask_data(df)
        dst_path = os.path.join(args.dst_dir, file)
        pq.write_table(
            pa.Table.from_pandas(df),
            dst_path,
            filesystem=filesystem,
        )
        _LOG.info("Resampled data was uploaded to %s", args.dst_dir)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Path to exchange dir with input parquet files to resample to 1 minute frequency",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination dir where to save resampled parquet files",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _run(args)


if __name__ == "__main__":
    _main(_parse())
