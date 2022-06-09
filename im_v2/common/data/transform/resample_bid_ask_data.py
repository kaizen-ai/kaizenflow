#!/usr/bin/env python
import argparse
import logging
import os

import core.finance.resampling as cfinresa
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hparser as hparser

_LOG = logging.getLogger(__name__)


def _run(args: argparse.Namespace) -> None:
    currency_pair_dirs = os.listdir(args.src_dir)
    for currency_pair_dir in currency_pair_dirs:
        src_path = os.path.join(args.src_dir, currency_pair_dir)
        df = hparque.from_parquet(src_path, aws_profile="ck")
        df = cfinresa.resample(df, rule="T").agg(
            {
                "bid_price": "last",
                "bid_size": "sum",
                "ask_price": "last",
                "ask_size": "last",
                "full_symbol": "last",
            }
        )
        currency_pair = currency_pair_dir.split("=")[0]
        full_symbol = [f"{df['exchange_id']}::{currency_pair}"] * df.shape[0]
        df = df.insert(0, "full_symbol", full_symbol)
        df = df.drop(columns=["exchange_id"])
        partition_columns = ["year", "month"]
        dst_path = os.path.join(args.dst_dir, currency_pair_dir)
        hparque.to_partitioned_parquet(df, partition_columns, dst_path)
        _LOG.info("Resampled data was uploaded to %s", dst_path)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Dir with input parquet files to resample to 1 minute frequency",
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
