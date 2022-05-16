#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT periodically.
TODO(timurg): Move this to im_v2/common

Use as:
> im_v2/ccxt/data/extract/download_realtime_for_one_exchange_periodically.py \
    --exchange_id 'binance' \
    --universe 'v3' \
    --db_stage 'dev' \
    --db_table 'ccxt_ohlcv_test' \
    --aws_profile 'ck' \
    --s3_path 's3://cryptokaizen-data-test/realtime/' \
    --interval_min '1' \
    --start_time '2022-05-16 00:45:00' \
    --stop_time '2022-05-16 00:55:00'
"""

import argparse
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.db.db_utils as imvcddbut


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start_time",
        action="store",
        required=True,
        type=str,
        help="Timestamp when the download should start (e.g., '2022-05-03 00:40:00')",
    )
    parser.add_argument(
        "--stop_time",
        action="store",
        required=True,
        type=str,
        help="Timestamp when the script should stop (e.g., '2022-05-03 00:30:00')",
    )
    parser.add_argument(
        "--interval_min",
        type=int,
        help="Interval between download attempts, in minutes",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of exchange to download data from (e.g., 'binance')",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trading universe to download data for",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    imvcdeexut.download_realtime_for_one_exchange_periodically(args, imvcdeexcl.CcxtExchange)


if __name__ == "__main__":
    _main(_parse())
