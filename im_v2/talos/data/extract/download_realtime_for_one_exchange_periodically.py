#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from Talos periodically.

Use as:
> im_v2/talos/data/extract/download_realtime_for_one_exchange_periodically.py \
    --exchange_id 'binance' \
    --universe 'v1' \
    --db_stage 'dev' \
    --db_table 'talos_ohlcv_test' \
    --aws_profile 'ck' \
    --s3_path 's3://cryptokaizen-data-test/realtime/' \
    --interval_min '1' \
    --start_time '2022-05-17T10:30:00.000000Z' \
    --stop_time '2022-05-17T10:35:00.000000Z'
"""

import argparse

import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.db.db_utils as imvcddbut
import im_v2.talos.data.extract.exchange_class as imvtdeexcl


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--api_stage",
        action="store",
        required=False,
        default="sandbox",
        type=str,
        help="(Optional) API 'stage' to use ('sandbox' or 'prod'), default: 'sandbox'",
    )
    parser = imvcdeexut.add_periodical_download_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    imvcdeexut.download_realtime_for_one_exchange_periodically(
        args, imvtdeexcl.TalosExchange
    )


if __name__ == "__main__":
    _main(_parse())
