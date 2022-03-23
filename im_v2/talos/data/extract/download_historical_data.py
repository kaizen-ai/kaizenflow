#!/usr/bin/env python
"""
Download historical data from Talos and save to S3. The script is meant to run
daily for reconciliation with realtime data.

Use as:

# Download data for Talos for binance from 2022-02-08 to 2022-02-09:
> im_v2/talos/data/extract/download_historical_data.py \
     --end_timestamp '2022-02-09' \
     --start_timestamp '2022-02-08' \
     --exchange_id 'binance' \
     --universe 'v01' \
     --aws_profile 'ck' \
     --s3_path 's3://cryptokaizen-data/daily_staged/'
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.talos.data.extract.exchange_class as imvtdeexcl

_LOG = logging.getLogger(__name__)


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
        choices=["sandbox", "prod"],
        type=str,
        help="(Optional) API 'stage' to use ('sandbox' or 'prod'), default: 'sandbox'",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = hs3.add_s3_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    imvcdeexut.download_historical_data(args, imvtdeexcl.TalosExchange)


if __name__ == "__main__":
    _main(_parse())
