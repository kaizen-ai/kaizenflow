#!/usr/bin/env python
"""
Download data from Crypto-Chassis/CCXT and save to S3 in a Parquet/CSV format.
The script is meant to run daily for in collaboration with realtime data QA or
downloading bulk data snapshots.

Use as:

# Download OHLCV futures data using CCXT from binance:
> im_v2/common/data/extract/download_bulk.py \
    --download_mode 'bulk' \
    --downloading_entity 'manual' \
    --action_tag 'downloaded_1min' \
    --vendor 'ccxt' \
    --start_timestamp '2022-10-18 12:15:00+00:00' \
    --end_timestamp '2022-10-18 12:30:00+00:00' \
    --exchange_id 'binance' \
    --universe 'v3' \
    --aws_profile 'ck' \
    --data_type 'ohlcv' \
    --data_format 'parquet' \
    --contract_type 'futures' \
    --s3_path 's3://cryptokaizen-data-test/'

Import as:

import im_v2.common.data.extract.download_bulk as imvcdexdb
"""
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.binance.data.extract.extractor as imvbdexex
import im_v2.ccxt.data.extract.extractor as imvcdexex
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.data.qa.validate_input_args as imvcdqvia
import im_v2.crypto_chassis.data.extract.extractor as imvccdexex

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = hs3.add_s3_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    parser.add_argument(
        "--universe_part",
        action="store",
        required=False,
        type=int,
        help="Only applicable if vendor = 'crypto_chassis'. The universe is split into \
            groups of 10 pairs. Denote which part should be downloaded \
            (e.g. 1 - first 10 symbols)",
    )
    parser.add_argument(
        "--assert_on_missing_data",
        required=False,
        default=False,
        action="store_true",
        help="Raise an Exception if no data is downloaded for "
        "one or more symbols in the universe",
    )
    return parser  # type: ignore[no-any-return]


def _run(args: argparse.Namespace) -> None:
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    args = vars(args)
    vendor = args["vendor"]
    args["unit"] = imvcdttrut.get_vendor_epoch_unit(vendor, args["data_type"])
    exchange = imvcdqvia.validate_vendor_arg(vendor=vendor, args=args)
    imvcdeexut.download_historical_data(args, exchange)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    _run(args)


if __name__ == "__main__":
    _main(_parse())
