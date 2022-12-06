#!/usr/bin/env python
"""
Download historical data from CCXT and save to S3. The script is meant to run
daily for reconciliation with realtime data.

Use as:

# Download data for CCXT for binance from 2022-02-08 to 2022-02-09:
> im_v2/ccxt/data/extract/download_historical_data.py \
     --end_timestamp '2022-02-09' \
     --start_timestamp '2022-02-08' \
     --exchange_id 'binance' \
     --data_type 'ohlcv' \
     --contract_type 'spot' \
     --universe 'v3' \
     --aws_profile 'ck' \
     --s3_path 's3://<ck-data>/historical/'
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.extractor as ivcdexex
import im_v2.common.data.extract.extract_utils as imvcdeexut

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = hs3.add_s3_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Initialize the CCXT Extractor class.
    # exchange = ivcdexex.CcxtExtractor(args.exchange_id, args.contract_type)
    # Assign extractor-specific variables.
    # args = vars(args)
    # args["unit"] = "ms"
    # imvcdeexut.download_historical_data(args, exchange)


if __name__ == "__main__":
    _main(_parse())