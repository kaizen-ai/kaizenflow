#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT.

Use as:

# Download OHLCV data for binance 'v3', saving dev_stage:
> im_v2/ccxt/data/extract/download_realtime_for_one_exchange.py \
    --start_timestamp '20211110-101100' \
    --end_timestamp '20211110-101200' \
    --exchange_id 'binance' \
    --universe 'v3' \
    --db_stage 'dev' \
    --db_table 'ccxt_ohlcv_test' \
    --aws_profile 'ck' \
    --s3_path 's3://cryptokaizen-data-test/realtime/' \
    --data_type 'ohlcv' \
    --contract_type 'spot' \
    --secret_id '***REMOVED***'
"""

import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsecrets as hsecret
import im_v2.ccxt.data.extract.extractor as ivcdexex
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.db.db_utils as imvcddbut


_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Build `SecretIdentifier` instance. 
    secret_id = hsecret.parse_secret_id(args.secret_id)
    # Initialize the CCXT Extractor class.
    exchange = ivcdexex.CcxtExtractor(args.exchange_id, args.contract_type, secret_id)
    args = vars(args)
    imvcdeexut.download_realtime_for_one_exchange(args, exchange)


if __name__ == "__main__":
    _main(_parse())