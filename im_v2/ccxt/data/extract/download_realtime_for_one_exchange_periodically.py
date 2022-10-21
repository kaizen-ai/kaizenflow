#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT periodically.

Use as:
> im_v2/ccxt/data/extract/download_realtime_for_one_exchange_periodically.py \
    --exchange_id 'binance' \
    --universe 'v3' \
    --db_stage 'dev' \
    --db_table 'ccxt_ohlcv_test' \
    --aws_profile 'ck' \
    --s3_path 's3://cryptokaizen-data-test/realtime/' \
    --data_type 'ohlcv' \
    --contract_type 'spot' \
    --interval_min '1' \
    --start_time '2022-05-16 00:45:00' \
    --stop_time '2022-05-16 00:55:00'
"""

import argparse

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.extractor as imvcdeex
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.db.db_utils as imvcddbut


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = imvcdeexut.add_periodical_download_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Initialize the CCXT Extractor class.
    exchange = imvcdeex.CcxtExtractor(args.exchange_id, args.contract_type)
    args = vars(args)
    imvcdeexut.download_realtime_for_one_exchange_periodically(
        args, exchange
    )


if __name__ == "__main__":
    _main(_parse())