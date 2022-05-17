#!/usr/bin/env python
"""
Download historical data from Crypto-Chassis and save to S3. The script is meant to run
daily for reconciliation with realtime data.

Use as:

# Download data for Crypto-Chassis for binance from 2022-02-08:
> python im_v2/crypto_chassis/data/extract/download_historical_data.py \
     --start_timestamp '2022-02-08' \
     --exchange_id 'binance' \
     --universe 'v1' \
     --depth '1' \
     --aws_profile 'ck' \
     --s3_path 's3://cryptokaizen-data/daily_staged/'
"""
    
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hdatetime as hdateti
import helpers.hparquet as hparque
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.crypto_chassis.data.extract.extractor as imvccdee

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # TODO(Danya): This can be uncommented once we have specific 
    # `download_kwargs` set inside `extract_utils`.
    # parser.add_argument(
    #     "--depth",
    #     action="store",
    #     required=False,
    #     type=int,
    #     default=None,
    #     help="The depth of market data.",
    # )
    parser.add_argument(
        # TODO(Danya): This should be propagated to `add_exchange_download_args`.
        "--data_type",
        action="store",
        required=True,
        type=str,
        help="OHLCV, market_depth or trades data."
    )
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = hs3.add_s3_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _run(args: argparse.Namespace) -> None:
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    imvcdeexut.download_historical_data(args, imvccdee.CryptoChassisExtractor)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    _run(args)


if __name__ == "__main__":
    _main(_parse())
