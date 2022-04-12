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
    --s3_path 's3://cryptokaizen-data-test/realtime/'\
    --run_for_sec '600'\
    --interval_sec '30'\
"""

import argparse
import concurrent.futures
import logging
import time

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.common.data.extract.extract_utils as imvcdeexut
import im_v2.common.db.db_utils as imvcddbut

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--run_for_sec",
                        type=int,
                        default=0,
    )
    parser.add_argument("--interval_sec",
                        type=int,
                        default=0,
                        help="interval between downloads, sec",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    run_for = args.run_for_sec
    interval = args.interval_sec
    # Execute downloading with the given intervals
    start_time = time.perf_counter()
    continue_running = True
    while continue_running:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            # Start downloading thread
            executor = executor.submit(
                imvcdeexut.download_realtime_for_one_exchange,
                args,
                imvcdeexcl.CcxtExchange
            )
            if interval:
                # Wait until next run
                time.sleep(interval)
                if not executor.done():
                    raise RuntimeError("Couldn't finish in time")
            # Check running time if running time argument was given
            running_time = time.perf_counter() - start_time
            continue_running = running_time < run_for if run_for else False


if __name__ == "__main__":
    _main(_parse())
