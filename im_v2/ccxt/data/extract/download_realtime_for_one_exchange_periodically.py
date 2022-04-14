#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT periodically.

Use as:

# Download OHLCV data for binance 'v3', saving dev_stage:
> im_v2/ccxt/data/extract/download_realtime_for_one_exchange_periodically.py \
    --start_timestamp '20211110-101100' \
    --end_timestamp '20211110-101200' \
    --exchange_id 'binance' \
    --universe 'v3' \
    --db_stage 'dev' \
    --db_table 'ccxt_ohlcv_test' \
    --aws_profile 'ck' \
    --s3_path 's3://cryptokaizen-data-test/realtime/'\
    --run_for_min '10'\
    --interval_min '1'
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

from datetime import datetime


_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--run_for_min",
                        type=int,
                        default=None,
                        help="Total running time, in minutes",
    )
    parser.add_argument("--interval_min",
                        type=int,
                        default=None,
                        help="Interval between download attempts, in minutes",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Error will be raised if we miss full 5 minute window of data,
    # even if the next download succeeds, we dont recover all of the previous data.
    failures_limit = 5
    concurrent_failures_left = failures_limit
    #
    sec_in_minute = 60
    run_for_sec = args.run_for_min * sec_in_minute
    interval_sec = args.interval_min * sec_in_minute
    # Delay start in order to align to the minutes grid of the realtime clock.
    time.sleep(sec_in_minute - datetime.now().second) if datetime.now().minute != 0 else None
    script_start_time = time.perf_counter()
    #
    continue_running = True
    while continue_running:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            # Start downloading thread.
            executor = executor.submit(
                imvcdeexut.download_realtime_for_one_exchange,
                args,
                imvcdeexcl.CcxtExchange
            )
            # Wait until next run.
            time.sleep(interval_sec)
            # Check if download finished in time.
            if not executor.done():
                raise RuntimeError(f"The download was not finished in {interval_sec} seconds.")
            # Check if there were no errors during download.
            try:
                executor.result()
                # Reset failures counter.
                concurrent_failures_left = failures_limit
            except Exception as exc:
                concurrent_failures_left -= 1
                # Download failed.
                if not concurrent_failures_left:
                    raise RuntimeError(f"More then {failures_limit} concurrent downloads failed")
            # Check running time if running time argument was given.
            running_time = time.perf_counter() - script_start_time
            continue_running = running_time < run_for_sec if run_for_sec else False


if __name__ == "__main__":
    _main(_parse())
