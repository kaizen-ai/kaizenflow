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
import logging
import time
from datetime import datetime, timedelta

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
    parser.add_argument(
        "--run_for_min",
        type=int,
        help="Total running time, in minutes",
    )
    parser.add_argument(
        "--interval_min",
        type=int,
        help="Interval between download attempts, in minutes",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcdeexut.add_exchange_download_args(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    #
    run_for_min = args.run_for_min
    interval_min = args.interval_min
    # Check values.
    for value in run_for_min, interval_min:
        hdbg.dassert_lte(
            1, value, msg="Value: '{value}' should be greater then 0"
        )
    # TODO(timurg): Align the use of "5" via a constant or argument throughout the scripts.
    # Error will be raised if we miss full 5 minute window of data,
    # even if the next download succeeds, we don't recover all of the previous data.
    failures_limit = 5 // interval_min + 5 % interval_min
    concurrent_failures_left = failures_limit
    # Delay start in order to align to the minutes grid of the realtime clock.
    next_start_time = datetime.now()
    run_delay_sec = 0.0
    if next_start_time.second != 0:
        next_start_time = next_start_time.replace(
            second=0, microsecond=0
        ) + timedelta(minutes=1)
        run_delay_sec = (next_start_time - datetime.now()).total_seconds()
    # Save time limit.
    script_stop_time = next_start_time + timedelta(minutes=run_for_min)
    #
    continue_running = True
    while continue_running:
        # Wait until next run.
        time.sleep(run_delay_sec)
        # Add interval in order to get next download time.
        next_start_time = next_start_time + timedelta(minutes=interval_min)
        try:
            _LOG.debug("Starting next download")
            imvcdeexut.download_realtime_for_one_exchange(
                args,
                imvcdeexcl.CcxtExchange,
            )
            # Reset failures counter.
            concurrent_failures_left = failures_limit
        except Exception as e:
            concurrent_failures_left -= 1
            # Download failed.
            if not concurrent_failures_left:
                raise RuntimeError(
                    f"{failures_limit} concurrent downloads were failed"
                ) from e
            _LOG.error(str(e))
        # if Download took more then expected.
        if datetime.now() > next_start_time:
            raise RuntimeError(
                f"The download was not finished in {interval_min} minutes."
            )
        # Calculate delay before next download.
        run_delay_sec = (next_start_time - datetime.now()).total_seconds()
        # Check if there is a time for the next iteration.
        continue_running = True if datetime.now() < script_stop_time else False


if __name__ == "__main__":
    _main(_parse())
