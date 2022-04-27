#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT periodically.
TODO(timurg): Move this to im_v2/common

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
    # Time range for each download
    time_window = 5
    #
    run_for_min = args.run_for_min
    interval_min = args.interval_min
    # Check values.
    for value in run_for_min, interval_min:
        hdbg.dassert_lte(1, value, msg="Value: '{value}' should be greater then 0")
    # TODO(timurg): Align the use of "5" via a constant or argument throughout the scripts.
    # Error will be raised if we miss full 5 minute window of data,
    # even if the next download succeeds, we don't recover all of the previous data.
    failures_limit = time_window // interval_min + time_window % interval_min
    consecutive_failures_left = failures_limit
    # Delay start
    iteration_start_time = datetime.now()
    iteration_delay_sec = 0.0
    if iteration_start_time.second != 0:
        iteration_start_time = iteration_start_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
        iteration_delay_sec = (iteration_start_time - datetime.now()).total_seconds()
        _LOG.info("Delay: %s seconds before first download", iteration_delay_sec)
    # Save time limit.
    script_stop_time = iteration_start_time + timedelta(minutes=run_for_min)
    while datetime.now() < script_stop_time:
        # Wait until next download.
        time.sleep(iteration_delay_sec)
        # TODO(timurg): investigate why end_timestamp ignored, and returned data up to now.
        args.start_timestamp = iteration_start_time - timedelta(minutes=time_window)
        args.end_timestamp = datetime.now()
        _LOG.info("Starting download data from: %s, till: %s", args.start_timestamp, args.end_timestamp)
        try:
            imvcdeexut.download_realtime_for_one_exchange(args, imvcdeexcl.CcxtExchange)
            # Reset failures counter.
            consecutive_failures_left = failures_limit
        except Exception as e:
            consecutive_failures_left -= 1
            _LOG.error("Download failed: %s, %s failures left", str(e), consecutive_failures_left)
            # Download failed.
            if not consecutive_failures_left:
                raise RuntimeError(f"{failures_limit} consecutive downloads were failed") from e
        # if Download took more then expected.
        if datetime.now() > iteration_start_time + timedelta(minutes=interval_min):
            _LOG.error("The download was not finished in %s minutes.", interval_min)
            iteration_delay_sec = 0
            # Download that will start after repeated one, should follow to the initial schedule.
            while datetime.now() > iteration_start_time + timedelta(minutes=interval_min):
                iteration_start_time = iteration_start_time + timedelta(minutes=interval_min)
        # If download failed, but there are time before next download.
        elif consecutive_failures_left < failures_limit:
            _LOG.info("Start repeat download immediately.")
            iteration_delay_sec = 0
        else:
            download_duration_sec = (datetime.now() - iteration_start_time).total_seconds()
            # Calculate delay before next download.
            iteration_delay_sec = (iteration_start_time + timedelta(minutes=interval_min) - datetime.now()).total_seconds()
            # Add interval in order to get next download time.
            iteration_start_time = iteration_start_time + timedelta(minutes=interval_min)
            _LOG.info("Succesfull download, took %s sec, delay %s sec until next iteration", download_duration_sec, iteration_delay_sec)


if __name__ == "__main__":
    _main(_parse())
