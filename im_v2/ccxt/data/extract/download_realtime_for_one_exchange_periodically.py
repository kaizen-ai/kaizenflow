#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT periodically.
TODO(timurg): Move this to im_v2/common

Use as:
> im_v2/ccxt/data/extract/download_realtime_for_one_exchange_periodically.py \
    --exchange_id 'binance' \
    --universe 'v3' \
    --db_stage 'dev' \
    --db_table 'ccxt_ohlcv_test' \
    --aws_profile 'ck' \
    --s3_path 's3://cryptokaizen-data-test/realtime/' \
    --interval_min '1' \
    --trigger_time '2022-04-28 14:22:51' \
    --stop_time '2022-04-28 17:23:51'
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
    parser.add_argument(
        "--trigger_time",
        action="store",
        default=str(
            datetime.now().replace(second=0, microsecond=0) + timedelta(minutes=1)
        ),
        type=str,
        help="When downloads should start",
    )
    parser.add_argument(
        "--stop_time",
        action="store",
        default=str(
            datetime.now().replace(second=0, microsecond=0)
            + timedelta(minutes=15)
        ),
        type=str,
        help="When script should stop",
    )
    parser.add_argument(
        "--interval_min",
        type=int,
        help="Interval between download attempts, in minutes",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of exchange to download data from",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe to download data for",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = imvcddbut.add_db_args(parser)
    parser = hs3.add_s3_args(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # Time range for each download
    time_window = 5
    # Checking values.
    stop_time = datetime.strptime(args.stop_time, "%Y-%m-%d %H:%M:%S")
    trigger_time = datetime.strptime(args.trigger_time, "%Y-%m-%d %H:%M:%S")
    interval_min = args.interval_min
    hdbg.dassert_lte(
        1, interval_min, "interval_min: %s should be greater then 0", interval_min
    )
    hdbg.dassert_lt(
        datetime.now(),
        trigger_time,
        "trigger_time: %s should be greater then current time: %s",
        trigger_time,
        datetime.now(),
    )
    hdbg.dassert_lt(
        trigger_time,
        stop_time,
        "stop_time: %s should be greater then trigger_time: %s",
        stop_time,
        trigger_time,
    )
    # Error will be raised if we miss full 5 minute window of data,
    # even if the next download succeeds, we don't recover all of the previous data.
    failures_limit = time_window // interval_min + time_window % interval_min
    consecutive_failures_left = failures_limit
    # Delay start
    iteration_start_time = trigger_time
    iteration_delay_sec = (iteration_start_time - datetime.now()).total_seconds()
    while datetime.now() < stop_time and consecutive_failures_left:
        # Wait until next download.
        _LOG.info("Delay %s sec until next iteration", iteration_delay_sec)
        time.sleep(iteration_delay_sec)
        # TODO(timurg): investigate why end_timestamp ignored, and returned data up to now.
        args.start_timestamp = iteration_start_time - timedelta(
            minutes=time_window
        )
        args.end_timestamp = datetime.now()
        _LOG.info(
            "Starting download data from: %s, till: %s",
            args.start_timestamp,
            args.end_timestamp,
        )
        try:
            imvcdeexut.download_realtime_for_one_exchange(
                args, imvcdeexcl.CcxtExchange
            )
            # Reset failures counter.
            consecutive_failures_left = failures_limit
        except (KeyboardInterrupt, Exception) as e:
            consecutive_failures_left -= 1
            _LOG.error(
                "Download failed: %s, %s failures left",
                str(e),
                consecutive_failures_left,
            )
            # Download failed.
            if not consecutive_failures_left:
                raise RuntimeError(
                    f"{failures_limit} consecutive downloads were failed"
                ) from e
        # if Download took more then expected.
        if datetime.now() > iteration_start_time + timedelta(
            minutes=interval_min
        ):
            _LOG.error(
                "The download was not finished in %s minutes.", interval_min
            )
            iteration_delay_sec = 0
            # Download that will start after repeated one, should follow to the initial schedule.
            while datetime.now() > iteration_start_time + timedelta(
                minutes=interval_min
            ):
                iteration_start_time = iteration_start_time + timedelta(
                    minutes=interval_min
                )
        # If download failed, but there are time before next download.
        elif consecutive_failures_left < failures_limit:
            _LOG.info("Start repeat download immediately.")
            iteration_delay_sec = 0
        else:
            download_duration_sec = (
                datetime.now() - iteration_start_time
            ).total_seconds()
            # Calculate delay before next download.
            iteration_delay_sec = (
                iteration_start_time
                + timedelta(minutes=interval_min)
                - datetime.now()
            ).total_seconds()
            # Add interval in order to get next download time.
            iteration_start_time = iteration_start_time + timedelta(
                minutes=interval_min
            )
            _LOG.info(
                "Successfully completed, iteration took %s sec",
                download_duration_sec,
            )


if __name__ == "__main__":
    _main(_parse())
