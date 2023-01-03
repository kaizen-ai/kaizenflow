#!/usr/bin/env python

# TODO(gp): Move to example/binance?

"""
Download historical data from Binance and save it as gzipped CSV locally.

Use as:
# Download OHLCV data for binance:
> download_bulk_manual_downloaded_1min_csv_ohlcv_spot_binance_v_1_0_0.py \
    --start_timestamp '2022-10-20 10:00:00-04:00' \
    --end_timestamp '2022-10-21 15:30:00-04:00' \
    --output_file test1.csv
"""
import argparse
import logging
import time
from typing import Generator, Tuple

import pandas as pd
import requests
import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)

_MAX_LINES = 1000
_OHLCV_HEADERS = ["symbol", "open_time", "open", "high", "low", "close", "volume"]


# TODO(gp): start_time -> start_timestamp_as_unix_epoch


def _build_url(
    start_time: int,
    end_time: int,
    symbol: str,
    *,
    interval: str = "1m",
    limit: int = 500,
) -> str:
    """
    Build up URL with the placeholders from the args.
    """
    _BASE_URL = "https://api.binance.com/api/v3/klines"
    return (
        f"{_BASE_URL}?startTime={start_time}&endTime={end_time}"
        f"&symbol={symbol}&interval={interval}&limit={limit}"
    )


def _process_symbol(symbol: str) -> str:
    """
    Transform symbol from universe to Binance format.
    """
    return symbol.replace("_", "")


def _split_period_to_days(
    start_time: int, end_time: int
) -> Generator[Tuple[int, int], None, None]:
    """
    Split period into chunks of the days.

    The reason is that Binance API don't allow to get more than 1500 rows at once.
    So if to get 1m interval, we need to chop a period into chunks that
    Binance allow us to get.

    :param start_time: timestamp as Unix epoch for the start time
    :param end_time: timestamp as Unix epoch for the end time
    :return: generator for loop
    """
    step = 1000 * 60 * _MAX_LINES
    for i in range(start_time, end_time, step):
        yield i, min(i + step, end_time)


def _add_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options for exchange download.
    """
    parser.add_argument(
        "--start_timestamp",
        required=True,
        action="store",
        type=str,
        help="Beginning of the downloaded period - example 2022-02-09 10:00:00-04:00",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded period - example 2022-02-10 10:00:00-04:00",
    )
    parser.add_argument(
        "--output_file",
        action="store",
        required=True,
        type=str,
        help="Absolute path to the output file",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = _add_download_args(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    hdateti.dassert_has_tz(start_timestamp)
    start_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
        start_timestamp
    )
    end_timestamp = pd.Timestamp(args.end_timestamp)
    hdateti.dassert_has_tz(end_timestamp)
    end_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(end_timestamp)
    hdbg.dassert_lt(
        start_timestamp_as_unix,
        end_timestamp_as_unix,
        msg="End timestamp should be greater then start timestamp.",
    )
    output = pd.DataFrame()
    _UNIVERSE = {
        "binance": [
            "ETH_USDT",
            "BTC_USDT",
            "SAND_USDT",
            "ETH_BUSD",
            "BTC_BUSD",
            "STORJ_USDT",
            "GMT_USDT",
            "AVAX_USDT",
            "BNB_USDT",
            "APE_USDT",
            "MATIC_USDT",
            "DYDX_USDT",
            "DOT_USDT",
            "UNFI_USDT",
            "LINK_USDT",
            "XRP_USDT",
            "CRV_USDT",
            "RUNE_USDT",
            "BAKE_USDT",
            "NEAR_USDT",
            "FTM_USDT",
            "WAVES_USDT",
            "AXS_USDT",
            "OGN_USDT",
            "DOGE_USDT",
            "SOL_USDT",
            "CTK_USDT",
        ]
    }
    _THROTTLE_DELAY_IN_SECS = 0.5
    for symbol in tqdm.tqdm(_UNIVERSE["binance"]):
        for start_time, end_time in _split_period_to_days(
            start_time=start_timestamp_as_unix, end_time=end_timestamp_as_unix
        ):
            url = _build_url(
                start_time=start_time,
                end_time=end_time,
                symbol=_process_symbol(symbol),
                limit=_MAX_LINES,
            )
            headers = {"Content-Type": "application/json"}
            response = requests.request(
                method="GET", url=url, headers=headers, data={}
            )
            hdbg.dassert_eq(response.status_code, 200)
            # Create a DataFrame with the results.
            data = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "open_time": hdateti.convert_unix_epoch_to_timestamp(
                            row[0]
                        ),
                        "open": row[1],
                        "high": row[2],
                        "low": row[3],
                        "close": row[4],
                        "volume": row[5],
                    }
                    for row in response.json()
                ]
            )
            output = pd.concat(objs=[output, data], ignore_index=True)
            time.sleep(_THROTTLE_DELAY_IN_SECS)
    # Save results.
    # TODO(gp): The user should add the gz and we could have a check that the
    #  suffix is ".csv.gz"
    output.to_csv(f"{args.output_file}.gz", index=False, compression="gzip")


if __name__ == "__main__":
    _main(_parse())
