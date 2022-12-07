#!/usr/bin/env python
"""
Download historical data from Binance and save it as CSV locally.

Use as:
# Download OHLCV data for binance:
> im_v2/ccxt/data/extract/download_historical_data_ohlcv.py \
    --start_timestamp '2022-02-09 10:00:00' \
    --end_timestamp '2022-02-19' \
    --output_file test1.csv
"""
import argparse
import dataclasses
import csv
import logging
from typing import Tuple, Generator

import requests
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hdatetime as hdateti


_LOG = logging.getLogger(__name__)

BASE_URL = "https://api.binance.com/api/v3/klines"
DEFAULT_INTERVAL = "1m"
MAX_LINES = 1000
UNIVERSE = {
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
        "CTK_USDT"
    ]
}
OHLCV_HEADERS = [
    "symbol",
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume"
]

def _build_url(
        start_time: int,
        end_time: int,
        symbol: str,
        interval: str = DEFAULT_INTERVAL,
        limit: int = 500
) -> str:
    return (f"{BASE_URL}?startTime={start_time}&endTime={end_time}"
            f"&symbol={symbol}&interval={interval}&limit={limit}")


def add_download_args(
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
        help="Beginning of the downloaded period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded period",
    )
    parser.add_argument(
        "--output_file",
        action="store",
        required=True,
        type=str,
        help="Absolute path to the output file",
    )
    return parser


def _process_symbol(symbol: str):
    return symbol.replace('_', '')


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = add_download_args(parser)
    return parser


def _split_period_to_days(
        start_time: int,
        end_time: int
) -> Generator[Tuple[int, int], None, None]:
    step = 1000*60*MAX_LINES
    for i in range(start_time, end_time, step):
        yield i, min(i + step, end_time)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    headers = {
        'Content-Type': 'application/json'
    }
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    start_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
        start_timestamp
    )
    end_timestamp = pd.Timestamp(args.end_timestamp)
    end_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
        end_timestamp
    )
    with open(args.output_file, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=OHLCV_HEADERS)
        writer.writeheader()
        for symbol in UNIVERSE['binance']:
            for start_time, end_time in _split_period_to_days(
                start_time=start_timestamp_as_unix,
                end_time=end_timestamp_as_unix
            ):
                url = _build_url(
                    start_time=start_time,
                    end_time=end_time,
                    symbol=_process_symbol(symbol),
                    limit=MAX_LINES
                )
                response = requests.request(
                    method="GET",
                    url=url,
                    headers=headers,
                    data={}
                )
                hdbg.dassert_eq(response.status_code, 200)
                data = pd.DataFrame(
                    [
                        {
                            "symbol": symbol,
                            "open_time": row[0],
                            "open": row[1],
                            "high": row[2],
                            "low": row[3],
                            "close": row[4],
                            "volume": row[5]
                        }
                        for row in response.json()
                    ]
                )
                data.to_csv(args.output_file, index=False)


if __name__ == "__main__":
    _main(_parse())