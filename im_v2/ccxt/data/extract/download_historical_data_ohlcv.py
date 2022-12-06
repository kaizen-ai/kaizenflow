#!/usr/bin/env python
"""
Download historical data from Binance and save it as CSV locally.

Use as:
# Download OHLCV data for binance:
> im_v2/ccxt/data/extract/download_historical_data_ohlcv.py \
    --start_timestamp 1670350563000 \
    --end_timestamp 1670352003000 \
    --output_file test1.csv
"""
import argparse
import dataclasses
import logging
from typing import Tuple, Generator

import requests
import csv


_LOG = logging.getLogger(__name__)
BASE_URL = "https://api.binance.com/api/v3/klines"
DEFAULT_INTERVAL = "1m"
MAX_LINES = 1000
UNIVERSE = {
    "CCXT": {
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
}


@dataclasses.dataclass
class OHLCVRow:
    symbol: str
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __init__(self, symbol: str, input_list: list):
        self.symbol = symbol
        self.open_time = input_list[0]
        self.open = input_list[1]
        self.high = input_list[2]
        self.low = input_list[3]
        self.close = input_list[4]
        self.volume = input_list[5]



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
        type=int,
        help="Beginning of the downloaded period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=int,
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
    with open(args.output_file, 'w') as output_file:
        csv_headers = list(OHLCVRow.__annotations__)
        writer = csv.DictWriter(output_file, fieldnames=csv_headers)
        writer.writeheader()
        for symbol in UNIVERSE['CCXT']['binance']:
            for start_time, end_time in _split_period_to_days(
                start_time=args.start_timestamp,
                end_time=args.end_timestamp
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
                assert response.status_code == 200
                writer.writerows(
                    [
                        dataclasses.asdict(
                            OHLCVRow(symbol=symbol, input_list=row)
                        )
                        for row in response.json()
                    ]
                )


if __name__ == "__main__":
    _main(_parse())
