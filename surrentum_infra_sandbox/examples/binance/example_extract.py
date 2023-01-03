#!/usr/bin/env python
"""
Example implementation of abstract classes for ETL and QA pipeline.

Download OHLCV data from Binance and save it as CSV locally.

Use as:
# Download OHLCV data for binance:
> example_extract.py \
    --start_timestamp '2022-10-20 10:00:00+00:00' \
    --end_timestamp '2022-10-21 15:30:00+00:00' \
    --target_dir '.'
"""
import argparse
import logging
import os
import time
from typing import Any, Generator, Tuple

import pandas as pd
import requests
import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import surrentum_infra_sandbox.download as sinsadow
import surrentum_infra_sandbox.save as sinsasav

_LOG = logging.getLogger(__name__)


class OhlcvBinanceRestApiDownloader(sinsadow.DataDownloader):
    """
    Class for downloading OHLCV data using REST API provided by binance.
    """

    _MAX_LINES = 1000
    _UNIVERSE = {
        "binance": [
            "ETH_USDT",
            "BTC_USDT",
        ]
    }

    def download(
        self, start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp
    ) -> sinsadow.RawData:
        # Convert timestamps.
        hdateti.dassert_has_tz(start_timestamp)
        start_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
            start_timestamp
        )
        hdateti.dassert_has_tz(end_timestamp)
        end_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
            end_timestamp
        )
        hdbg.dassert_lt(
            start_timestamp_as_unix,
            end_timestamp_as_unix,
            msg="End timestamp should be greater then start timestamp.",
        )
        dfs = []
        for symbol in tqdm.tqdm(self._UNIVERSE["binance"]):
            for start_time, end_time in self._split_period_to_days(
                start_time=start_timestamp_as_unix, end_time=end_timestamp_as_unix
            ):
                url = self._build_url(
                    start_time=start_time,
                    end_time=end_time,
                    symbol=self._process_symbol(symbol),
                    limit=self._MAX_LINES,
                )
                response = requests.request(
                    method="GET",
                    url=url,
                    headers={"Content-Type": "application/json"},
                    data={},
                )
                hdbg.dassert_eq(response.status_code, 200)
                data = pd.DataFrame(
                    [
                        {
                            "currency_pair": symbol,
                            "open": row[1],
                            "high": row[2],
                            "low": row[3],
                            "close": row[4],
                            "volume": row[5],
                            # close_time from the raw response.
                            # The value is in ms, we add one milisecond.
                            #  based on the surrentum protocol data interval
                            #  specification, where interval [a, b) is labeled
                            #  with timestamp 'b'.
                            "timestamp": int(row[6]) + 1,
                        }
                        for row in response.json()
                    ]
                )
                dfs.append(data)
                # Delay for throttling in seconds.
                time.sleep(0.5)

        return sinsadow.RawData(pd.concat(dfs, ignore_index=True))

    def _build_url(
        self,
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
        return (
            f"https://api.binance.com/api/v3/klines?startTime={start_time}&endTime={end_time}"
            f"&symbol={symbol}&interval={interval}&limit={limit}"
        )

    def _process_symbol(self, symbol: str) -> str:
        """
        Transform symbol from universe to Binance format.
        """
        return symbol.replace("_", "")

    def _split_period_to_days(
        self, start_time: int, end_time: int
    ) -> Generator[Tuple[int, int], None, None]:
        """
        Chop period to chunks of the days.

        TLDR:
            The reason is:
                Binance API don't allow to get more then 1500 rows at once.
                So if we trying to get 1m interval, then we need to chop a period to
                chunks which Binance allow to get

        :param start_time: timestamp for the start time
        :param end_time: timestamp for the end time
        :return: generator for loop
        """
        step = 1000 * 60 * self._MAX_LINES
        for i in range(start_time, end_time, step):
            yield i, min(i + step, end_time)


class CSVDataFrameSaver(sinsasav.DataSaver):
    """
    Class for saving pandas DataFrame as CSV to a local filesystem at desired
    location.
    """

    def __init__(self, target_dir: str) -> None:
        """
        Constructor.

        :param target_dir: path to save data to.
        """
        self.target_dir = target_dir

    def save(self, data: sinsadow.RawData, **kwargs: Any) -> None:
        """
        Save RawData storing a DataFrame to CSV.

        :param data: data to persists into CSV.
        """
        if not isinstance(data.get_data(), pd.DataFrame):
            raise ValueError("Only DataFrame is supported.")
        # TODO(Juraj): rewrite using dataset_schema_utils.
        signature = (
            "bulk.manual.download_1min.csv.ohlcv.spot.v7.binance.binance.v1_0_0"
        )
        signature += ".csv"
        target_path = os.path.join(self.target_dir, signature)
        data.get_data().to_csv(target_path, index=False)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    downloader = OhlcvBinanceRestApiDownloader()
    raw_data = downloader.download(start_timestamp, end_timestamp)
    saver = CSVDataFrameSaver(args.target_dir)
    saver.save(raw_data)


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
        help="Beginning of the loaded period, e.g. 2022-02-09 10:00:00+00:00",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the loaded period, e.g. 2022-02-10 10:00:00+00:00",
    )
    parser.add_argument(
        "--target_dir",
        action="store",
        required=True,
        type=str,
        help="Absolute path to the target directory to store data to",
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    hdbg.init_logger(use_exec_path=True)
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = add_download_args(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
