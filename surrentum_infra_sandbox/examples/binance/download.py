"""
Example implementation of abstract classes for extract part of the ETL and QA
pipeline.

Import as:

import surrentum_infra_sandbox.examples.binance.download as sisebido
"""

import logging
import time
from typing import Generator, Tuple

import pandas as pd
import requests
import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import surrentum_infra_sandbox.download as sinsadow

_LOG = logging.getLogger(__name__)

# TODO(gp): example_extract.py -> extract.py


# TODO(gp): -> OhlcvRestApiDownloader
class OhlcvBinanceRestApiDownloader(sinsadow.DataDownloader):
    """
    Class for downloading OHLCV data using REST API provided by Binance.
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
                            # The value is in ms, we add one millisecond.
                            # based on the Surrentum protocol data interval
                            # specification, where interval [a, b) is labeled
                            # with timestamp 'b'.
                            "timestamp": int(row[6]) + 1,
                            "end_download_timestamp": hdateti.get_current_time(
                                "UTC"
                            ),
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
