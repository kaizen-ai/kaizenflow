import logging
import time
from typing import Generator, Tuple

import pandas as pd
import requests
import kaiko as ka

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import common.download as ssandown
_LOG = logging.getLogger(__name__)

class KaikoDownloader(ssandown.DataDownloader):
    """
    Class for downloading kaiko Data using kaiko python library
    """

    _MAX_LINES = 1000
    _UNIVERSE = {
        "kaiko": [
            "ETH-USD",
            "BTC-USD",
        ]
    }

    def download(
        self, start_timestamp: str, end_timestamp: str, interval
    ) -> ssandown.RawData:
        dfs = []
        for symbol in self._UNIVERSE["kaiko"]:

            data = ka.download(
            tickers = symbol,
            start=start_timestamp,
            end=end_timestamp,
            interval = interval,
            ignore_tz = True,
            prepost = False,
            )
            data['timestamp']=data.index
            data['currency_pair']=symbol
            data['exchangeTimezoneName'] = ka.Ticker(symbol).history_metadata['exchangeTimezoneName']
            data['timezone'] = ka.Ticker(symbol).history_metadata['timezone']
            
            dfs.append(data)
            # Delay for throttling in seconds.
            time.sleep(0.5)
        df = pd.concat(dfs, ignore_index=True)
        df.columns=[x.replace(' ','_').lower() for x in list(df.columns)]
        print(df.columns)

        #df = df[df["timestamp"] <= end_timestamp_as_unix]
        _LOG.info(f"Downloaded data: \n\t {df.head()}")
        return ssandown.RawData(df)

print('Done')