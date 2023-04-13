"""
Extract part of the ETL and QA pipeline.

Import as:

import sorrentum_sandbox.projects.Issue22_Team3_Implement_sandbox_for_Coinmarketcap.download_cmc as ssexbido
"""
import os
import logging
import time
from typing import Generator, Tuple

import pandas as pd
import requests
import tqdm

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import sorrentum_sandbox.common.download as ssandown

_LOG = logging.getLogger(__name__)
COINMARKETCAP_API_KEY = os.environ["COINMARKETCAP_API_KEY"]

# #############################################################################
# CoinMarketCap Api Downloader
# #############################################################################


class OhlcvRestApiDownloader(ssandown.DataDownloader):
    """
    Class for downloading data using REST API provided by CoinMarketCap.
    """

    _MAX_LINES = 5000

    def download(self) -> ssandown.RawData:
        # Download data once symbol at a time.
        dfs = []
        url = self._build_url(
            self,
            limit=self._MAX_LINES,
        )
        api_key=COINMARKETCAP_API_KEY
        print(api_key)

        # request data here
        response = requests.request(
            method="GET",
            url=url,
            headers={"Content-Type": "application/json", "X-CMC_PRO_API_KEY": api_key, "Accepts": "application/json"},
            data={},
        )

        print(response.json()["data"][0])

        hdbg.dassert_eq(response.status_code, 200)
        data = pd.DataFrame(
            [
                {
                    "id"                : row['id'],
                    "name"              : row['name'],
                    "num_market_pairs"  : row['num_market_pairs'],
                    "circulating_supply": row['circulating_supply'],
                    "price"             :row['quote']['USD']['price'],
                    "market_cap"        :row['quote']['USD']['market_cap'],
                    "volume_24h"        :row['quote']['USD']['volume_24h'],
                    "volume_change_24h"        :row['quote']['USD']['volume_change_24h'],
                    "percent_change_1h"        :row['quote']['USD']['percent_change_1h'],
                    "percent_change_24h"        :row['quote']['USD']['percent_change_24h'],
                    "percent_change_7d"        :row['quote']['USD']['percent_change_7d'],
                    "percent_change_30d"        :row['quote']['USD']['percent_change_30d'],
                    "percent_change_60d"        :row['quote']['USD']['percent_change_60d'],
                    "percent_change_90d"        :row['quote']['USD']['percent_change_90d'],
                }
                for row in response.json()["data"]
            ]
        )

        dfs.append(data)
        # Delay for throttling in seconds.
        time.sleep(0.5)
        df = pd.concat(dfs, ignore_index=True)
        _LOG.info(f"Downloaded data: \n\t {df.head()}")
        return ssandown.RawData(df)

    @staticmethod
    def _build_url(
        self,
        start: int = 1,
        convert: str = "USD",
        limit: int = 5000,
    ) -> str:
        """
        Build up URL with the placeholders from the args.
        """
        return (
            f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?"
            f"start={start}&convert={convert}&limit={limit}"
        )
