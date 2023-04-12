#!/usr/bin/env python
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
_MAX_LINES = 5000

# #############################################################################
# CoinMarketCap(CMC) Api Downloader
# #############################################################################

class CMCRestApiDownloader(ssandown.DataDownloader):
    """
    Class for downloading data using REST API provided by CoinMarketCap.
    """
    def __init__(self) -> None:
        print(os.environ["COINMARKETCAP_API_KEY"])
        self.api_key = os.environ["COINMARKETCAP_API_KEY"]


    def get_data(self, start, limit):
        url = self._build_url(self, start, limit)
        print(url)
        print(self.api_key)
        # Request error and exception handling
        try:
            print("Requesting data...")
            response = requests.request(
                method="GET",
                url=url,
                headers={
                    "Content-Type": "application/json", 
                    "X-CMC_PRO_API_KEY": self.api_key, 
                    "Accepts": "application/json"},
                data={},
            )

            if response.status_code == 200:
                print("Request successful!")
                # return data
                # Convert all int to str to avoid OverflowError: MongoDB can only handle up to 8-byte ints
                return response.json(parse_int=str)['data']  
            else:
                print("Request failed, status code:", response.json()['status']['error_code'])
        except Exception as e:
            print("Request exception:", e)       


    def download(self, start, limit) -> ssandown.RawData:
        # request data
        cmc_data = self.get_data(start, limit)
        _LOG.info(f"Downloaded data")
        print("The First Data:", cmc_data[0])
        return ssandown.RawData(cmc_data)

    @staticmethod
    def _build_url(
        self,
        start: int = 1,
        limit: int = 5000,
        convert: str = "USD",
    ) -> str:
        """
        Build up URL with the placeholders from the args.
        """
        return (
            f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?"
            f"start={start}&convert={convert}&limit={limit}"
        )

# if __name__ == "__main__":
#     downloader = CMCRestApiDownloader()
#     raw_data = downloader.download(1,5000)