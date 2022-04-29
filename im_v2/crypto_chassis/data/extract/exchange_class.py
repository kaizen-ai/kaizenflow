"""
Interface for downloading data from Crypto-Chassis:
https://github.com/crypto-chassis

Import as:

import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl
"""
from typing import Optional

import pandas as pd
import requests

import helpers.hdbg as hdbg


class CryptoChassisExchange:
    """
    A class for accessing Crypto-Chassis data.

    This class implements an access layer that retrieves data from
    specified exchange(s) via Crypto-Chassis REST API.
    """

    def __init__(self) -> None:
        self._endpoint = "https://api.cryptochassis.com/v1"

    def build_url(
        self,
        data_type: str,
        exchange: str,
        currency_pair: str,
    ) -> str:
        """
        Build valid URL to send request to CryptoChassis API.

        :param data_type:
        :param exchange:
        :param currency_pair:
        :return: 
        """
        # Build main API URL.
        core_url = f"{self._endpoint}/{data_type}/{exchange}/{currency_pair}"
        return core_url

    def download_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
        depth: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        
                
        :param exchange:
        :param currency_pair:
        :param start_timestamp: 
        :param depth:
        :return:
        """
        # Verify that date parameters are of correct format.
        hdbg.dassert_isinstance(
            start_timestamp,
            pd.Timestamp,
        )
        return self._fetch_market_depth(
            exchange, currency_pair, start_timestamp, depth
        )

    def _fetch_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
        depth: Optional[int],
    ) -> pd.DataFrame:
        """ """
        # Build URL.
        url = self.build_url(
            data_type="market_data",
            exchange=exchange,
            currency_pair=currency_pair,
            depth=depth,
            start_timestamp=start_timestamp,
        )
        market_depth = self._get_data(url)
        return market_depth

    def _get_data(self, url: str) -> pd.DataFrame:
        """ 
        
        """
        # Request the data.
        r = requests.get(url)
        # Get data CSV.
        df_csv = r.json()["urls"][0]["url"]
        # Read CSV.
        market_depth = pd.read_csv(df_csv, compression="gzip")
        return market_depth


