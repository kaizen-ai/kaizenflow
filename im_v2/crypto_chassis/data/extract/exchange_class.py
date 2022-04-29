"""
Interface for downloading data from Crypto-Chassis:
https://github.com/crypto-chassis

Import as:

import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl
"""
import pandas as pd
import requests

import helpers.hdbg as hdbg


class CryptoChassisExchange:
    """
    A class for accessing Crypto-Chassis data.

    This class implements an access layer that
    retrieves data from specified exchange(s) 
    via Crypto-Chassis REST API.
    """

    def __init__(self) -> None:
        self._endpoint = "https://api.cryptochassis.com/v1"

    def download_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        **kwargs
    ) -> pd.DataFrame:
        """
        Download snapshot data on market depth.

        Additional parameters that can be passed as **kwargs:
          - startTime: pd.Timestamp 
          - depth: int - allowed values: 1, 10. Defaults to 1.
                
        :param exchange: the type of exchange, e.g. `binance`, `coinbase`
        :param currency_pair: the pair of currency to exchange, e.g. `btc-usd`
        :param kwargs: additional parameters of processing
        :return: market depth data
        """
        # Verify that date parameters are of correct format.
        if kwargs.get("startTime"):
            hdbg.dassert_isinstance(
                kwargs["startTime"],
                pd.Timestamp,
            )
            kwargs["startTime"] = kwargs["startTime"].strftime("%Y-%m-%dT%XZ")

        return self._fetch_market_depth(
            exchange, currency_pair, **kwargs
        )

    def _build_base_url(
        self,
        data_type: str,
        exchange: str,
        currency_pair: str,
    ) -> str:
        """
        Build valid URL to send request to CryptoChassis API.

        :param data_type: the type of data source, `market-depth`,
          `trade` or `ohlc`
        :param exchange: the exchange type, e.g. 'binance'
        :param currency_pair: the pair of currency to exchange,
          e.g. `btc-usd`
        :return: base URL of CryptoChassis API
        """
        # Build main API URL.
        core_url = f"{self._endpoint}/{data_type}/{exchange}/{currency_pair}"
        return core_url

    def _fetch_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        **kwargs
    ) -> pd.DataFrame:
        """ 
        Download snapshot data on market depth.
                
        :param exchange: the type of exchange, e.g. `binance`, `coinbase`
        :param currency_pair: the pair of currency to exchange, e.g. `btc-usd`
        :param kwargs: additional parameters of processing
        :return: market depth data
        """
        # Build URL.
        core_url = self.build_base_url(
            data_type="market-depth",
            exchange=exchange,
            currency_pair=currency_pair,
        )
        query_url = self._build_query_url(core_url, **kwargs)
        # Request the data.
        r = requests.get(query_url)
        market_depth = self._get_data(query_url)
        return market_depth

    def _build_query_url(self, base_url: str, **kwargs) -> str:
        """
        Combine base API URL and query parameters.

        :param base_url: base URL of CryptoChassis API
        :return: query URL with parameters
        """
        params = []
        for pair in kwargs.items():
            joined = "=".join(pair)
            params.append(joined)
        joined_params = "&".join(params)
        query_url = f"{base_url}?{joined_params}"
        return query_url
        
    def _get_data(self, url: str) -> pd.DataFrame:
        """ 
        Query API and load dataframe.
        
        :param url: API url to query
        :return: Crypto-Chassis data
        """
        # Request the data.
        r = requests.get(url)
        # Get data CSV.
        df_csv = r.json()["urls"][0]["url"]
        # Read CSV.
        market_depth = pd.read_csv(df_csv, compression="gzip")
        return market_depth


