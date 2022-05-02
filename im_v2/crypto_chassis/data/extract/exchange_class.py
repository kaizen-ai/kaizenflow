"""
Download data from Crypto-Chassis: https://github.com/crypto-chassis.

Import as:

import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl
"""
from typing import Any, Optional

import pandas as pd
import requests

import helpers.hdbg as hdbg


class CryptoChassisExchange:
    """
    Access exchange data from Crypto-Chassis through REST API.
    """

    def __init__(self) -> None:
        self._endpoint = "https://api.cryptochassis.com/v1"

    def download_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        depth: Optional[int] = None,
        start_timestamp: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """
        Download snapshot data on market depth.

        Additional parameters that can be passed as **kwargs:
          - startTime: pd.Timestamp
          - depth: int - allowed values: 1 to 10. Defaults to 1.

        :param exchange: the name of exchange, e.g. `binance`, `coinbase`
        :param currency_pair: the pair of currency to exchange, e.g. `btc-usd`
        :param kwargs: additional parameters of processing
        :return: market depth data
        """
        # Verify that date parameters are of correct format.
        if start_timestamp:
            hdbg.dassert_isinstance(
                start_timestamp,
                pd.Timestamp,
            )
            start_timestamp = start_timestamp.strftime("%Y-%m-%dT%XZ")
        # Currency pairs in market data are stored in `cur1/cur2` format, 
        # Crypto Chassis API processes currencies in `cur1-cur2` format, therefore
        # convert the specified pair to this view.
        currency_pair = currency_pair.replace("/", "-")
        # Build base URL.
        core_url = self._build_base_url(
            data_type="market-depth",
            exchange=exchange,
            currency_pair=currency_pair,
        )
        # Build URL with specified parameters.
        query_url = self._build_query_url(
            core_url, startTime=start_timestamp, depth=depth
        )
        # Request the data.
        r = requests.get(query_url)
        # Retrieve raw data.
        df_csv = r.json()["urls"][0]["url"]
        # Convert CSV into dataframe.
        market_depth = pd.read_csv(df_csv, compression="gzip")
        # Separate `bid_price_bid_size` column to `bid_price` and `bid_size`.
        market_depth["bid_price"], market_depth["bid_size"] = zip(
            *market_depth["bid_price_bid_size"].apply(lambda x: x.split("_"))
        )
        # Separate `ask_price_ask_size` column to `ask_price` and `ask_size`.
        market_depth["ask_price"], market_depth["ask_size"] = zip(
            *market_depth["ask_price_ask_size"].apply(lambda x: x.split("_"))
        )
        # Remove deprecated columns.
        market_depth = market_depth.drop(
            columns=["bid_price_bid_size", "ask_price_ask_size"]
        )
        return market_depth

    def _build_base_url(
        self,
        data_type: str,
        exchange: str,
        currency_pair: str,
    ) -> str:
        """
        Build valid URL to send request to CryptoChassis API.

        :param data_type: the type of data source, `market-depth`, `trade` or `ohlc`
        :param exchange: the exchange type, e.g. 'binance'
        :param currency_pair: the pair of currency to exchange, e.g. `btc-usd`
        :return: base URL of CryptoChassis API
        """
        # Build main API URL.
        core_url = f"{self._endpoint}/{data_type}/{exchange}/{currency_pair}"
        return core_url

    def _build_query_url(self, base_url: str, **kwargs: Any) -> str:
        """
        Combine base API URL and query parameters.

        :param base_url: base URL of CryptoChassis API
        :return: query URL with parameters
        """
        params = []
        for pair in kwargs.items():
            if pair[1] is not None:
                # Check whether the parameter is not empty.
                joined = "=".join(pair)
                params.append(joined)
        joined_params = "&".join(params)
        query_url = f"{base_url}?{joined_params}"
        return query_url
