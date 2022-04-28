"""
This file contains a class for providing interface to download data from
Crypto-Chassis.

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
        self.exchanges = [
            "coinbase",
            "gemini",
            "kraken",
            "bitstamp",
            "bitfinex" "bitmex",
            "binance",
            "binance-us",
            "binance-usds-futures",
            "binance-coin-futures",
            "huobi",
            "huobi-usdt-swap",
            "huobi-coin-swap",
            "okex",
            "kucoin",
            "ftx",
            "ftx-us",
            "deribit",
        ]

    def build_url(
        self,
        data_type: str,
        exchange: str,
        currency_pair: str,
        depth: Optional[str] = None,
        interval: Optional[str] = None,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        include_real_time: Optional[int] = None,
    ) -> str:
        """
        Build valid URL to send request to CryptoChassis API.

        :param data_type:
        :param exchange:
        :param depth:
        :param currency_pair:
        :param interval:
        :param start_timestamp:
        :param end_timestamp:
        :param include_real_time:
        :return:
        """
        # Build main API URL.
        core_url = f"{self._endpoint}/{data_type}/{exchange}/{currency_pair}"
        # Store query parameters in the array to join them together.
        params = []
        if depth:
            params.append(f"depth={depth}")
        if interval:
            params.append(f"interval={interval}")
        if start_timestamp:
            str_start_time = start_timestamp.strftime("%Y-%m-%d")
            params.append(f"startTime={str_start_time}")
        if end_timestamp:
            str_end_time = end_timestamp.strftime("%Y-%m-%d")
            params.append(f"endTime={str_end_time}")
        if include_real_time:
            params.append(f"includeRealTime={str(include_real_time)}")
        #
        joined_params = "&".join(params)
        #
        parametrized_url = f"{core_url}?{joined_params}"
        return parametrized_url

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

    def download_trade(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """ """
        # Verify that date parameters are of correct format.
        hdbg.dassert_isinstance(
            start_timestamp,
            pd.Timestamp,
        )
        return self._fetch_trade(exchange, currency_pair, start_timestamp)

    def download_ohlc(
        self,
        exchange: str,
        currency_pair: str,
        interval: Optional[str],
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        include_real_time: Optional[int] = None,
    ) -> pd.DataFrame:
        """

        :param exchange:
        :param currency_pair:
        :param interval:
        :param start_timestamp:
        :param end_timestamp:
        :param include_real_time:
        """
        # Verify that date parameters are of correct format.
        hdbg.dassert_isinstance(
            end_timestamp,
            pd.Timestamp,
        )
        hdbg.dassert_isinstance(
            start_timestamp,
            pd.Timestamp,
        )
        hdbg.dassert_lte(
            start_timestamp,
            end_timestamp,
        )
        # TODO(Danya): @toma put data type assertions here before calling `fetch_market_depth`
        return self._fetch_ohlc(
            exchange,
            currency_pair,
            interval,
            start_timestamp,
            end_timestamp,
            include_real_time,
        )

    def _get_data(self, url: str) -> pd.DataFrame:
        """ """
        # Request the data.
        r = requests.get(url)
        # Get data CSV.
        df_csv = r.json()["urls"][0]["url"]
        # Read CSV.
        market_depth = pd.read_csv(df_csv, compression="gzip")
        return market_depth

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

    def _fetch_trade(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """ """
        # Build URL.
        url = self.build_url(
            data_type="trade",
            exchange=exchange,
            currency_pair=currency_pair,
            start_timestamp=start_timestamp,
        )
        # Request the data.
        market_depth = self._get_data(url)
        return market_depth

    def _fetch_ohlc(
        self,
        exchange: str,
        currency_pair: str,
        interval: Optional[str] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        include_real_time: Optional[int] = None,
    ) -> pd.DataFrame:
        """ """
        # Build URL.
        url = self.build_url(
            data_type="ohlc",
            exchange=exchange,
            currency_pair=currency_pair,
            interval=interval,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            include_real_time=include_real_time,
        )
        # Request the data.
        market_depth = self._get_data(url)
        return market_depth
