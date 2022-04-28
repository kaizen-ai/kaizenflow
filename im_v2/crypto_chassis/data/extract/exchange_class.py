"""
This file contains a class for providing interface to download data from
Crypto-Chassis.

Import as:

import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl
"""
from typing import Optional

import pandas as pd

class CryptoChassisExchange:
    """
    A class for accessing Crypto-Chassis data.

    This class implements an access layer that retrieves data from
    specified exchange(s) via Crypto-Chassis REST API.
    """

    def __init__(self) -> None:
        self._endpoint = "https://api.cryptochassis.com/v1"
        self.exchanges = ["coinbase",
        "gemini",
        "kraken",
        "bitstamp",
        "bitfinex"
        "bitmex",
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


    def build_url(self, 
        data_type: str, 
        exchange: str, 
        currency_pair: str, 
        depth: Optional[str]=None, 
        interval: Optional[str]=None,
        start_time: Optional[str]=None, 
        end_time: Optional[str]=None,
        include_real_time: Optional[int]=None
        ):
        """
        """
        core_url = f"{self.url}/{data_type}/{exchange}/{currency_pair}"
        params = []
        if depth:
            params.append(f"depth={depth}")
        if interval:
            params.append(f"interval={interval}")
        if start_time:
            params.append(f"startTime={start_time}")
        if end_time:
            params.append(f"endTime={end_time}")
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
        *,
        depth: Optional[int] = None
    ) -> pd.DataFrame:
        # TODO(Danya): @toma put data type assertions here before calling `fetch_market_depth`
        return market_depth

    def download_trade(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        # TODO(Danya): @toma put data type assertions here before calling `fetch_market_depth`

        return market_depth

    def download_ohlc(
        self,
        exchange: str,
        currency_pair: str,
        interval: Optional[str],
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        include_real_time: Optional[int]=None
    ) -> pd.DataFrame:
        # TODO(Danya): @toma put data type assertions here before calling `fetch_market_depth`
        
        return market_depth


    def _fetch_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
        depth: Optional[int],
    ) -> pd.DataFrame:
        """
        Example here: https://github.com/crypto-chassis/cryptochassis-data-api-
        docs#market-depth.
        """
        url = self.build_url(data_type="market_data", exchange=exchange, currency_pair=currency_pair, depth=depth, start_timestamp=start_timestamp)
        return market_depth

    def _fetch_trade(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Example here: https://github.com/crypto-chassis/cryptochassis-data-api-
        docs#market-depth.
        """
        url = self.build_url(data_type="trade", exchange=exchange, currency_pair=currency_pair)
        return market_depth

    def _fetch_ohlc(
        self,
        exchange: str,
        currency_pair: str,
        start_timestamp: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Example here: https://github.com/crypto-chassis/cryptochassis-data-api-
        docs#market-depth.
        """
        url = self.build_url(data_type="ohlc", exchange=exchange, currency_pair=currency_pair, )
        return market_depth