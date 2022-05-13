"""
Download data from Crypto-Chassis: https://github.com/crypto-chassis.

Import as:

import im_v2.crypto_chassis.data.extract.exchange_class as imvccdeecl
"""
import logging
from typing import Any, Optional

import pandas as pd
import requests

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


class CryptoChassisExchange:
    """
    Access exchange data from Crypto-Chassis through REST API.
    """

    def __init__(self) -> None:
        self._endpoint = "https://api.cryptochassis.com/v1"

    @staticmethod
    def convert_currency_pair(currency_pair: str) -> str:
        """
        Convert currency pair used for getting data from exchange.
        """
        return currency_pair.replace("_", "/").lower()

    def download_data(self, data_type: str, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """
        Download Crypto Chassis data.

        :param data_type: the type of data, e.g. `market_depth`
        :return: Crypto Chassis data
        """
        # Get data.
        if data_type == "ohlcv":
            data = self._download_ohlcv(
            exchange=kwargs["exchange_id"],
            currency_pair=kwargs["currency_pair"],
            mode=kwargs["mode"],
            start_timestamp=kwargs["start_timestamp"],
            end_timestamp=kwargs["end_timestamp"],
            interval=kwargs["interval"],
            include_realtime=kwargs["include_realtime"],
        )
        elif data_type == "market_depth":
            data = self._download_market_depth(
                exchange=kwargs["exchange_id"],
                currency_pair=kwargs["currency_pair"],
                depth=kwargs["depth"],
                start_timestamp=kwargs["start_timestamp"],
        )
        elif data_type == "trades":
            data = self._download_trade(
                exchange=kwargs["exchange_id"],
                currency_pair=kwargs["currency_pair"],
                start_timestamp=kwargs["start_timestamp"],
        )
        else:
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, market_depth"
            )
        return data

    def _download_market_depth(
        self,
        exchange: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
        depth: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Download snapshot data on market depth.

            timestamp 	bid_price 	bid_size 	ask_price 	ask_size
        0 	1641686400 	41678.35 	0.017939 	41686.97 	1.69712319
        1 	1641686401 	41678.35 	0.017939 	41690.58 	0.04

        :param exchange: the name of exchange, e.g. `binance`, `coinbase`
        :param currency_pair: the pair of currency to exchange, e.g. `btc-usd`
        :param start_timestamp: start of processing
        :param depth: allowed values: 1 to 10. Defaults to 1.
        :return: market depth data
        """
        # Verify that date parameters are of correct format.
        if start_timestamp:
            hdbg.dassert_isinstance(
                start_timestamp,
                pd.Timestamp,
            )
            start_timestamp = start_timestamp.strftime("%Y-%m-%dT%XZ")
        if depth:
            hdbg.dassert_lgt(1, depth, 10, True, True)
            depth = str(depth)
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
        data_json = r.json()
        if data_json.get("urls") is None:
            # Return empty dataframe if there is no results.
            return pd.DataFrame()
        df_csv = data_json["urls"][0]["url"]
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
        # Rename time column.
        market_depth = market_depth.rename(columns={"time_seconds": "timestamp"})
        return market_depth

    def _download_ohlcv(
        self,
        exchange: str,
        currency_pair: str,
        mode: str,
        *,
        interval: Optional[str] = None,
        start_timestamp: Optional[pd.Timestamp] = None,
        end_timestamp: Optional[pd.Timestamp] = None,
        include_realtime: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Download snapshot of ohlcv.

            timestamp 	open 	    high 	    low 	    close 	   volume  vwap  number_of_trades 	twap
        0 	1634011620 	56775.59 	56799.51 	56775.59 	56799.51 	0.184718 	56781.6130 	9 	56783.3033
        1 	1634011680 	56822.35 	56832.25 	56815.59 	56815.59 	0.363495 	56828.9840 	16 	56828.9512

        :param exchange: the name of exchange, e.g. `binance`, `coinbase`
        :param currency_pair: the pair of currency to download, e.g. `btc-usd`
        :param mode: `recent` for real-time data, `historical` for historical data
        :param interval: interval between data points in one bar, e.g. `1m` (default), `5h`, `2d`
        :param start_time: timestamp of start
        :param end_time: timestamp of end
        :param include_realtime: 0 (default) or 1. If set to 1, request rate limit on this
            endpoint is 1 request per second per public IP.
        :return: ohlcv data
        """
        # Verify that date parameters are of correct format.
        if start_timestamp:
            hdbg.dassert_isinstance(
                start_timestamp,
                pd.Timestamp,
            )
            # Convert datetime to unix time, e.g. `2022-01-09T00:00:00` -> `1641686400`.
            start_timestamp = int(start_timestamp.timestamp())
        if end_timestamp:
            hdbg.dassert_isinstance(
                end_timestamp,
                pd.Timestamp,
            )
            end_timestamp = int(end_timestamp.timestamp())
        # Currency pairs in market data are stored in `cur1/cur2` format,
        # Crypto Chassis API processes currencies in `cur1-cur2` format, therefore
        # convert the specified pair to this view.
        currency_pair = currency_pair.replace("/", "-")
        # Build base URL.
        core_url = self._build_base_url(
            data_type="ohlc",
            exchange=exchange,
            currency_pair=currency_pair,
        )
        # Build URL with specified parameters.
        query_url = self._build_query_url(
            core_url,
            startTime=start_timestamp,
            endTime=end_timestamp,
            interval=interval,
            includeRealTime=include_realtime,
        )
        # Request the data.
        r = requests.get(query_url)
        # Retrieve raw data.
        data_json = r.json()
        if data_json.get(mode) is None:
            # Return empty dataframe if there is no results.
            ohlcv_data = pd.DataFrame()
            _LOG.warning("No data found at `%s`. Returning empty DataFrame.", query_url)
        else:
            if mode == "recent":
                # Process real-time.
                # Get columns.
                columns = data_json[mode]["fields"].split(", ")
                # Build Dataframe.
                ohlcv_data = pd.DataFrame(
                    columns=columns, data=data_json[mode]["data"]
                )
            elif mode == "historical":
                # Process historical data.
                df_csv = data_json[mode]["urls"][0]["url"]
                # Convert CSV into dataframe.
                ohlcv_data = pd.read_csv(df_csv, compression="gzip")
                # Filter the time period since Crypto Chassis doesn't provide this functionality.
                # (CmTask #1887).
                if start_timestamp:
                    ohlcv_data = ohlcv_data[
                        (ohlcv_data["time_seconds"] >= start_timestamp)
                    ]
                if end_timestamp:
                    ohlcv_data = ohlcv_data[
                        (ohlcv_data["time_seconds"] <= end_timestamp)
                    ]
            else:
                raise ValueError(f"Invalid mode=`{mode}`")
        # Rename time column.
        ohlcv_data = ohlcv_data.rename(columns={"time_seconds": "timestamp"})
        return ohlcv_data

    def _download_trade(
        self,
        exchange: str,
        currency_pair: str,
        *,
        start_timestamp: Optional[pd.Timestamp] = None,
    ) -> pd.DataFrame:
        """
        Download snapshot of trade data.

            timestamp 	price 	    size 	   is_buyer_maker
        0 	1641686404 	41692.50 	0.012473 	0
        1 	1641686441 	41670.00 	0.001194 	0

        :param exchange: the name of exchange, e.g. `binance`, `coinbase`
        :param currency_pair: the pair of currency to download, e.g. `btc-usd`
        :param start_time: timestamp of start
        :return: trade data
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
            data_type="trade",
            exchange=exchange,
            currency_pair=currency_pair,
        )
        # Build URL with specified parameters.
        query_url = self._build_query_url(
            core_url, startTime=start_timestamp
        )
        # Request the data.
        r = requests.get(query_url)
        # Retrieve raw data.
        data_json = r.json() 
        if data_json.get("urls") is None:
            # Return empty dataframe if there is no results.
            return pd.DataFrame()
        df_csv = data_json["urls"][0]["url"]
        # Convert CSV into dataframe.
        trade = pd.read_csv(df_csv, compression="gzip")
        # Rename time column.
        trade = trade.rename(columns={"time_seconds": "timestamp"})
        # Convert float timestamp to int.
        trade["timestamp"] = trade["timestamp"].apply(lambda x: int(x))
        return trade

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
        Additional parameters that can be passed as **kwargs:
          - depth: int - allowed values: 1 to 10. Defaults to 1.
          - interval: str, e.g. `1m`, `3m`, `5m` etc.
          - startTime: pd.Timestamp
          - endTime: pd.Timestamp
          - includeRealTime: 0, 1. If set to 1, request rate limit on this
            endpoint is 1 request per second per public IP.
        :return: query URL with parameters
        """
        params = []
        for pair in kwargs.items():
            if pair[1] is not None:
                # Check whether the parameter is not empty.
                # Convert value to string and join query parameters.
                joined = "=".join([pair[0], str(pair[1])])
                params.append(joined)
        joined_params = "&".join(params)
        query_url = f"{base_url}?{joined_params}"
        return query_url
