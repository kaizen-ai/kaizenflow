"""
This file contains a class for providing interface to download data from Talos
broker.

Import as:

import im_v2.talos.data.extract.exchange_class as imvtdeexcl
"""


import logging
from typing import Dict, Union

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret

_LOG = logging.getLogger(__name__)


class TalosExchange:
    """
    A class for accessing Talos exchange data.

    This class implements an access layer that retrieves data from
    specified exchange(s) via Talos REST API.
    """

    def __init__(self, environment: str) -> None:
        """
        Constructor.

        :param environment: specify if this instance should call the 'sandbox'
          or 'prod' API
        """
        _TALOS_HOST = "talostrading.com"
        hdbg.dassert_in(environment, ["sandbox", "prod"])
        keys = hsecret.get_secret(f"talos_{environment}")
        self._api_host = (
            _TALOS_HOST if environment == "prod" else f"sandbox.{_TALOS_HOST}"
        )
        self._api_key = keys["apiKey"]
        self._api_secret = keys["secret"]

    def build_talos_ohlcv_path(
        self, currency_pair: str, exchange: str, *, resolution: str = "1m"
    ) -> str:
        """
        Get data path for given symbol and exchange.

        Example: /v1/symbols/BTC-USD/markets/coinbase/ohlcv/1m
        """
        currency_pair = currency_pair.replace("_", "-")
        data_path = (
            f"/v1/symbols/{currency_pair}/markets/{exchange}/ohlcv/{resolution}"
        )
        return data_path

    def timestamp_to_talos_iso_8601(self, timestamp: pd.Timestamp) -> str:
        """
        Transform Timestamp into a string in the format accepted by Talos API.

        Example:
        2019-10-20T15:00:00.000000Z

        Note: microseconds must be included.
        """
        hdateti.dassert_is_tz_naive(timestamp)
        timestamp_iso_8601 = timestamp.isoformat(timespec="microseconds") + "Z"
        return timestamp_iso_8601  # type: ignore

    def build_talos_query_params(
        self,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        limit: int = 10000,
    ) -> Dict[str, Union[str, int]]:
        """
        Build params dictionary to pass into query.

        Example:
        params = { "startDate": 2019-10-20T15:00:00.000000Z,
                    "endDate=2019-10-23:28:0.000000Z,
                    "limit": 10000
                 }

        Note that endDate is an open interval, i.e. endDate is NOT included
        in the response.

        :param limit: number of records to return in request response
        """
        params: Dict[str, Union[str, int]] = {}
        start_date = self.timestamp_to_talos_iso_8601(start_timestamp)
        params["startDate"] = start_date
        end_date = self.timestamp_to_talos_iso_8601(end_timestamp)
        params["endDate"] = end_date
        params["limit"] = limit
        return params

    def download_ohlcv_data(
        self,
        currency_pair: str,
        exchange: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        bar_per_iteration: int = 10000,
    ) -> pd.DataFrame:
        """
        Download minute OHLCV bars for given currency pair for given crypto
        exchange.

        :param currency_pair: a currency pair, e.g. "BTC_USDT"
        :param exchange: crypto exchange, e.g. "binance"
        :param start_timestamp: starting point for data
        :param end_timestamp: end point for data (NOT included)
        :param bar_per_iteration: number of bars per iteration
        :return: dataframe with OHLCV data
        """
        # TODO(Juraj): we can implement this check later if needed.
        # hdbg.dassert_in(
        #     currency_pair,
        #     self.currency_pairs,
        #     "Currency pair is not present in exchange",
        # )
        return self._fetch_ohlcv(
            currency_pair,
            exchange,
            start_timestamp,
            end_timestamp,
            bar_per_iteration=bar_per_iteration,
        )

    def _fetch_ohlcv(
        self,
        currency_pair: str,
        exchange: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        bar_per_iteration: int = 10000,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data for given currency and time.

         Example of full API request URL:
          https://sandbox.talostrading.com/v1/symbols/BTC-USDT/markets/binance/ohlcv/1m?startDate=2022-02-24T19:21:00.000000Z&startDate=2022-02-24T19:25:00.000000Z&limit=100
        url = f"https://{self._api_host}{path}{query}"

        :return: Dataframe with OHLCV data
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
        headers = {"TALOS-KEY": self._api_key}
        params = self.build_talos_query_params(
            start_timestamp, end_timestamp, limit=bar_per_iteration
        )
        path = self.build_talos_ohlcv_path(currency_pair, exchange)
        url = f"https://{self._api_host}{path}"

        has_next = True
        dfs = []
        while has_next:
            r = requests.get(url=url, params=params, headers=headers)
            if r.status_code == 200:
                data = r.json()["data"]
                # Transform to dataframe and drop unnecessary columns.
                df = pd.DataFrame(data).drop(["Symbol"], axis=1)
                df["end_download_timestamp"] = str(
                    hdateti.get_current_time("UTC")
                )
                dfs.append(df)
                has_next = "next" in r.json()
                # Handle pagination, details at:
                # https://docs.talostrading.com/#historical-ohlcv-candlesticks-rest
                if has_next:
                    params["after"] = r.json()["next"]
            else:
                raise ValueError(
                    f"Request: {r.url} \n Finished with code: {r.status_code}"
                )
        # Assemble the results in a dataframe.
        columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ticks",
            "end_download_timestamp",
        ]
        concat_df = pd.concat(dfs)
        concat_df.columns = columns
        # Change from Talos date format (returned as string) to pd.Timestamp.
        concat_df["timestamp"] = concat_df["timestamp"].apply(
            hdateti.to_timestamp
        )
        # Change to unix epoch timestamp.
        concat_df["timestamp"] = concat_df["timestamp"].apply(
            hdateti.convert_timestamp_to_unix_epoch
        )
        return concat_df
