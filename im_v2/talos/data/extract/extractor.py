"""
This file contains a class for providing interface to download data from Talos
broker.

Import as:

import im_v2.talos.data.extract.extractor as imvtdexex
"""


import logging
from typing import Any, Dict, Union

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import im_v2.common.data.extract.extractor as ivcdexex
import im_v2.talos.utils as imv2tauti

_LOG = logging.getLogger(__name__)


class TalosExtractor(ivcdexex.Extractor):
    """
    A class for accessing Talos exchange data.

    This class implements an access layer that retrieves data from
    specified exchange(s) via Talos REST API.
    """

    def __init__(self, account: str) -> None:
        """
        Constructor.
        """
        super().__init__()
        self._account = account
        self._api = imv2tauti.TalosApiBuilder(self._account)
        self._endpoint = self._api.get_endpoint()
        self.vendor = "talos"

    @staticmethod
    def build_talos_query_params(
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

        :param start_timestamp: beginning of the queried time period
        :param end_timestamp: end of the queried time period
        :param limit: number of records to return in request response
        :return: query parameters for OHLCV data
        """
        params: Dict[str, Union[str, int]] = {}
        start_date = imv2tauti.timestamp_to_talos_iso_8601(start_timestamp)
        params["startDate"] = start_date
        end_date = imv2tauti.timestamp_to_talos_iso_8601(end_timestamp)
        params["endDate"] = end_date
        params["limit"] = limit
        return params

    @staticmethod
    def convert_currency_pair(currency_pair: str) -> str:
        """
        Convert currency pair used for getting data from exchange.
        """
        return currency_pair.replace("_", "-")

    def build_url(
        self, exchange: str, currency_pair: str, *, resolution: str = "1m"
    ) -> str:
        """
        Get url for given symbol and exchange.
        """
        currency_pair = currency_pair.replace("_", "-")
        data_path = (
            f"/v1/symbols/{currency_pair}/markets/{exchange}/ohlcv/{resolution}"
        )
        url = f"https://{self._endpoint}{data_path}"
        return url

    def _download_ohlcv(
        self,
        exchange_id: str,
        currency_pair: str,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        *,
        bar_per_iteration: int = 10000,
    ) -> pd.DataFrame:
        """
        Download minute OHLCV bars for given currency pair for given crypto
        exchange.

        :param currency_pair: a currency pair, e.g. "BTC_USDT"
        :param exchange_id: crypto exchange, e.g. "binance"
        :param start_timestamp: starting point for data
        :param end_timestamp: end point for data
        :param bar_per_iteration: number of bars per iteration
        :return: dataframe with OHLCV data
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
        # TODO(Juraj): we can implement this check later if needed.
        # hdbg.dassert_in(
        #     currency_pair,
        #     self.currency_pairs,
        #     "Currency pair is not present in exchange",
        # )
        # Workaround for including `end_timestamp` value.
        # If proposing query for a complete minute (e.g., 10:07:00)
        # Results will contain itemps with timestamp - 1min (i.e., 10:06:00).
        # In order to get items including items with timestamp=`end_timestamp`
        # `end_timestamp` should be shifted on 1 sec from complete minute.
        rounded_timestamp = end_timestamp.round(freq="T")
        if rounded_timestamp == end_timestamp:
            end_timestamp = end_timestamp + pd.Timedelta("1sec")
        #
        return self._fetch_ohlcv(
            exchange_id,
            currency_pair,
            start_timestamp,
            end_timestamp,
            bar_per_iteration=bar_per_iteration,
        )

    def _download_bid_ask(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError(
            "Bid_ask data is not available for Talos vendor"
        )

    def _download_trades(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError("Trades data is not available for Talos vendor")

    def _fetch_ohlcv(
        self,
        exchange: str,
        currency_pair: str,
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
        # Create header with secret key.
        headers = self._api.build_headers(parts=None, wall_clock_timestamp=None)
        # Create OHLCV-specific query parameters.
        params = self.build_talos_query_params(
            start_timestamp, end_timestamp, limit=bar_per_iteration
        )
        url = self.build_url(exchange, currency_pair)
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

    def _download_websocket_ohlcv(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        raise NotImplementedError(
            "This method is not implemented for Talos vendor yet."
        )

    def _download_websocket_bid_ask(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        raise NotImplementedError(
            "This method is not implemented for Talos vendor yet."
        )

    def _download_websocket_trades(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        raise NotImplementedError(
            "This method is not implemented for Talos vendor yet."
        )

    def _subscribe_to_websocket_ohlcv(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        raise NotImplementedError(
            "This method is not implemented for Talos vendor yet."
        )

    def _subscribe_to_websocket_bid_ask(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        raise NotImplementedError(
            "This method is not implemented for Talos vendor yet."
        )

    def _subscribe_to_websocket_trades(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        raise NotImplementedError(
            "This method is not implemented for Talos vendor yet."
        )
