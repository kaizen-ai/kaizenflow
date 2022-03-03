"""
Import as:

import im_v2.talos.data.extract.exchange_class as imvtdeexcl
"""


import logging
from typing import Dict, Optional, Union

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hsecrets as hsecret

_LOG = logging.getLogger(__name__)

_TALOS_HOST = "talostrading.com"


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
        hdbg.dassert_in(environment, ["sandbox", "prod"])
        keys = hsecret.get_secret(f"talos_{environment}")
        self._api_host = (
            _TALOS_HOST if environment == "prod" else f"sandbox.{_TALOS_HOST}"
        )
        self._api_key = keys["apiKey"]
        self._api_secret = keys["secret"]

    # TODO(Juraj): not needed for now
    # def calculate_signature(parts: Optional[List[str]]) -> str:
    #     """
    #     Compute a signature required for some types of GET and POST requests.
    #     (Not required for historical data.)

    #     :param parts: parts of the query to calculate signature from
    #     """
    #     payload = "\n".join(parts)
    #     hash = hmac.new(
    #         self._api_secret.encode("ascii"), payload.encode("ascii"), hashlib.sha256
    #     )
    #     hash.hexdigest()
    #     signature = base64.urlsafe_b64encode(hash.digest()).decode()
    #     return signature

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

    def timestamp_to_tz_naive_ISO_8601(self, timestamp: pd.Timestamp) -> str:
        """
        Transform Timestamp into a string in format accepted by Talos API.

        Example:
        2019-10-20T15:00:00.000000Z

        Note: microseconds must be included.
        """
        hdateti.dassert_is_tz_naive(timestamp)
        timestamp_iso_8601 = timestamp.isoformat(timespec="microseconds") + "Z"
        return timestamp_iso_8601

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
        """
        # TODO(Danya): Note: end timestamp is NOT included.
        # Start
        params: Dict[str, Union[str, int]] = {}
        if start_timestamp:
            start_date = self.timestamp_to_tz_naive_ISO_8601(start_timestamp)
            params["startDate"] = start_date
        if end_timestamp:
            end_date = self.timestamp_to_tz_naive_ISO_8601(end_timestamp)
            params["endDate"] = end_date
        params["limit"] = limit
        return params

    def download_ohlcv_data(
        self,
        currency_pair: str,
        exchange: str,
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        *,
        bar_per_iteration: int = 10000,
    ) -> pd.DataFrame:
        """
        Download minute OHLCV bars for given currency pair for given crypto
        exchange.

        :param currency_pair: a currency pair, e.g. "BTC_USDT"
        :param exchange: crypto exchange, e.g. "binance"
        :param start_timestamp: starting point for data
        :param end_timestamp: end point for data (included)
        :param bar_per_iteration: number of bars per iteration
        :return: Dataframe with OHLCV data from Talos
        """
        # TODO(Juraj): we can implement this check later if needed.
        # hdbg.dassert_in(
        #     currency_pair,
        #     self.currency_pairs,
        #     "Currency pair is not present in exchange",
        # )

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
        start_timestamp: Optional[pd.Timestamp],
        end_timestamp: Optional[pd.Timestamp],
        *,
        bar_per_iteration: int = 10000,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data for given currency and time.

         Example of full API request URL:
          https://sandbox.talostrading.com/v1/symbols/BTC-USDT/markets/binance/ohlcv/1m?startDate=2022-02-24T19:21:00.000000Z&startDate=2022-02-24T19:25:00.000000Z&limit=100
        url = f"https://{self._api_host}{path}{query}"

        :return: Dataframe with OHLCV data from Talos
        """
        headers = {"TALOS-KEY": self._api_key}
        params = self.build_talos_query_params(
            start_timestamp, end_timestamp, limit=bar_per_iteration
        )
        path = self.build_talos_ohlcv_path(currency_pair, exchange)
        url = f"https://{self._api_host}{path}"

        hasNext = True
        dfs = []
        while hasNext:
            r = requests.get(url=url, params=params, headers=headers)
            if r.status_code == 200:
                data = r.json()["data"]
                # Transform to dataframe and drop unnecessary columns.
                df = pd.DataFrame(data).drop(["Symbol", "Ticks"], axis=1)
                df["end_download_timestamp"] = str(
                    hdateti.get_current_time("UTC")
                )
                dfs.append(df)
                hasNext = "next" in r.json()
                # Handle pagination, details at:
                # https://docs.talostrading.com/#historical-ohlcv-candlesticks-rest
                if hasNext:
                    params["after"] = r.json()["next"]
            else:
                raise ValueError(
                    f"Request: {r.url} \n Finished with code: {r.status_code}"
                )

        columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "end_download_timestamp",
        ]
        concat_df = pd.concat(dfs)
        concat_df.columns = columns
        return concat_df