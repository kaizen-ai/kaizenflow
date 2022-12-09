"""
Implement abstract extractor class.

Import as:

import im_v2.common.data.extract.extractor as ivcdexex
"""

import abc
from typing import Any, Dict

import pandas as pd

import helpers.hdbg as hdbg


class Extractor(abc.ABC):
    """
    Abstract class for downloading raw data from all vendors.
    """

    def __init__(self) -> None:
        super().__init__()

    # TODO(Juraj): refactor to download_data_rest to clarify after addition of
    #  websocket based download in #CmTask2912.
    def download_data(
        self, data_type: str, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Download exchange data using REST API based download.

        :param data_type: the type of data, e.g. `bid_ask`
        :return: exchange data
        """
        if data_type == "ohlcv":
            data = self._download_ohlcv(
                exchange_id,
                currency_pair,
                **kwargs,
            )
        elif data_type == "bid_ask":
            data = self._download_bid_ask(exchange_id, currency_pair, **kwargs)
        elif data_type == "trades":
            data = self._download_trades(exchange_id, currency_pair, **kwargs)
        else:
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, bid_ask, trades"
            )
        return data

    def download_websocket_data(
        self, data_type: str, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        """
        Download exchange data using websocket based approach assuming the
        websocket corresponding to the particular exchange and currency_pair
        has been subscribed to via subscribe_to_websocket_data.

        :param data_type: the type of data, e.g. `bid_ask`
        :param exchange_id: exchange to get data from, e.g. `binance`
        :param currency_pair: currency_pair to get data about, e.g. `ETH_USDT`
        :return: exchange data
        """
        if data_type == "ohlcv":
            data = self._download_websocket_ohlcv(
                exchange_id,
                currency_pair,
                **kwargs,
            )
        elif data_type == "bid_ask":
            data = self._download_websocket_bid_ask(
                exchange_id, currency_pair, **kwargs
            )
        elif data_type == "trades":
            data = self._download_websocket_trades(
                exchange_id, currency_pair, **kwargs
            )
        else:
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, bid_ask, trades"
            )
        return data

    async def subscribe_to_websocket_data(
        self, data_type: str, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> None:
        """
        Subscribe to websocket based data stream for a particular exchange
        exchange_id and currency pair currency_pair.

        :param data_type: the type of data, e.g. `bid_ask`
        :param exchange_id: exchange to subscribe to, e.g. `binance`
        :param currency_pair: currency_pair to get data about, e.g. `ETH_USDT`
        :return: exchange data
        """
        if data_type == "ohlcv":
            await self._subscribe_to_websocket_ohlcv(
                exchange_id,
                currency_pair,
                **kwargs,
            )
        elif data_type == "bid_ask":
            await self._subscribe_to_websocket_bid_ask(
                exchange_id, currency_pair, **kwargs
            )
        elif data_type == "trades":
            await self._subscribe_to_websocket_trades(
                exchange_id, currency_pair, **kwargs
            )
        else:
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, bid_ask, trades"
            )

    @abc.abstractmethod
    def _download_ohlcv(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_bid_ask(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_trades(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_websocket_ohlcv(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        ...

    @abc.abstractmethod
    def _download_websocket_bid_ask(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        ...

    @abc.abstractmethod
    def _download_websocket_trades(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        ...

    @abc.abstractmethod
    def _subscribe_to_websocket_ohlcv(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        ...

    @abc.abstractmethod
    def _subscribe_to_websocket_bid_ask(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        ...

    @abc.abstractmethod
    def _subscribe_to_websocket_trades(
        self, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> Dict:
        ...