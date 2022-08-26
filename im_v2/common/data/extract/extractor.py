"""
Implement abstract extractor class.

Import as:

import im_v2.common.data.extract.extractor as imvcdexex
"""

import abc
from typing import Any

import pandas as pd

import helpers.hdbg as hdbg


class Extractor(abc.ABC):
    """
    Abstract class for downloading raw data from all vendors.
    """

    def __init__(self) -> None:
        super().__init__()

    def download_data(
        self, data_type: str, exchange_id: str, currency_pair: str, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Download exchange data.

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
            data = self._download_bid_ask(
                exchange_id, currency_pair, **kwargs
            )
        elif data_type == "trades":
            data = self._download_trades(exchange_id, currency_pair, **kwargs)
        else:
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, bid_ask, trades"
            )
        return data

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