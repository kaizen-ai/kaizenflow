"""
Implement abstract extractor class.

Import as:

import im_v2.common.data.extract.extractor as imvcdeext
"""

import abc

import pandas as pd

import helpers.hdbg as hdbg


class Extractor(abc.ABC):
    """
    Abstract class for downloading raw data from all vendors.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__()

    def download_data(self, data_type: str, **kwargs) -> pd.DataFrame:
        """
        Download exchange data.

        :param data_type: the type of data, e.g. `market_depth`
        :return: exchange data
        """
        if data_type == "ohlcv":
            data = self._download_ohlcv(
            **kwargs,
        )
        elif data_type == "market_depth":
            data = self._download_market_depth(
                exchange_id=kwargs["exchange_id"],
                currency_pair=kwargs["currency_pair"],
                depth=kwargs["depth"],
                start_timestamp=kwargs["start_timestamp"],
        )
        elif data_type == "trades":
            data = self._download_trades(
                exchange_id=kwargs["exchange_id"],
                currency_pair=kwargs["currency_pair"],
                start_timestamp=kwargs["start_timestamp"],
        )
        else:            
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, market_depth, trades"
            )
        return data

    @abc.abstractmethod
    def _download_ohlcv(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_market_depth(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_trades(self, **kwargs) -> pd.DataFrame:
        ...

