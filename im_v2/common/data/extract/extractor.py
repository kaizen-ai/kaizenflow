"""
Implement abstract extractor class.

Import as:

import im_v2.common.data.extract.extractor as imvcdeext
"""

import abc
import logging
import os

import pandas as pd

import helpers.hdbg as hdbg


class Extractor(abc.ABC):
    """
    """
    def __init__(self, **kwargs) -> None:
        super().__init__()

    def download_data(self, data_type: str, **kwargs) -> pd.DataFrame:
        """
        Download Crypto Chassis data.

        :param data_type: the type of data, e.g. `market_depth`
        :return: Crypto Chassis data
        """
        if data_type == "ohlcv":
            data = self.download_ohlcv(**kwargs)
        elif data_type == "market_depth":
            data = self.download_market_depth(**kwargs)
        elif data_type == "trades":
            data = self.download_trades(**kwargs)
        elif data_type =="bid_ask":
            data = self.download_bid_ask(**kwargs)
        else:            
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, market_depth"
            )
        return data

    @abc.abstractmethod
    def _download_ohlcv(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_bid_ask(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_market_depth(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def _download_trades(self, **kwargs) -> pd.DataFrame:
        ...

