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
            data = self.download_ohlcv(
            # TODO(Danya): The thing is, we don't pass all of these values
            #  all the time. For example, `mode` is very vague concept, and
            #  `interval` has a very definite default value ("1m").
            # Ideally, we should have defaults for each parameter except for `exchange_id` and `currency_pair`.
            **kwargs,
        )
        elif data_type == "market_depth":
            data = self.download_market_depth(
                exchange=kwargs["exchange_id"],
                currency_pair=kwargs["currency_pair"],
                depth=kwargs["depth"],
                start_timestamp=kwargs["start_timestamp"],
        )
        elif data_type == "trades":
            data = self.download_trade(
                exchange=kwargs["exchange_id"],
                currency_pair=kwargs["currency_pair"],
                start_timestamp=kwargs["start_timestamp"],
        )
        else:            
            hdbg.dfatal(
                f"Unknown data type {data_type}. Possible data types: ohlcv, market_depth, trades"
            )
        return data

    @abc.abstractmethod
    def download_ohlcv(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def download_bid_ask(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def download_market_depth(self, **kwargs) -> pd.DataFrame:
        ...

    @abc.abstractmethod
    def download_trades(self, **kwargs) -> pd.DataFrame:
        ...

