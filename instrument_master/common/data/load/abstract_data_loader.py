"""
Import as:

import instrument_master.common.data.load.abstract_data_loader as icdlab
"""

import abc
from typing import Optional

import pandas as pd

import helpers.dbg as dbg
import instrument_master.common.data.types as icdtyp
import instrument_master.common.data.types as vcdtyp


class AbstractDataLoader(abc.ABC):
    """
    Read data for symbols of a given asset, exchange, and frequency.
    """
    @abc.abstractmethod
    def read_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        """
        Read data.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type `futures`
        :param unadjusted: required for asset classes of type `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe based on frequency
        :return: a dataframe with the data
        """


class AbstractS3DataLoader(AbstractDataLoader):
    """Interface for class reading data from S3."""

    def __init__(self):
        self.MAPPING = {
            icdtyp.Frequency.Daily: self._normalize_daily,
            icdtyp.Frequency.Minutely: self._normalize_1_min,
        }

    def normalize(
        self, df: pd.DataFrame, frequency: icdtyp.Frequency
    ) -> pd.DataFrame:
        """
        Apply a normalizer function based on the frequency.

        :param df: a dataframe that should be normalized
        :param frequency: frequency of the data
        :return: a normalized dataframe
        :raises AssertionError: if frequency is not supported
        """
        if frequency not in self.MAPPING:
            dbg.dfatal(
                "Support for frequency '%s' not implemented yet", frequency
            )
        normalizer = self.MAPPING[frequency]
        return normalizer(df)

    @abc.abstractstaticmethod
    def _normalize_1_min(df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method for minutes data normalization.

        :param df: Pandas DataFrame for the normalization.
        :return: Normalized Pandas DataFrame
        """

    @abc.abstractstaticmethod
    def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method for minutes data normalization.

        :param df: Pandas DataFrame for the normalization.
        :return: Normalized Pandas DataFrame
        """
