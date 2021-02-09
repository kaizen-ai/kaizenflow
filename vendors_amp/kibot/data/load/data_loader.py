import abc
from typing import Optional

import pandas as pd

import vendors_amp.kibot.data.types as vkdtyp


class AbstractKibotDataLoader(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def read_data(
        cls,
        exchange: str,
        symbol: str,
        asset_class: vkdtyp.AssetClass,
        frequency: vkdtyp.Frequency,
        contract_type: Optional[vkdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        """
        Read kibot data.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe by frequency
        :return: a dataframe with the symbol data
        """
        ...
