import abc
from typing import Optional, Union, List, Dict

import pandas as pd

import vendors2.kibot.data.types as vkdtyp


class AbstractKibotDataLoader(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def read_data(
        cls,
        exchange: str,
        symbol: Union[str, List[str]],
        asset_class: vkdtyp.AssetClass,
        frequency: vkdtyp.Frequency,
        contract_type: Optional[vkdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read kibot data.

        If symbol is a string, return a dataframe with data related to this symbol.
        If symbol is a list, return a dictionary with symbol as key, data as value pairs.

        :param exchange: name of the exchange
        :param symbol: symbol or list of symbols to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe by frequency
        :return: a dataframe with the symbol data
        """
        ...
