import abc
from typing import Optional

import pandas as pd

import instrument_master.common.data.types as vcdtyp


class AbstractDataLoader(abc.ABC):
    """
    Reads data for symbols of a given asset and exchange.
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