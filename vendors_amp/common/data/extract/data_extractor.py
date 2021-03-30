import abc
from typing import Optional

import pandas as pd

import vendors_amp.common.data.types as vcdtyp


class AbstractDataExtractor(abc.ABC):
    """
    Load data from external sources.
    """

    @abc.abstractmethod
    def extract_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        frequency: vcdtyp.Frequency,
        contract_type: Optional[vcdtyp.ContractType] = None,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        incremental: Optional[bool] = None,
    ) -> pd.DataFrame:
        """
        Extract the data, save it and return all data for symbol.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type `futures`
        :param start_ts: start time of data to extract,
            by default - the oldest available
        :param end_ts: end time of data to extract,
            by default - now
        :param incremental: if True - save only new data,
            if False - remove old firstly,
            True by default
        :return: a dataframe with the data
        """
