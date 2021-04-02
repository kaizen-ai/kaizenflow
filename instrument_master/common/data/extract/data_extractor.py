import abc
from typing import Optional

import pandas as pd

import instrument_master.common.data.types as vcdtyp


# TODO(gp): Rename file abstract_data_extractor.py
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
        dst_dir: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract the data for symbol, save , and return it.

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
        :param dst_dir: path to store the data
        :return: a dataframe with the data
        """
