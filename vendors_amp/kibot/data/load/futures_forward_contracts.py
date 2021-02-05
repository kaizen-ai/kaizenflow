import datetime
from typing import Union

import pandas as pd

import helpers.dataframe as hdataf
import helpers.dbg as dbg
import vendors_amp.kibot.data.load.data_loader as vkdlda
import vendors_amp.kibot.data.types as vkdtyp

_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


class FuturesForwardContracts:
    """
    Contract data for open futures contracts.
    """

    def __init__(self, data_loader: vkdlda.AbstractKibotDataLoader) -> None:
        """
        Initialize by injecting a data loader.

        :param data_loader: data loader implementing abstract interface
        """
        self._data_loader = data_loader

    def replace_contracts_with_data(
        self, df: pd.DataFrame, col: str
    ) -> pd.DataFrame:
        """
        Accept a series of contracts and return market data.

        :param df: dataframe of contracts indexed by a datetime index with a
            frequency, e.g.,
                                CL1   CL2
                2010-01-12    CLG10 CLH10
                2010-01-13    CLG10 CLH10
                2010-01-14    CLH10 CLJ10
        :param col: name of column to extract, e.g., "open", "close", "volume"
        :return: dataframe of market data indexed like `df`. Each contract
            name is replaced with its relevant col of market data (as of the
            time given by the index). E.g.,
                              CL1    CL2
                2010-01-12  80.79  81.17
                2010-01-13  79.65  80.04
                2010-01-14  79.88  80.47
        """
        data = []
        for column in df.columns:
            contract_srs = df[column]
            market_data = self._replace_contracts_with_data(contract_srs)
            data_srs = market_data[col]
            data_srs.name = column
            data.append(data_srs)
        data_df = pd.concat(data, axis=1)
        return data_df

    def _replace_contracts_with_data(self, srs: pd.Series) -> pd.DataFrame:
        """
        Accept a series of contracts and return market data.

        :param srs: series of contracts indexed by a datetime index with a
            frequency, e.g.,
                2010-01-12    CLG10
                2010-01-13    CLG10
                2010-01-14    CLH10
        :return: dataframe of market data indexed like `srs`. Each contract
            name is replaced with a row of market data (as of the time given
            by the index). E.g.,
                             open   high    low  close     vol
                2010-01-12  82.07  82.34  79.91  80.79  333866
                2010-01-13  80.06  80.67  78.37  79.65  401627
                2010-01-14  79.97  80.75  79.32  79.88  197449
        """
        # Determine whether to use daily or minutely contract data.
        ppy = hdataf.infer_sampling_points_per_year(srs)
        if ppy < 366:
            freq = vkdtyp.Frequency.Daily
        else:
            freq = vkdtyp.Frequency.Minutely
        # Get the list of contracts to extract data for.
        contracts = srs.unique().tolist()
        # Extract relevant data subseries for each contract and put in list.
        data_subseries = []
        for contract in contracts:
            # Load contract data.
            data = self._data_loader.read_data(
                "Kibot",
                contract,
                vkdtyp.AssetClass.Futures,
                freq,
                vkdtyp.ContractType.Expiry,
            )
            # Restrict to relevant subseries.
            subseries = data.reindex(srs[srs == contract].index)
            data_subseries.append(subseries.copy())
        # Merge the contract data over the partitioned srs index.
        df = pd.concat(data_subseries, axis=0)
        dbg.dassert_strictly_increasing_index(df)
        dbg.dassert(df.index.equals(srs.index))
        return df
