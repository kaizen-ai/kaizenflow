from typing import Union
import datetime
import pandas as pd
import helpers.dbg as dbg
import vendors2.kibot.data.types as vkdt
import vendors2.kibot.data.load.s3_data_loader as vkdls3

_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


class FuturesForwardContracts:
    """
    Contract data for open futures contracts.
    """

    def _replace_contracts_with_data(srs: pd.Series) -> pd.Dataframe:
        """
        Accepts a series of contracts and returns market data.

        :param srs: series of contracts indexed by a datetime index with a
            frequency, e.g.,
                2010-01-12    CLG10
                2010-01-13    CLG10
                2010-01-14    CLH10
        :return: dataframe of market data indexed like `srs`. Each contract
            name is replaced with a row of market data (as of the time given
            by the index).
        """
        # Determine whether to use daily or minutely contract data.
        ppy = hdataf.infer_sampling_points_per_year(srs)
        if ppy < 366:
            freq = vkdt.Frequency.Daily
        else:
            freq = vkdt.Frequency.Minutely
        # Get the list of contracts to extract data for.
        contracts = srs.unique().tolist()
        # Create a list of data for each contract.
        data_subseries = []
        for contract in contracts:
            data = vkdls3.read_data("Kibot", contract, vkdt.AssetClass.Futures,
                                    freq, vkdt.ContractType.Expiry)
            data_subseries.append(data.reindex(srs[srs == contract].index).copy())
        # Merge the contract data over the partitioned srs index.
        df = pd.concat(data_subseries, axis=0)
        dbg.dassert_strictly_increasing_index(df)
        dbg.dassert(df.index.equals(srs.index))
        return df
