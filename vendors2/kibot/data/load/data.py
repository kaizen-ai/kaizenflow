import functools
from typing import Optional

import pandas as pd

import helpers.cache as cache
import vendors2.kibot.data.load.file_path_generator as fpgen
import vendors2.kibot.data.types as types

MEMORY = cache.get_disk_cache(tag=None)


class KibotDataLoader:
    @staticmethod
    @functools.lru_cache(maxsize=None)
    def read_data(
        symbol: str,
        asset_class: types.AssetClass,
        frequency: types.Frequency,
        contract_type: Optional[types.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        cache_data: bool = True,
    ) -> pd.DataFrame:
        """Read kibot data.

        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param cache_data: whether to use cached data if exists
        :return: a dataframe with the symbol data
        """
        file_path = fpgen.FilePathGenerator().generate_file_path(
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
            unadjusted=unadjusted,
        )
        if cache_data:
            data = _read_data_from_disk_cache(file_path, nrows)
        else:
            data = _read_data_from_disk(file_path, nrows)
        return data


@MEMORY.cache
def _read_data_from_disk_cache(
    file_path: str, nrows: Optional[int]
) -> pd.DataFrame:
    data = _read_data(file_path, nrows)
    return data


def _read_data_from_disk(file_path: str, nrows: Optional[int]) -> pd.DataFrame:
    data = _read_data(file_path, nrows)
    return data


def _read_data(file_path: str, nrows: Optional[int]) -> pd.DataFrame:
    df = pd.read_parquet(file_path)
    if nrows is not None:
        df = df.head(nrows)
    return df
