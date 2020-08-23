import functools
import os
from typing import Dict, Optional, Tuple

import pandas as pd

import helpers.cache as cache
import helpers.s3 as hs3
import vendors2.kibot.data.types as types
import vendors2.kibot.data.load.file_path_generator as fpgen


class KibotDataLoader:
    @staticmethod
    @functools.lru_cache(maxsize=None)
    def read_data(
        frequency: types.Frequency,
        contract_type: types.ContractType,
        symbols: Tuple[str],
        nrows: Optional[int] = None,
        cache_data: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """Read kibot data.

        If the ext is `csv`, this function will
        - parse dates
        - add column names
        - check for monotonic index
        For pq data, all of those transformations have already been made,
        so this function reads the data without modifying it.

        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: `continuous` or `expiry`
        :param symbols: tuple of symbols
        :param nrows: if not None, return only the first nrows of the data
        :param cache_data: whether to use cached data if exists
        :return: return a dictionary of dataframes for each symbol.
        """
        return {
            symbol: _read_symbol_data(
                frequency, contract_type, symbol, nrows, cache_data
            )
            for symbol in symbols
        }


MEMORY = cache.get_disk_cache(tag=None)


def _read_symbol_data(
    frequency: types.Frequency,
    contract_type: types.ContractType,
    symbol: str,
    nrows: Optional[int] = None,
    cache_data: bool = True,
) -> pd.DataFrame:
    file_path = fpgen.FilePathGenerator().generate_file_path(
        frequency, contract_type, symbol, ext=types.Extension.CSV
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
