import functools
import os
from typing import Dict, Optional, Tuple, Union

import pandas as pd

import helpers.cache as cache
import helpers.s3 as hs3
from kibot2.data.transform import _get_normalizer


class KibotDataLoader:
    @functools.lru_cache(maxsize=None)
    @staticmethod
    def read_data(
        frequency: str,
        contract_type: str,
        symbols: Union[str, Tuple[str, ...]],
        ext: str = "pq",
        nrows: Optional[int] = None,
        cache_data: bool = True,
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Read kibot data.

        If the ext is `csv`, this function will
        - parse dates
        - add column names
        - check for monotonic index
        For pq data, all of those transformations have already been made,
        so this function reads the data without modifying it.

        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: `continuous` or `expiry`
        :param symbols: symbol or tuple of symbols
        :param ext: whether to read `pq` or `csv` data
        :param nrows: if not None, return only the first nrows of the data
        :param cache_data: whether to use cached data if exists
        :return: if `symbols` is a string, return pd.DataFrame with kibot
            data. If `symbols` is a list, return a dictionary of dataframes
            for each symbol.
        """
        if isinstance(symbols, str):
            data = _read_single_symbol_data(
                frequency, contract_type, symbols, ext, nrows, cache_data
            )
        elif isinstance(symbols, tuple):
            data = _read_multiple_symbol_data(
                frequency, contract_type, symbols, ext, nrows, cache_data
            )
        else:
            raise ValueError("Invalid type(symbols)=%s" % type(symbols))
        return data


MEMORY = cache.get_disk_cache(tag=None)


def _get_kibot_path(
    frequency: str, contract_type: str, symbol: str, ext: str = "pq"
) -> str:
    """Get the path to a specific kibot dataset on s3.

    Parameters as in `read_data`.
    :return: path to the file
    """
    if frequency == "T":
        freq_path = "1min"
    elif frequency == "D":
        freq_path = "daily"
    else:
        raise ValueError("Invalid frequency='%s'" % frequency)
    if contract_type == "continuous":
        contract_path = "_Continuous"
    elif contract_type == "expiry":
        contract_path = ""
    else:
        raise ValueError("Invalid contract_type='%s'" % contract_type)
    dir_name = f"All_Futures{contract_path}_Contracts_{freq_path}"
    file_path = os.path.join(dir_name, symbol)
    if ext == "pq":
        # Parquet files are located in `pq/` subdirectory.
        file_path = os.path.join("pq", file_path)
        file_path += ".pq"
    elif ext == "csv":
        file_path += ".csv.gz"
    else:
        raise ValueError("Invalid ext='%s" % ext)
    file_path = os.path.join(hs3.get_path(), "kibot", file_path)
    return file_path


def _read_multiple_symbol_data(
    frequency: str,
    contract_type: str,
    symbols: Tuple[str, ...],
    ext: str = "pq",
    nrows: Optional[int] = None,
    cache_data: bool = True,
) -> Dict[str, pd.DataFrame]:
    return {
        symbol: _read_single_symbol_data(
            frequency, contract_type, symbol, ext, nrows, cache_data
        )
        for symbol in symbols
    }


def _read_single_symbol_data(
    frequency: str,
    contract_type: str,
    symbol: str,
    ext: str = "pq",
    nrows: Optional[int] = None,
    cache_data: bool = True,
) -> pd.DataFrame:
    file_path = _get_kibot_path(frequency, contract_type, symbol, ext)
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
    ext = _split_multiple_ext(file_path)[-1]
    if ext == ".pq":
        # Read the data.
        df = pd.read_parquet(file_path)
        if nrows is not None:
            df = df.head(nrows)
    elif ext == ".csv.gz":
        # Read and normalize the data.
        # In the parquet flow we have already applied all the
        # transformations, while in the csv.gz flow we apply the
        # transformation on the raw data that comes from Kibot.
        df = pd.read_csv(file_path, header=None, nrows=nrows)
        dir_name = os.path.basename(os.path.dirname(file_path))
        normalizer = _get_normalizer(dir_name)
        if normalizer is not None:
            df = normalizer(df)
        else:
            raise ValueError(
                "Invalid dir_name='%s' in file_name='%s'" % (dir_name, file_path)
            )
    else:
        raise ValueError("Invalid extension='%s'" % ext)
    return df


# TODO(J): Move this function to `helpers.io_`.
def _split_multiple_ext(file_name: str) -> Tuple[str, str]:
    """Split file name into root and extension. Extension is everything after
    the first dot, ignoring the leading dot.

    The difference with `os.path.splitext` is that this function assumes
    that extension starts after the first dot, not the last. Therefore,
    if `file_name='file.csv.gz'`, this function will extract `.csv.gz`
    extension, while `os.path.splitext` will extract only `.gz`.

    :param file_name: the name of the file
    :return: root of the file name and extension
    """
    file_name_without_ext = file_name
    while True:
        file_name_without_ext, ext = os.path.splitext(file_name_without_ext)
        if ext == "":
            break
    full_ext = file_name.replace(file_name_without_ext, "", 1)
    return file_name_without_ext, full_ext
