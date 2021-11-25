"""
Import as:

import im_v2.common.data.client.abstract_data_loader as imvcdcadlo
"""

import abc
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)

# Store information about an exchange and a symbol (e.g., `binance::BTC_USDT`).
# Note that information about the vendor is carried in the `ImClient` itself,
# i.e. using `CcxtImClient` serves data from CCXT.
FullSymbol = str


def dassert_is_full_symbol_valid(full_symbol: FullSymbol) -> None:
    """
    Check that full symbol has valid format, i.e. `exchange::symbol`.

    Note: digits and special symbols (except underscore) are not allowed.
    """
    hdbg.dassert_isinstance(full_symbol, str)
    hdbg.dassert_ne(full_symbol, "")
    # Only letters and underscores are allowed.
    letter_underscore_pattern = "[a-zA-Z_]"
    # Exchanges and symbols must be separated by `::`.
    regex_pattern = fr"{letter_underscore_pattern}*::{letter_underscore_pattern}*"
    # Input full symbol must exactly match the pattern.
    full_match = re.fullmatch(regex_pattern, full_symbol, re.IGNORECASE)
    hdbg.dassert(
        full_match,
        msg=f"Incorrect full_symbol format {full_symbol}, must be `exchange::symbol`"
    )


def parse_full_symbol(full_symbol: FullSymbol) -> Tuple[str, str]:
    """
    Split a full_symbol into exchange and symbol.

    :return: exchange, symbol
    """
    dassert_is_full_symbol_valid(full_symbol)
    exchange, symbol = full_symbol.split("::")
    return exchange, symbol


def construct_full_symbol(exchange: str, symbol: str) -> FullSymbol:
    """
    Combine exchange and symbol in `FullSymbol`.
    """
    hdbg.dassert_isinstance(exchange, str)
    hdbg.dassert_ne(exchange, "")
    #
    hdbg.dassert_isinstance(symbol, str)
    hdbg.dassert_ne(symbol, "")
    #
    full_symbol = f"{exchange}::{symbol}"
    dassert_is_full_symbol_valid(full_symbol)
    return full_symbol


# TODO(Grisha): add methods `get_start(end)_ts_available()`, `get_universe()` #543.
class AbstractImClient(abc.ABC):
    """
    Abstract Interface for `IM` client.
    """
    def read_data(
        self,
        full_symbol: FullSymbol,
        *,
        normalize: bool = True,
        drop_duplicates: bool = True,
        resample_to_1_min: bool = True,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read and process data for a single `FullSymbol` (i.e. currency pair
        from a single exchange) in [start_ts, end_ts).

        None `start_ts` and `end_ts` means the entire period of time available.

        Data processing includes:
            - normalization specific of the vendor
            - dropping duplicates
            - resampling to 1 minute
            - sanity check of the data

        :param full_symbol: `exchange::symbol`, e.g. `binance::BTC_USDT`
        :param normalize: transform data, e.g. rename columns, convert data types
        :param drop_duplicates: whether to drop full duplicates or not
        :param resample_to_1_min: whether to resample to 1 min or not
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: data for a single `FullSymbol` in [start_ts, end_ts)
        """
        data = self._read_data(
            full_symbol, start_ts=start_ts, end_ts=end_ts, **kwargs
        )
        if normalize:
            data = self._normalize_data(data)
        if drop_duplicates:
            data = hpandas.drop_duplicates(data)
        if resample_to_1_min:
            data = hpandas.resample_df(data, "T")
        # Verify that data is valid.
        self._dassert_is_valid(data)
        return data

    @abc.abstractmethod
    def _read_data(
        self,
        full_symbol: FullSymbol,
        *,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read data for a single `FullSymbol` (i.e. currency pair from a single
        exchange) in [start_ts, end_ts).

        None `start_ts` and `end_ts` means the entire period of time available.

        :param full_symbol: `exchange::symbol`, e.g. `binance::BTC_USDT`
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: data for a single `FullSymbol` in [start_ts, end_ts)
        """

    @staticmethod
    @abc.abstractmethod
    def _normalize_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformation specific of the vendor, e.g. rename columns,
        convert data types.

        :param df: raw data
        :return: normalized data
        """

    @staticmethod
    def _dassert_is_valid(df: pd.DataFrame) -> None:
        """
        Verify that data is valid.

        Sanity checks include:
            - index is `pd.DatetimeIndex`
            - index is monotonic increasing/decreasing
            - index has timezone "US/Eastern"
            - data has no duplicates
        """
        hpandas.dassert_index_is_datetime(df)
        hpandas.dassert_monotonic_index(df)
        # Verify that timezone info is correct.
        # TODO(Grisha): make everything in `UTC` #580.
        expected_tz = ["America/New_York", "US/Eastern"]
        # Is is assumed that the 1st value of an index is representative.
        hdateti.dassert_has_specified_tz(
            df.index[0],
            expected_tz,
        )
        # Verify that there are no duplicates in data. We do not want to
        # consider missing rows that appear due to resampling duplicated.
        n_duplicated_rows = df.dropna().duplicated().sum()
        hdbg.dassert_eq(
            n_duplicated_rows,
            0,
            msg=f"There are {n_duplicated_rows} duplicated rows in data",
        )


class MultipleSymbolsClient(AbstractImClient):
    """
    Object compatible with `AbstractImClient` interface which reads data for multiple full symbols.
    """
    def __init__(self, class_: AbstractImClient, mode: str) -> None:
        """
        :param class_: `AbstractImClient` object
        :param mode: output mode
            - concat: store data for multiple full symbols in one dataframe
            - dict: store data in a dict of a type `Dict[full_symbol, data]`
        """
        # Store an object from `AbstractImClient`.
        self._class = class_
        # Specify output mode.
        hdbg.dassert_in(mode, ("concat", "dict"))
        self._mode = mode

    @abc.abstractmethod
    def read_data(
        self,
        full_symbols: Union[str, List[str]],
        *,
        full_symbol_col_name: str = "full_symbol",
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Dict[str, Any],
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read data for multiple full symbols or a specified universe version.

        None `start_ts` and `end_ts` means the entire period of time available.

        :param full_symbols: universe version or a list of full symbols, e.g.
            `['binance::BTC_USDT', 'kucoin::ETH_USDT']`
        :param full_symbol_col_name: name of the column with full symbols
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: combined data for provided symbols or universe version
        """
        # TODO(Dan): Implement the case when `full_symbols` is string, e.g."v01".
        # Verify that all the provided full symbols are unique.
        hdbg.dassert_no_duplicates(full_symbols)
        # Initialize results dict.
        full_symbol_to_df = {}
        for full_symbol in sorted(full_symbols):
            # Read data for each given full symbol.
            df = self._class.read_data(
                full_symbol=full_symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                **kwargs,
            )
            # Insert column with full symbol to the dataframe.
            df.insert(0, full_symbol_col_name, full_symbol)
            # Add full symbol data to the results dict.
            full_symbol_to_df[full_symbol] = df
        if self._mode == "concat":
            # Combine results dict in a dataframe if specified.
            ret = pd.concat(full_symbol_to_df.values())
            # Sort results dataframe by increasing index and full symbol.
            ret = ret.sort_index().sort_values(by=full_symbol_col_name)
        elif self._mode == "dict":
            # Return results dict if specified.
            ret = full_symbol_to_df
        else:
            raise ValueError("Invalid mode=%s", self._mode)
        return ret

    # TODO(Grisha/Dan): Decide if we want to also implement other methods of the base class.
    # TODO(Grisha/Dan): Decide if we want to add get_start(end)_ts_available() methods.
