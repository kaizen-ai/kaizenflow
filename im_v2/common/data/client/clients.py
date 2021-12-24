"""
Import as:

import im_v2.common.data.client.clients as ivcdclcl
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
    # TODO(gp): I think we might need non-leading numbers.
    letter_underscore_pattern = "[a-zA-Z_]"
    # Exchanges and symbols must be separated by `::`.
    regex_pattern = fr"{letter_underscore_pattern}*::{letter_underscore_pattern}*"
    # Input full symbol must exactly match the pattern.
    full_match = re.fullmatch(regex_pattern, full_symbol, re.IGNORECASE)
    hdbg.dassert(
        full_match,
        msg=f"Incorrect full_symbol format {full_symbol}, must be `exchange::symbol`",
    )


def parse_full_symbol(full_symbol: FullSymbol) -> Tuple[str, str]:
    """
    Split a full_symbol into a tuple of exchange and symbol.

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


# #############################################################################
# AbstractImClient
# #############################################################################


class AbstractImClient(abc.ABC):
    """
    Abstract Interface for `IM` client.

    Clients derived from `AbstractImClient` read data for a single
    `FullSymbol`, to read data for multiple `FullSymbols` use
    `MultipleSymbolsImClient`.
    """
    def read_data(
        self,
        full_symbols: List[FullSymbol],
        *,
        mode: str = "concat",
        full_symbol_col_name: str = "full_symbol",
        normalize: bool = True,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Dict[str, Any],
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read data for multiple full symbols.

        None `start_ts` and `end_ts` means the entire period of time available.

        :param full_symbols: list of full symbols, e.g.
            `['binance::BTC_USDT', 'kucoin::ETH_USDT']`
        :param mode: output mode
            - concat: store data for multiple full symbols in one dataframe
            - dict: store data in a dict of a type `Dict[full_symbol, data]`
        :param full_symbol_col_name: name of the column with full symbols
        :param normalize: whether to transform data or not
        :param start_ts: the earliest date timestamp to load data for
        :param end_ts: the latest date timestamp to load data for
        :return: combined data for provided symbols
        """
        # Verify that all the provided full symbols are unique.
        hdbg.dassert_no_duplicates(full_symbols)
        # Initialize results dict.
        full_symbol_to_df = {}
        for full_symbol in sorted(full_symbols):
            cur_df = self._read_data_for_single_full_symbol(
            # Insert column with full symbol to the dataframe.
            df.insert(0, full_symbol_col_name, full_symbol)
            # Add full symbol data to the results dict.
            full_symbol_to_df[full_symbol] = df
        if mode == "concat":
            # Combine results dict in a dataframe if specified.
            ret = pd.concat(full_symbol_to_df.values())
            # Sort results dataframe by increasing index and full symbol.
            ret = ret.sort_index().sort_values(by=full_symbol_col_name)
        elif mode == "dict":
            # Return results dict if specified.
            ret = full_symbol_to_df
        else:
            raise ValueError(f"Invalid mode=`{mode}`")
        return ret

    def get_start_ts_available(self, full_symbol: FullSymbol) -> pd.Timestamp:
        """
        Return the earliest timestamp available for a given `FullSymbol`.
        """
        # TODO(Grisha): add caching.
        normalize = True
        data = self._read_data_for_single_full_symbol(full_symbol, normalize)
        # It is assumed that timestamp is always stored as index.
        start_ts = data.index.min()
        return start_ts

    def get_end_ts_available(self, full_symbol: FullSymbol) -> pd.Timestamp:
        """
        Return the latest timestamp available for a given `FullSymbol`.
        """
        # TODO(Grisha): add caching.
        normalize = True
        data = self._read_data_for_single_full_symbol(full_symbol, normalize)
        # It is assumed that timestamp is always stored as index.
        end_ts = data.index.max()
        return end_ts

    @staticmethod
    @abc.abstractmethod
    def get_universe() -> List[FullSymbol]:
        """
        Get universe as full symbols.
        """

    def _read_data_for_single_full_symbol(
        self,
        full_symbol: FullSymbol,
        normalize: bool,
        *,
        start_ts: Optional[pd.Timestamp] = None,
        end_ts: Optional[pd.Timestamp] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        # Read data for each given full symbol.
        df = self._read_data(
            full_symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            **kwargs,
        )
        if normalize:
            df = self._normalize_data(df)
            df = hpandas.drop_duplicates(df)
            df = hpandas.resample_df(df, "T")
            # Verify that data is valid.
            self._dassert_is_valid(df)
        return df

    @abc.abstractmethod
    def _read_data(
        self,
        full_symbol: FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Read data for a single `FullSymbol` (i.e. currency pair from a single
        exchange) in [start_ts, end_ts).

        Parameters have the same meaning as parameters in `read_data()`
        with the same name.
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
        Verify that normalized data is valid.

        Sanity checks include:
            - index is `pd.DatetimeIndex`
            - index is monotonic increasing/decreasing
            - index frequency is "T" (1 minute)
            - index has timezone "UTC"
            - data has no duplicates
        """
        hpandas.dassert_index_is_datetime(df)
        hpandas.dassert_strictly_increasing_index(df)
        # Verify that index frequency is "T" (1 minute).
        hdbg.dassert_eq(df.index.freq, "T")
        # Verify that timezone info is correct.
        expected_tz = ["UTC"]
        # It is assumed that the 1st value of an index is representative.
        hdateti.dassert_has_specified_tz(
            df.index[0],
            expected_tz,
        )
        # Verify that there are no duplicates in the data.
        n_duplicated_rows = df.dropna(how="all").duplicated().sum()
        hdbg.dassert_eq(
            n_duplicated_rows,
            0,
            msg=f"There are {n_duplicated_rows} duplicated rows in data",
        )

