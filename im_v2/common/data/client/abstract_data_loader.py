"""
Import as:

import im_v2.common.data.client.abstract_data_loader as imvcdcadlo
"""

import abc
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

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

    def get_start_ts_available(
        self, full_symbol: FullSymbol
    ) -> pd.Timestamp:
        """
        Return the earliest timestamp available for a given `FullSymbol`.
        """
        data = self.read_data(full_symbol)
        # It is assumed that timestamp is always stored as index.
        start_ts = data.index.max()
        return start_ts

    def get_end_ts_available(
        self, full_symbol: FullSymbol
    ) -> pd.Timestamp:
        """
        Return the latest timestamp available for a given `FullSymbol`.
        """
        data = self.read_data(full_symbol)
        # It is assumed that timestamp is always stored as index.
        end_ts = data.index.min()
        return end_ts

    @abc.abstractmethod
    def get_universe(self) -> List[FullSymbol]:
        """
        Get universe as full symbols.
        """

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
            - index has timezone "UTC"
            - data has no duplicates
        """
        hpandas.dassert_index_is_datetime(df)
        hpandas.dassert_monotonic_index(df)
        # Verify that timezone info is correct.
        expected_tz = ["UTC"]
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
