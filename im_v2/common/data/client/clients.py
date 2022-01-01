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
import helpers.printing as hprint

_LOG = logging.getLogger(__name__)

# TODO(gp): Move to symbol.py or full_symbol.py

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
    Retrieve market data for different vendors from different backends.

    Invariants:
    - data is normalized so that the index is a UTC timestamp
    - data is resampled on a 1min grid
    - data has no duplicates
    """

    def read_data(
        self,
        full_symbols: List[FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read data in [start_ts, end_ts) for the symbols `FullSymbols`.

        # TODO: Add example of the data

        :param full_symbols: list of full symbols, e.g.
            `['binance::BTC_USDT', 'kucoin::ETH_USDT']`
        :param start_ts: the earliest date timestamp to load data for
            - `None` means start from the beginning of the available data
        :param end_ts: the latest date timestamp to load data for
            - `None` means end at the end of the available data
        :param full_symbol_col_name: name of the column storing the full
            symbols (e.g., `asset_id`)
        :return: combined data for all the requested symbols
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts full_symbol_col_name kwargs"
            )
        )
        self._check_full_symbols(full_symbols)
        df = self._read_data(
            full_symbols,
            start_ts,
            end_ts,
            full_symbol_col_name=full_symbol_col_name,
            **kwargs,
        )
        hdbg.dassert_in(full_symbol_col_name, df.columns)
        df.index.name = "timestamp"
        _LOG.debug("After _read_data: df=\n%s", hprint.dataframe_to_str(df))
        # Normalize data for each symbol.
        dfs = []
        for _, df_tmp in df.groupby(full_symbol_col_name):
            df_tmp = self._apply_im_normalizations(df_tmp, start_ts, end_ts)
            self._dassert_is_valid(df_tmp)
            dfs.append(df_tmp)
        df = pd.concat(dfs, axis=0)
        _LOG.debug("After im_normalization: df=\n%s", hprint.dataframe_to_str(df))
        # Sort by index and `full_symbol_col_name`.
        # There is not a simple way to sort by index and columns in Pandas,
        # so we convert the index into a column, sort.
        df = df.reset_index()
        df = df.sort_values(by=["timestamp", full_symbol_col_name])
        df = df.set_index("timestamp", drop=True)
        _LOG.debug("After sorting: df=\n%s", hprint.dataframe_to_str(df))
        return df

    def get_start_ts_for_symbol(self, full_symbol: FullSymbol) -> pd.Timestamp:
        """
        Return the earliest timestamp available for a given `FullSymbol`.

        This implementation relies on reading all the data and then finding the min.
        Derived classes can override this method if there is a more efficient
        way to get this information.
        """
        _LOG.debug(hprint.to_str("full_symbol"))
        # Read data for the entire period of time available.
        start_timestamp = None
        end_timestamp = None
        data = self.read_data([full_symbol], start_timestamp, end_timestamp)
        # Assume that the timestamp is always stored as index.
        start_ts = data.index.min()
        hdbg.dassert_isinstance(start_ts, pd.Timestamp)
        # TODO(gp): Check that is UTC.
        return start_ts

    def get_end_ts_for_symbol(self, full_symbol: FullSymbol) -> pd.Timestamp:
        """
        Return the latest timestamp available for a given `FullSymbol`.
        """
        _LOG.debug(hprint.to_str("full_symbol"))
        # Read data for the entire period of time available.
        start_timestamp = None
        end_timestamp = None
        data = self.read_data([full_symbol], start_timestamp, end_timestamp)
        # Assume that the timestamp is always stored as index.
        end_ts = data.index.max()
        hdbg.dassert_isinstance(end_ts, pd.Timestamp)
        # TODO(gp): Check that is UTC.
        return end_ts

    @staticmethod
    @abc.abstractmethod
    def get_universe() -> List[FullSymbol]:
        """
        Get universe as full symbols.
        """
        ...

    @abc.abstractmethod
    def _read_data(
        self,
        full_symbols: List[FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        ...

    @staticmethod
    def _check_full_symbols(full_symbols: List[FullSymbol]) -> None:
        hdbg.dassert_isinstance(full_symbols, list)
        hdbg.dassert_no_duplicates(full_symbols)

    @staticmethod
    def _apply_im_normalizations(
        df: pd.DataFrame,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        _LOG.debug(hprint.to_str("start_ts end_ts"))
        df = hpandas.drop_duplicates(df)
        df = hpandas.resample_df(df, "T")
        # Trim the data with index in [start_ts, end_ts].
        if False:
            # Instance of '2021-09-09T00:02:00.000000000' is '<class 'numpy.datetime64'>' instead of '(<class 'pandas._libs.tslibs.timestamps.Timestamp'>, <class 'datetime.datetime'>)'
            ts_col_name = None
            left_close = True
            right_close = True
            df = hpandas.trim_df(
                df, ts_col_name, start_ts, end_ts, left_close, right_close
            )
        return df

    @staticmethod
    @abc.abstractmethod
    def _apply_vendor_normalization(df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformation specific of the vendor, e.g. rename columns,
        convert data types.

        :param df: raw data
        :return: normalized data
        """
        ...

    @staticmethod
    def _dassert_is_valid(df: pd.DataFrame) -> None:
        """
        Verify that the normalized data is valid.
        """
        # Check that index is `pd.DatetimeIndex`.
        hpandas.dassert_index_is_datetime(df)
        # Check that index is monotonic increasing.
        hpandas.dassert_strictly_increasing_index(df)
        # Verify that index frequency is "T" (1 minute).
        hdbg.dassert_eq(df.index.freq, "T")
        # Check that timezone info is correct.
        expected_tz = ["UTC"]
        # Assume that the first value of an index is representative.
        hdateti.dassert_has_specified_tz(
            df.index[0],
            expected_tz,
        )
        # Check that there are no duplicates in the data.
        n_duplicated_rows = df.dropna(how="all").duplicated().sum()
        hdbg.dassert_eq(
            n_duplicated_rows, 0, msg="There are duplicated rows in the data"
        )


# #############################################################################
# ImClientReadingOneSymbol
# #############################################################################


class ImClientReadingOneSymbol(AbstractImClient, abc.ABC):
    """
    Abstract IM client for a backend that can read one symbol at a time.
    """

    def _read_data(
        self,
        full_symbols: List[FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Same as the method in the parent class.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts full_symbol_col_name kwargs"
            )
        )
        # Load the data for each symbol.
        full_symbol_to_df = {}
        for full_symbol in sorted(full_symbols):
            df = self._read_data_for_one_symbol(
                full_symbol,
                start_ts,
                end_ts,
                **kwargs,
            )
            # Normalize data.
            df = self._apply_vendor_normalization(df)
            # Insert column with full symbol to the dataframe.
            hdbg.dassert_is_not(full_symbol_col_name, df.columns)
            df.insert(0, full_symbol_col_name, full_symbol)
            # Add full symbol data to the results dict.
            full_symbol_to_df[full_symbol] = df
        # Combine results dict in a dataframe.
        df = pd.concat(full_symbol_to_df.values())
        return df

    @abc.abstractmethod
    def _read_data_for_one_symbol(
        self,
        full_symbol: FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read data for a single `FullSymbol` in [start_ts, end_ts).

        Parameters have the same meaning as parameters in `read_data()` with
        the same name.
        """
        ...


# #############################################################################
# ImClientReadingMultipleSymbols
# #############################################################################


class ImClientReadingMultipleSymbols(AbstractImClient, abc.ABC):
    """
    Abstract IM client for backend that can read multiple symbols at the same
    time.

    This is used for reading data from Parquet by-date files, where multiple
    assets are stored in the same file and can be accessed together.
    """

    def _read_data(
        self,
        full_symbols: List[FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Same as the abstract class.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts full_symbol_col_name kwargs"
            )
        )
        df = self._read_data_for_multiple_symbols(
            full_symbols, start_ts, end_ts, full_symbol_col_name, **kwargs
        )
        df = self._apply_vendor_normalization(df)
        return df

    @abc.abstractmethod
    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Derived classes need to implement this.
        """
        ...
