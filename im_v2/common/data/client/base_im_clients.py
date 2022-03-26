"""
Import as:

import im_v2.common.data.client.base_im_clients as imvcdcbimcl
"""

import abc
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.data.client.full_symbol as imvcdcfusy
import im_v2.common.universe.universe_utils as imvcuunut

_LOG = logging.getLogger(__name__)

# #############################################################################
# ImClient
# #############################################################################

# TODO(gp): Consider splitting in one file per class. Not sure about the trade-off
#  between file proliferation and more organization.

# TODO(gp): The output of ImClient should be in the form of `start_timestamp`,
#  `end_timestamp`, and `knowledge_timestamp` since these depend on the specific
#  data source. @Grisha let's do this, but let's schedule a clean up later and
#  not right now


class ImClient(abc.ABC):
    """
    Retrieve market data for different vendors and backends.

    The data in output of a class derived from `ImClient` is normalized so that:
    - the index:
      - represents the knowledge time
      - is the end of the sampling interval
      - is called `timestamp`
      - is a tz-aware timestamp in UTC
    - the data:
      - is resampled on a 1 minute grid and filled with NaN values
      - is sorted by index and `full_symbol`
      - is guaranteed to have no duplicates
      - belongs to intervals like [a, b]
      - has a `full_symbol` column with a string representing the canonical name
        of the instrument

    E.g.,
    ```
                                    full_symbol     close     volume
                    timestamp
    2021-07-26 13:42:00+00:00  binance:BTC_USDT  47063.51  29.403690
    2021-07-26 13:43:00+00:00  binance:BTC_USDT  46946.30  58.246946
    2021-07-26 13:44:00+00:00  binance:BTC_USDT  46895.39  81.264098
    ```
    """

    def __init__(self, vendor: str, resample_1min: bool) -> None:
        """
        Constructor.

        :param vendor: price data provider
        :param resample_1min: whether to resample data to 1 minute or not
        """
        self._vendor = vendor
        self._resample_1min = resample_1min
        self._asset_id_to_full_symbol_mapping = (
            self._build_asset_id_to_full_symbol_mapping()
        )

    @staticmethod
    @abc.abstractmethod
    def get_universe() -> List[imvcdcfusy.FullSymbol]:
        """
        Return the entire universe of valid full symbols.
        """

    @staticmethod
    @abc.abstractmethod
    def get_metadata() -> pd.DataFrame:
        """
        Return metadata.
        """

    @staticmethod
    def get_asset_ids_from_full_symbols(
        full_symbols: List[imvcdcfusy.FullSymbol],
    ) -> List[int]:
        """
        Convert full symbols into asset ids.

        :param full_symbols: assets as full symbols
        :return: assets as numerical ids
        """
        numerical_asset_id = [
            imvcuunut.string_to_numerical_id(full_symbol)
            for full_symbol in full_symbols
        ]
        return numerical_asset_id

    def read_data(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read data in `[start_ts, end_ts]` for `imvcdcfusy.FullSymbol` symbols.

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
        # Verify the requested parameters.
        imvcdcfusy.dassert_valid_full_symbols(full_symbols)
        #
        left_close = True
        right_close = True
        hdateti.dassert_is_valid_interval(
            start_ts, end_ts, left_close, right_close
        )
        # Delegate to the derived classes to retrieve the data.
        df = self._read_data(
            full_symbols,
            start_ts,
            end_ts,
            full_symbol_col_name=full_symbol_col_name,
            **kwargs,
        )
        _LOG.debug("After read_data: df=\n%s", hpandas.df_to_str(df, num_rows=3))
        # Check that we got what we asked for.
        # hpandas.dassert_increasing_index(df)
        #
        hdbg.dassert_in(full_symbol_col_name, df.columns)
        loaded_full_symbols = df[full_symbol_col_name].unique().tolist()
        imvcdcfusy.dassert_valid_full_symbols(loaded_full_symbols)
        hdbg.dassert_set_eq(
            full_symbols,
            loaded_full_symbols,
            msg="Not all the requested symbols were retrieved",
            only_warning=True,
        )
        #
        hdateti.dassert_timestamp_lte(start_ts, df.index.min())
        hdateti.dassert_timestamp_lte(df.index.max(), end_ts)
        # Rename index.
        df.index.name = "timestamp"
        # Normalize data for each symbol.
        _LOG.debug("full_symbols=%s", df[full_symbol_col_name].unique())
        dfs = []
        for full_symbol, df_tmp in df.groupby(full_symbol_col_name):
            _LOG.debug("apply_im_normalization: full_symbol=%s", full_symbol)
            df_tmp = self._apply_im_normalizations(
                df_tmp,
                full_symbol_col_name,
                self._resample_1min,
                start_ts,
                end_ts,
            )
            self._dassert_output_data_is_valid(
                df_tmp,
                full_symbol_col_name,
                self._resample_1min,
                start_ts,
                end_ts,
            )
            dfs.append(df_tmp)
        # TODO(Nikola): raise error on empty df?
        df = pd.concat(dfs, axis=0)
        _LOG.debug("After im_normalization: df=\n%s", hpandas.df_to_str(df))
        # Sort by index and `full_symbol_col_name`.
        # There is not a simple way to sort by index and columns in Pandas,
        # so we convert the index into a column, sort, and convert back.
        df = df.reset_index()
        df = df.sort_values(by=["timestamp", full_symbol_col_name])
        df = df.set_index("timestamp", drop=True)
        _LOG.debug("After sorting: df=\n%s", hpandas.df_to_str(df))
        return df

    # /////////////////////////////////////////////////////////////////////////

    def get_start_ts_for_symbol(
        self, full_symbol: imvcdcfusy.FullSymbol
    ) -> pd.Timestamp:
        """
        Return the earliest timestamp available for a given `full_symbol`.

        This implementation relies on reading all the data and then
        finding the min. Derived classes can override this method if
        there is a more efficient way to get this information.
        """
        mode = "start"
        return self._get_start_end_ts_for_symbol(full_symbol, mode)

    def get_end_ts_for_symbol(
        self, full_symbol: imvcdcfusy.FullSymbol
    ) -> pd.Timestamp:
        """
        Same as `get_start_ts_for_symbol()`.
        """
        mode = "end"
        return self._get_start_end_ts_for_symbol(full_symbol, mode)

    def get_full_symbols_from_asset_ids(
        self, asset_ids: List[int]
    ) -> List[imvcdcfusy.FullSymbol]:
        """
        Convert asset ids into full symbols.

        :param asset_ids: assets ids
        :return: assets as full symbols
        """
        # Check that provided ids are part of the universe.
        hdbg.dassert_is_subset(asset_ids, self._asset_id_to_full_symbol_mapping)
        # Convert ids to full symbols.
        full_symbols = [
            self._asset_id_to_full_symbol_mapping[asset_id]
            for asset_id in asset_ids
        ]
        return full_symbols

    # /////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _apply_im_normalizations(
        df: pd.DataFrame,
        full_symbol_col_name: str,
        resample_1min: bool,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
    ) -> pd.DataFrame:
        """
        Apply normalizations to IM data.
        """
        _LOG.debug(hprint.to_str("full_symbol_col_name start_ts end_ts"))
        hdbg.dassert(not df.empty, "Empty df=\n%s", df)
        # 1) Drop duplicates.
        df = hpandas.drop_duplicates(df)
        # 2) Trim the data keeping only the data with index in [start_ts, end_ts].
        # Trimming of the data is done because:
        # - some data sources can be only queried at day resolution so we get
        #   a date range and then we trim
        # - we want to guarantee that no derived class returns data outside the
        #   requested interval
        ts_col_name = None
        left_close = True
        right_close = True
        df = hpandas.trim_df(
            df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        # 3) Resample index to 1 min frequency if specified.
        if resample_1min:
            df = hpandas.resample_df(df, "T")
            # Fill NaN values appeared after resampling in full symbol column.
            # Combination of full symbol and timestamp is a unique identifier,
            # so full symbol cannot be NaN.
            df[full_symbol_col_name] = df[full_symbol_col_name].fillna(
                method="bfill"
            )
        # 4) Convert to UTC.
        df.index = df.index.tz_convert("UTC")
        return df

    @staticmethod
    def _dassert_output_data_is_valid(
        df: pd.DataFrame,
        full_symbol_col_name: str,
        resample_1min: bool,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
    ) -> None:
        """
        Verify that the normalized data is valid.
        """
        # Check that index is `pd.DatetimeIndex`.
        hpandas.dassert_index_is_datetime(df)
        if resample_1min:
            # Check that index is monotonic increasing.
            hpandas.dassert_strictly_increasing_index(df)
            # Verify that index frequency is 1 minute.
            hdbg.dassert_eq(df.index.freq, "T")
        # Check that timezone info is correct.
        expected_tz = ["UTC"]
        # Assume that the first value of an index is representative.
        hdateti.dassert_has_specified_tz(
            df.index[0],
            expected_tz,
        )
        # Check that full symbol column has no NaNs.
        hdbg.dassert(df[full_symbol_col_name].notna().all())
        # Check that there are no duplicates in data by index and full symbol.
        n_duplicated_rows = (
            df.reset_index()
            .duplicated(subset=["timestamp", full_symbol_col_name])
            .sum()
        )
        hdbg.dassert_eq(
            n_duplicated_rows, 0, msg="There are duplicated rows in the data"
        )
        # Ensure that all the data is in [start_ts, end_ts].
        if start_ts:
            hdbg.dassert_lte(start_ts, df.index.min())
        if end_ts:
            hdbg.dassert_lte(df.index.max(), end_ts)

    # //////////////////////////////////////////////////////////////////////////

    @abc.abstractmethod
    def _read_data(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        ...

    def _build_asset_id_to_full_symbol_mapping(self) -> Dict[int, str]:
        """
        Build asset id to full symbol mapping.
        """
        # Get full symbol universe.
        full_symbol_universe = self.get_universe()
        # Build the mapping.
        asset_id_to_full_symbol_mapping = (
            imvcuunut.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        return asset_id_to_full_symbol_mapping  # type: ignore[no-any-return]

    def _get_start_end_ts_for_symbol(
        self, full_symbol: imvcdcfusy.FullSymbol, mode: str
    ) -> pd.Timestamp:
        _LOG.debug(hprint.to_str("full_symbol"))
        # Read data for the entire period of time available.
        start_timestamp = None
        end_timestamp = None
        data = self.read_data([full_symbol], start_timestamp, end_timestamp)
        # Assume that the timestamp is always stored as index.
        if mode == "start":
            timestamp = data.index.min()
        elif mode == "end":
            timestamp = data.index.max()
        else:
            raise ValueError("Invalid mode='%s'" % mode)
        #
        hdbg.dassert_isinstance(timestamp, pd.Timestamp)
        hdateti.dassert_has_specified_tz(timestamp, ["UTC"])
        return timestamp


# #############################################################################
# ImClientReadingOneSymbol
# #############################################################################


class ImClientReadingOneSymbol(ImClient, abc.ABC):
    """
    IM client for a backend that can only read one symbol at a time.

    E.g., CSV with data organized by-asset.
    """

    def _read_data(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
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
        hdbg.dassert_container_type(full_symbols, list, str)
        # Check timestamp interval.
        left_close = True
        right_close = True
        hdateti.dassert_is_valid_interval(
            start_ts,
            end_ts,
            left_close=left_close,
            right_close=right_close,
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
            # Insert column with full symbol into the result dataframe.
            hdbg.dassert_is_not(full_symbol_col_name, df.columns)
            df.insert(0, full_symbol_col_name, full_symbol)
            # Add data to the result dict.
            full_symbol_to_df[full_symbol] = df
        # Combine results dict in a dataframe.
        df = pd.concat(full_symbol_to_df.values())
        # We rely on the parent class to sort.
        return df

    @abc.abstractmethod
    def _read_data_for_one_symbol(
        self,
        full_symbol: imvcdcfusy.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Read data for a single symbol in [start_ts, end_ts].

        Parameters have the same meaning as in `read_data()`.
        """
        ...


# #############################################################################
# ImClientReadingMultipleSymbols
# #############################################################################


class ImClientReadingMultipleSymbols(ImClient, abc.ABC):
    """
    IM client for backend that can read multiple symbols at the same time.

    E.g., Parquet by-date or by-asset files allow to read data for
    multiple assets stored in the same file.
    """

    def _read_data(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Same as the parent class.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts full_symbol_col_name kwargs"
            )
        )
        df = self._read_data_for_multiple_symbols(
            full_symbols, start_ts, end_ts, full_symbol_col_name, **kwargs
        )
        return df

    @abc.abstractmethod
    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        ...
