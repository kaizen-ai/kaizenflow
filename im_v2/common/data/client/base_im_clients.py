"""
Import as:

import im_v2.common.data.client.base_im_clients as imvcdcbimcl
"""

import abc
import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import core.finance.bid_ask as cfibiask
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.common.universe as ivcu

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

    def __init__(
        self,
        vendor: str,
        universe_version: Optional[str],
        resample_1min: bool,
        *,
        full_symbol_col_name: Optional[str] = None,
        timestamp_col_name: str = "timestamp",
    ) -> None:
        """
        Constructor.

        :param vendor: price data provider
        :param universe_version: version of universe file
        :param resample_1min: whether to resample data to 1 minute or not
        :param full_symbol_col_name: the name of the column storing the symbol
            name. It can be overridden by other methods
        :param timestamp_col_name: the name of the column storing timestamp
        """
        _LOG.debug(
            hprint.to_str(
                "vendor universe_version resample_1min full_symbol_col_name timestamp_col_name"
            )
        )
        hdbg.dassert_isinstance(vendor, str)
        self._vendor = vendor
        if universe_version is not None:
            hdbg.dassert_isinstance(universe_version, str)
        self._universe_version = universe_version
        hdbg.dassert_isinstance(resample_1min, bool)
        self._resample_1min = resample_1min
        hdbg.dassert_isinstance(timestamp_col_name, str)
        self._timestamp_col_name = timestamp_col_name
        # TODO(gp): This is the name of the column of the asset_id in the data
        #  as it is read by the derived classes (e.g., `igid`, `asset_id`).
        #  We should rename this as "full_symbol" so that all the code downstream
        #  knows how to call it and / or we can add a column_remap.
        if full_symbol_col_name is not None:
            hdbg.dassert_isinstance(full_symbol_col_name, str)
        self._full_symbol_col_name = full_symbol_col_name
        #
        self._asset_id_to_full_symbol_mapping = (
            self._build_asset_id_to_full_symbol_mapping()
        )

    # TODO(gp): Why static?
    @staticmethod
    @abc.abstractmethod
    def get_metadata() -> pd.DataFrame:
        """
        Return metadata.
        """

    @staticmethod
    def get_asset_ids_from_full_symbols(
        full_symbols: List[ivcu.FullSymbol],
    ) -> List[int]:
        """
        Convert full symbols into asset ids.

        :param full_symbols: assets as full symbols
        :return: assets as numerical ids
        """
        hdbg.dassert_container_type(full_symbols, list, ivcu.FullSymbol)
        numerical_asset_id = [
            ivcu.string_to_numerical_id(full_symbol)
            for full_symbol in full_symbols
        ]
        return numerical_asset_id

    def get_universe(self) -> List[ivcu.FullSymbol]:
        """
        Return the entire universe of valid full symbols.
        """
        # We use only `trade` universe for `ImClient`.
        universe_mode = "trade"
        universe = ivcu.get_vendor_universe(
            self._vendor,
            universe_mode,
            version=self._universe_version,
            as_full_symbol=True,
        )
        return universe  # type: ignore[no-any-return]

    def read_data(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        filter_data_mode: str,
        *,
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Read data in `[start_ts, end_ts]` for `ivcu.FullSymbol` symbols.

        :param full_symbols: list of full symbols, e.g.
            `['binance::BTC_USDT', 'kucoin::ETH_USDT']`
        :param start_ts: the earliest date timestamp to load data for
            - `None` means start from the beginning of the available data
        :param end_ts: the latest date timestamp to load data for
            - `None` means end at the end of the available data
        :param columns: columns to return, skipping reading columns that are not requested
            - `None` means return all available columns
        :param filter_data_mode: control class behavior with respect to extra
            or missing columns, like in `hpandas.check_and_filter_matching_columns()`
        :param full_symbol_col_name: name of the column storing the full
            symbols (e.g., `asset_id`)
        :return: combined data for all the requested symbols
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts columns full_symbol_col_name kwargs"
            )
        )
        # Verify the requested parameters.
        ivcu.dassert_valid_full_symbols(full_symbols)
        #
        left_close = True
        right_close = True
        hdateti.dassert_is_valid_interval(
            start_ts, end_ts, left_close, right_close
        )
        # Delegate to the derived classes to retrieve the data.
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        if columns is not None:
            # Check before reading the data.
            hdbg.dassert_container_type(columns, list, str)
            hdbg.dassert_lte(1, len(columns))
        df = self._read_data(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            full_symbol_col_name=full_symbol_col_name,
            **kwargs,
        )
        _LOG.debug("After read_data: df=\n%s", hpandas.df_to_str(df, num_rows=3))
        # Check that we got what we asked for.
        # hpandas.dassert_increasing_index(df)
        if "level" in df.columns:
            _LOG.debug(
                "Detected level column and calling handle_orderbook_levels"
            )
            # Transform bid ask data with multiple order book levels.
            timestamp_col = self._timestamp_col_name
            df = cfibiask.handle_orderbook_levels(df, timestamp_col)
        #
        hdbg.dassert_in(full_symbol_col_name, df.columns)
        loaded_full_symbols = df[full_symbol_col_name].unique().tolist()
        ivcu.dassert_valid_full_symbols(loaded_full_symbols)
        hdbg.dassert_set_eq(
            full_symbols,
            loaded_full_symbols,
            msg="Not all the requested symbols were retrieved",
            # TODO(Grisha): add param `assert_on_missing_asset_ids` that
            # allows to either assert or issues a warning.
            only_warning=True,
        )
        # Rename index.
        df.index.name = self._timestamp_col_name
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
                self._timestamp_col_name,
            )
            dfs.append(df_tmp)
        hdbg.dassert_lt(0, df.shape[0], "Empty df=\n%s", df)
        df = pd.concat(dfs, axis=0)
        _LOG.debug("After im_normalization: df=\n%s", hpandas.df_to_str(df))
        # Sort by index and `full_symbol_col_name`.
        # There is not a simple way to sort by index and columns in Pandas,
        # so we convert the index into a column, sort, and convert back.
        df = df.reset_index()
        df = df.sort_values(by=[self._timestamp_col_name, full_symbol_col_name])
        df = df.set_index(self._timestamp_col_name, drop=True)
        # The full_symbol should be a string.
        hdbg.dassert_isinstance(df[full_symbol_col_name].values[0], str)
        _LOG.debug("After sorting: df=\n%s", hpandas.df_to_str(df))
        # Check that columns are required ones.
        # TODO(gp): Difference between amp and cmamp.
        # TODO(gp): This makes a test in E8 fail.
        if False and columns is not None:
            df = hpandas.check_and_filter_matching_columns(
                df, columns, filter_data_mode
            )
        return df

    # /////////////////////////////////////////////////////////////////////////

    def get_start_ts_for_symbol(
        self, full_symbol: ivcu.FullSymbol
    ) -> pd.Timestamp:
        """
        Return the earliest timestamp available for a given `full_symbol`.

        This implementation relies on reading all the data and then
        finding the min. Derived classes can override this method if
        there is a more efficient way to get this information.
        """
        mode = "start"
        return self._get_start_end_ts_for_symbol(full_symbol, mode)

    def get_end_ts_for_symbol(self, full_symbol: ivcu.FullSymbol) -> pd.Timestamp:
        """
        Same as `get_start_ts_for_symbol()`.
        """
        mode = "end"
        return self._get_start_end_ts_for_symbol(full_symbol, mode)

    def get_full_symbols_from_asset_ids(
        self, asset_ids: List[int]
    ) -> List[ivcu.FullSymbol]:
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
        # 1) Drop duplicates.
        use_index = True
        df = hpandas.drop_duplicates(df, use_index)
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
        timestamp_col_name: str,
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
            .duplicated(subset=[timestamp_col_name, full_symbol_col_name])
            .sum()
        )
        hdbg.dassert_eq(
            n_duplicated_rows, 0, msg="There are duplicated rows in the data"
        )
        # Ensure that all the data is in [start_ts, end_ts].
        hdateti.dassert_timestamp_lte(start_ts, df.index.min())
        hdateti.dassert_timestamp_lte(df.index.max(), end_ts)

    # //////////////////////////////////////////////////////////////////////////

    def _get_full_symbol_col_name(
        self, full_symbol_col_name: Optional[str]
    ) -> str:
        """
        Resolve the name of the `full_symbol_col_name` using the value in the
        ctor and the one passed to the function.
        """
        ret = self._full_symbol_col_name
        if full_symbol_col_name is not None:
            # The function has specified it, so this value overwrites the
            # constructor value.
            hdbg.dassert_isinstance(full_symbol_col_name, str)
            ret = full_symbol_col_name
        else:
            if self._full_symbol_col_name is None:
                # Both constructor and method have not specified the value, so
                # use the default value.
                ret = "full_symbol"
        hdbg.dassert_is_not(
            ret,
            None,
            "No value for 'full_symbol_col_name' was specified: ctor value='%s', method value='%s'",
            self._full_symbol_col_name,
            full_symbol_col_name,
        )
        return ret

    @abc.abstractmethod
    def _read_data(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        *,
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Any,
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
            ivcu.build_numerical_to_string_id_mapping(full_symbol_universe)
        )
        return asset_id_to_full_symbol_mapping  # type: ignore[no-any-return]

    def _get_start_end_ts_for_symbol(
        self, full_symbol: ivcu.FullSymbol, mode: str
    ) -> pd.Timestamp:
        _LOG.debug(hprint.to_str("full_symbol"))
        # Read data for the entire period of time available.
        start_timestamp = None
        end_timestamp = None
        # Use only `self._full_symbol_col_name` after CmTask1588 is fixed.
        columns = None
        filter_data_mode = "assert"
        data = self.read_data(
            [full_symbol],
            start_timestamp,
            end_timestamp,
            columns,
            filter_data_mode,
        )
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


# TODO(Dan): Implement usage of `columns` parameter in descendant classes.
class ImClientReadingOneSymbol(ImClient, abc.ABC):
    """
    IM client for a backend that can only read one symbol at a time.

    E.g., CSV with data organized by-asset.
    """

    def _read_data(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        *,
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Same as the method in the parent class.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts columns full_symbol_col_name kwargs"
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
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
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
        full_symbol: ivcu.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Any,
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
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        *,
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Same as the parent class.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts columns full_symbol_col_name kwargs"
            )
        )
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        df = self._read_data_for_multiple_symbols(
            full_symbols,
            start_ts,
            end_ts,
            columns,
            full_symbol_col_name,
            **kwargs,
        )
        return df

    @abc.abstractmethod
    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        full_symbol_col_name: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        ...


# #############################################################################
# SqlRealTimeImClient
# #############################################################################


class RealTimeImClient(ImClient):
    """
    A realtime client for typing annotation.

    In practice all realtime clients use SQL backend.
    """


# TODO(gp): @all cleanup resample_1min should go last and probably have a default
#  value of False.
class SqlRealTimeImClient(RealTimeImClient):
    """
    Read data from a table of an SQL DB.
    """

    def __init__(
        self,
        vendor: str,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
    ) -> None:
        _LOG.debug(hprint.to_str("db_connection table_name"))
        # Real-time implementation has a different mechanism for getting universe.
        # Passing to make the parent class happy.
        universe_version = None
        # These parameters are needed to get the universe which is needed to init
        # the parent class so they go before the parent's init.
        self._table_name = table_name
        self._db_connection = db_connection
        super().__init__(vendor, universe_version, resample_1min)

    @staticmethod
    def get_metadata() -> pd.DataFrame:
        """
        Return metadata.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def should_be_online(self, wall_clock_time: pd.Timestamp) -> bool:
        pass

    def get_universe(self) -> List[ivcu.FullSymbol]:
        """
        See description in the parent class.
        """
        # Extract DataFrame with unique combinations of `exchange_id`,
        # `currency_pair`.
        query = (
            f"SELECT DISTINCT exchange_id, currency_pair FROM {self._table_name}"
        )
        currency_exchange_df = hsql.execute_query_to_df(
            self._db_connection, query
        )
        # Merge these columns to the general `full_symbol` format.
        full_symbols = ivcu.build_full_symbol(
            currency_exchange_df["exchange_id"],
            currency_exchange_df["currency_pair"],
        )
        # Convert to list.
        full_symbols = full_symbols.to_list()
        _LOG.debug(hprint.to_str("full_symbols"))
        return full_symbols

    # TODO(Danya): Propagate usage of `columns` parameter here and in descendant
    #  classes.
    def _read_data(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        columns: Optional[List[str]],
        *,
        full_symbol_col_name: Optional[str] = None,
        # Extra arguments for building a query.
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Create a select query and load data from database.

        Extra parameters for building a query can also be passed,
        see keyword args for `_build_select_query`.

        :param full_symbols: a list of full symbols, e.g., `["ftx::BTC_USDT"]`
        :param start_ts: beginning of the time interval
        :param end_ts: end of the time interval
        :param full_symbol_col_name: name of column containing full symbols
        :return:
        """
        # Parse symbols into exchange and currency pair.
        parsed_symbols = [ivcu.parse_full_symbol(s) for s in full_symbols]
        # Convert timestamps to epochs.
        if start_ts:
            start_unix_epoch = hdateti.convert_timestamp_to_unix_epoch(start_ts)
        else:
            start_unix_epoch = start_ts
        if end_ts:
            end_unix_epoch = hdateti.convert_timestamp_to_unix_epoch(end_ts)
        else:
            end_unix_epoch = end_ts
        # Read data from DB.
        select_query = self._build_select_query(
            parsed_symbols, start_unix_epoch, end_unix_epoch, **kwargs
        )
        data = hsql.execute_query_to_df(self._db_connection, select_query)
        # Add a full symbol column.
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        data[full_symbol_col_name] = ivcu.build_full_symbol(
            data["exchange_id"], data["currency_pair"]
        )
        data = data.drop(["exchange_id", "currency_pair"], axis=1)
        # Convert timestamp column with Unix epoch to timestamp format.
        data[self._timestamp_col_name] = data[self._timestamp_col_name].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
        # Remove duplicates in data.
        data = self._filter_duplicates(data, full_symbol_col_name)
        # TODO(Dan): Move column filtering to the SQL query.
        if columns is None:
            columns = data.columns
        hdbg.dassert_is_subset(columns, data.columns.to_list())
        data = data[columns]
        return data

    def _build_select_query(
        self,
        parsed_symbols: List[Tuple],
        start_unix_epoch: Optional[int],
        end_unix_epoch: Optional[int],
        *,
        columns: Optional[List[str]] = None,
        ts_col_name: Optional[str] = "timestamp",
        left_close: bool = True,
        right_close: bool = True,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build a SELECT query for SQL DB.

        Time is provided as unix epochs in ms, the time range
        is considered closed on both sides, i.e. [1647470940000, 1647471180000]

        Example of a full query:
        ```
        "SELECT * FROM talos_ohlcv WHERE timestamp >= 1647470940000
        AND timestamp <= 1647471180000
        AND ((exchange_id='binance' AND currency_pair='AVAX_USDT')
        OR (exchange_id='ftx' AND currency_pair='BTC_USDT'))
        ```

        :param parsed_symbols: List of tuples, e.g. [(`exchange_id`, `currency_pair`),..]
        :param start_unix_epoch: start of time period in ms, e.g. 1647470940000
        :param end_unix_epoch: end of the time period in ms, e.g. 1647471180000
        :param columns: columns to select from `table_name`
        - `None` means all columns.
        :param ts_col_name: name of timestamp column
        :param left_close: if operator for `start_unix_epoch` is either > or >=
        :param right_close: if operator for `end_unix_epoch` is either < or <=
        :param limit: number of rows to return
        :return: SELECT query for SQL data
        """
        hdbg.dassert_container_type(
            obj=parsed_symbols,
            container_type=List,
            elem_type=tuple,
            msg="`parsed_symbols` should be a list of tuple",
        )
        table_columns = hsql.get_table_columns(
            self._db_connection, self._table_name
        )
        if columns is None:
            columns = table_columns
        hdbg.dassert_is_subset(columns, table_columns)
        # Add columns to the SELECT query
        columns_as_str = ",".join(columns)
        # Build a SELECT query.
        select_query = f"SELECT {columns_as_str} FROM {self._table_name} WHERE "
        # Build a WHERE query.
        # TODO(Danya): Generalize to hsql with dictionary input.
        where_clause = []
        if start_unix_epoch:
            hdbg.dassert_isinstance(
                start_unix_epoch,
                int,
            )
            operator = ">=" if left_close else ">"
            where_clause.append(f"{ts_col_name} {operator} {start_unix_epoch}")
        if end_unix_epoch:
            hdbg.dassert_isinstance(
                end_unix_epoch,
                int,
            )
            operator = "<=" if right_close else "<"
            where_clause.append(f"{ts_col_name} {operator} {end_unix_epoch}")
        if start_unix_epoch and end_unix_epoch:
            hdbg.dassert_lte(
                start_unix_epoch,
                end_unix_epoch,
                msg="Start unix epoch should be smaller than end.",
            )
        # Create conditions for getting values by exchange_id and currency_pair
        # In the end there should be something like:
        # (exchange_id='binance' AND currency_pair='ADA_USDT') OR (exchange_id='ftx' AND currency_pair='BTC_USDT') # pylint: disable=line-too-long
        exchange_currency_conditions = [
            f"(exchange_id='{exchange_id}' AND currency_pair='{currency_pair}')"
            for exchange_id, currency_pair in parsed_symbols
            if exchange_id and currency_pair
        ]
        if exchange_currency_conditions:
            # Add OR conditions between each pair of `exchange_id` and `currency_pair`
            where_clause.append(
                "(" + " OR ".join(exchange_currency_conditions) + ")"
            )
        # Build whole query.
        query = select_query + " AND ".join(where_clause)
        if limit:
            query += f" LIMIT {limit}"
        return query

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[ivcu.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],  # Converts to unix epoch
        columns: Optional[List[str]],
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Read data for the given time range and full symbols.

        The method builds a SELECT query like:

        SELECT * FROM {self._table_name} WHERE exchange_id="binance" AND currency_pair="ADA_USDT"

        The WHERE clause with AND/OR operators is built using a built-in method.

        :param full_symbols: a list of symbols, e.g. ["binance::ADA_USDT"]
        :param start_ts: beginning of the period, is converted to unix epoch
        :param end_ts: end of the period, is converted to unix epoch
        :param full_symbol_col_name: the name of the full_symbol column
        """
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        # TODO(Danya): Convert timestamps to int when reading.
        # TODO(Danya): add a full symbol column to the output
        raise NotImplementedError

    def _get_start_end_ts_for_symbol(
        self, full_symbol: ivcu.FullSymbol, mode: str
    ) -> pd.Timestamp:
        """
        Select a maximum/minimum timestamp for the given symbol.

        Overrides the method in parent class to utilize
        the MIN/MAX SQL operators.

        :param full_symbol: unparsed full_symbol value
        :param mode: 'start' or 'end'
        :return: min or max value of 'timestamp' column.
        """
        _LOG.debug(hprint.to_str("full_symbol"))
        exchange, currency_pair = ivcu.parse_full_symbol(full_symbol)
        # Build a MIN/MAX query.
        if mode == "start":
            query = (
                f"SELECT MIN(timestamp) from {self._table_name}"
                f" WHERE currency_pair='{currency_pair}'"
                f" AND exchange_id='{exchange}'"
            )
        elif mode == "end":
            query = (
                f"SELECT MAX(timestamp) from {self._table_name}"
                f" WHERE currency_pair='{currency_pair}'"
                f" AND exchange_id='{exchange}'"
            )
        else:
            raise ValueError("Invalid mode='%s'" % mode)
        # TODO(Danya): factor out min/max as helper function.
        # Load the target timestamp as unix epoch.
        timestamp = hsql.execute_query_to_df(self._db_connection, query).loc[0][0]
        # Convert to `pd.Timestamp` type.
        timestamp = hdateti.convert_unix_epoch_to_timestamp(timestamp)
        hdateti.dassert_has_specified_tz(timestamp, ["UTC"])
        return timestamp

    def _filter_duplicates(
        self, data: pd.DataFrame, full_symbol_col_name: str
    ) -> pd.DataFrame:
        """
        Remove duplicates from data based on full symbol and timestamp.

        Keeps the row with the highest 'knowledge_timestamp' value.

        The function gives a warning if the knowledge timestamp is less
        than a minute over the data timestamp. This might indicate that
        the data hasn't been downloaded in full, although the risk is
        very low.

        :param data: data from the DB
        :return: DB data with duplicates removed
        """
        hdbg.dassert_is_subset(
            [
                "knowledge_timestamp",
                self._timestamp_col_name,
                full_symbol_col_name,
            ],
            data.columns,
        )
        duplicate_columns = [self._timestamp_col_name, full_symbol_col_name]
        # Remove duplicates.
        data = data.sort_values("knowledge_timestamp", ascending=False)
        use_index = False
        data = hpandas.drop_duplicates(
            data, use_index, subset=duplicate_columns
        ).sort_index()
        hdbg.dassert_lt(0, data.shape[0], "Empty df=\n%s", data)
        # Check if the knowledge_timestamp is over the candle timestamp by at least minute.
        #
        # Assert that both timestamps have timezone info.
        # TODO(Danya): Create a `hdatetime` function to assert tz in pd.Series.
        hdbg.dassert_is_not(data["knowledge_timestamp"].dt.tz, None)
        hdbg.dassert_is_not(data[self._timestamp_col_name].dt.tz, None)
        # Get all "early" data.
        mask = data["knowledge_timestamp"] <= (
            data[self._timestamp_col_name] + pd.DateOffset(minutes=1)
        )
        early_data = data.loc[mask]
        if not early_data.empty:
            _LOG.warning(
                "Knowledge timestamp for the following rows is <1m after data timestamp>:\n%s",
                hpandas.df_to_str(early_data, num_rows=None),
            )
        data = data.set_index(
            self._timestamp_col_name,
        )
        return data
