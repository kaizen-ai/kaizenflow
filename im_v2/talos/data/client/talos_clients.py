"""
Import as:

import im_v2.talos.data.client.talos_clients as imvtdctacl
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.data.client.full_symbol as imvcdcfusy
import im_v2.common.data.client.historical_pq_clients as imvcdchpcl

_LOG = logging.getLogger(__name__)


# #############################################################################
# TalosHistoricalPqByTileClient
# #############################################################################


class TalosHistoricalPqByTileClient(imvcdchpcl.HistoricalPqByTileClient):
    """
    Read historical data for `Talos` assets stored as Parquet dataset.

    It can read data from local or S3 filesystem as backend.

    The timing semantic of several clients is described below:
    1) Talos DB client
    2) Talos Parquet client
    3) CCXT CSV / Parquet client

    In a query for data in the interval `[a, b]`, the extremes `a` and b are
    rounded to the floor of the minute to retrieve the data.
    - E.g., for all the 3 clients:
        - [10:00:00, 10:00:36] retrieves data for [10:00:00, 10:00:00]
        - [10:07:00, 10:08:24] retrieves data for [10:07:00, 10:08:00]

    Note that for Talos DB if `b` is already a round minute, it's rounded down
    to the previous minute.
    - E.g., [10:06:00, 10:08:00]
        - For Talos DB client, retrieved data is in [10:06:00, 10:07:00]
        - For CCXT Client and Talos Client the data is in [10:06:00, 10:08:00]

    # TODO(gp): Change the Talos DB implementation to uniform the semantics,
    # since `MarketData` will not be happy with rewinding one minute.
    """

    def __init__(
        self,
        resample_1min: bool,
        root_dir: str,
        partition_mode: str,
        *,
        data_snapshot: str = "latest",
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Load `Talos` data from local or S3 filesystem.
        """
        vendor = "talos"
        super().__init__(
            vendor,
            resample_1min,
            root_dir,
            partition_mode,
            aws_profile=aws_profile,
        )
        self._data_snapshot = data_snapshot

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Danya): CmTask1420.
        return []

    @staticmethod
    def _get_columns_for_query() -> List[str]:
        """
        Get columns for Parquet data query.
        """
        columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "exchange_id",
            "currency_pair",
        ]
        return columns

    @staticmethod
    def _apply_transformations(
        df: pd.DataFrame, full_symbol_col_name: str
    ) -> pd.DataFrame:
        """
        Apply transformations to loaded data.
        """
        # Create full symbols column and drop its components.
        df[full_symbol_col_name] = (
            df["exchange_id"].astype(str) + "::" + df["currency_pair"].astype(str)
        )
        # Select only necessary columns.
        columns = ["full_symbol", "open", "high", "low", "close", "volume"]
        df = df[columns]
        return df

    def _get_root_dir_and_symbol_filter(
        self, full_symbols: List[icdc.FullSymbol], full_symbol_col_name: str
    ) -> Tuple[str, hparque.ParquetFilter]:
        """
        Get the root dir of the `Talos` data and filtering condition on
        currency pair column.
        """
        # Get the lists of exchange ids and currency pairs.
        exchange_ids, currency_pairs = tuple(
            zip(
                *[
                    icdc.parse_full_symbol(full_symbol)
                    for full_symbol in full_symbols
                ]
            )
        )
        # TODO(Dan) Extend functionality to load data for multiple exchange
        #  ids in one query when data partitioning on S3 is changed.
        # Verify that all full symbols in a query belong to one exchange id
        # since dataset is partitioned only by currency pairs.
        hdbg.dassert_eq(1, len(set(exchange_ids)))
        # Extend the root dir to include the exchange dir, e.g.,
        # "s3://cryptokaizen-data/historical/talos/latest/binance"
        root_dir = os.path.join(
            self._root_dir, self._vendor, self._data_snapshot, exchange_ids[0]
        )
        # Add a filter on currency pairs.
        symbol_filter = ("currency_pair", "in", currency_pairs)
        return root_dir, symbol_filter


# #############################################################################
# RealTimeSqlTalosClient
# #############################################################################


class RealTimeSqlTalosClient(icdc.ImClient):
    """
    Retrieve real-time Talos data from DB using SQL queries.
    """

    def __init__(
        self,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
    ) -> None:
        vendor = "talos"
        super().__init__(vendor, resample_1min)
        self._db_connection = db_connection
        self._table_name = table_name

    @staticmethod
    def should_be_online() -> bool:
        """
        The real-time system for Talos should always be online.
        """
        return True

    @staticmethod
    def get_metadata() -> pd.DataFrame:
        """
        Return metadata.
        """
        raise NotImplementedError

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Danya): CmTask1420.
        return []

    @staticmethod
    # TODO(Danya): Move up to hsql.
    def _create_in_operator(values: List[str], column_name: str) -> str:
        """
        Transform a list of possible values into an IN operator clause.

        Example:
            (`["binance", "ftx"]`, 'exchange_id') =>
            "exchange_id IN ('binance', 'ftx')"
        """
        in_operator = (
            f"{column_name} IN ("
            + ",".join([f"'{value}'" for value in values])
            + ")"
        )
        return in_operator

    def _apply_talos_normalization(
        self,
        data: pd.DataFrame,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply Talos-specific normalization:

        - Convert `timestamp` column to a UTC timestamp and set index.
        - Drop extra columns (e.g. `id` created by the DB).
        """
        # Convert timestamp column with Unix epoch to timestamp format.
        data["timestamp"] = data["timestamp"].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
        data = data.set_index("timestamp")
        # Specify OHLCV columns.
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        ohlcv_columns = [
            # "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            full_symbol_col_name,
        ]
        # Verify that dataframe contains OHLCV columns.
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data.loc[:, ohlcv_columns]
        return data

    def _read_data(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Create a select query and load data from database.
        """
        # Parse symbols into exchange and currency pair.
        parsed_symbols = [imvcdcfusy.parse_full_symbol(s) for s in full_symbols]
        exchange_ids = [symbol[0] for symbol in parsed_symbols]
        currency_pairs = [symbol[1] for symbol in parsed_symbols]
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
            exchange_ids, currency_pairs, start_unix_epoch, end_unix_epoch
        )
        data = hsql.execute_query_to_df(self._db_connection, select_query)
        # Add a full symbol column.
        full_symbol_col_name = self._get_full_symbol_col_name(
            full_symbol_col_name
        )
        data[full_symbol_col_name] = data[["exchange_id", "currency_pair"]].agg(
            "::".join, axis=1
        )
        # Remove extra columns and create a timestamp index.
        # TODO(Danya): The normalization may change depending on use of the class.
        data = self._apply_talos_normalization(
            data, full_symbol_col_name=full_symbol_col_name
        )
        return data

    def _build_select_query(
        self,
        exchange_ids: List[str],
        currency_pairs: List[str],
        start_unix_epoch: Optional[int],
        end_unix_epoch: Optional[int],
        *,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build a SELECT query for Talos DB.

        Time is provided as unix epochs in ms, the time range
        is considered closed on both sides, i.e. [1647470940000, 1647471180000]

        Example of a full query:
        ```
        "SELECT * FROM talos_ohlcv WHERE timestamp >= 1647470940000
         AND timestamp <= 1647471180000
         AND exchange_id IN ('binance')
         AND currency_pair IN ('AVAX_USDT')"
        ```

        :param exchange_ids: list of exchanges, e.g. ['binance', 'ftx']
        :param currency_pairs: list of currency pairs, e.g. ['BTC_USDT']
        :param start_unix_epoch: start of time period in ms, e.g. 1647470940000
        :param end_unix_epoch: end of the time period in ms, e.g. 1647471180000
        :return: SELECT query for Talos data
        """
        # TODO(Danya): Make all params optional to select all data.
        hdbg.dassert_list_of_strings(
            exchange_ids,
            msg="'exchange_ids' should be a list of strings, e.g. `['binance', 'ftx']`",
        )
        hdbg.dassert_list_of_strings(
            currency_pairs,
            msg="'currency_pairs' should be a list of strings, e.g. `['AVA_USDT', 'BTC_USDT']`",
        )
        # Build a SELECT query.
        select_query = f"SELECT * FROM {self._table_name} WHERE "
        # Build a WHERE query.
        # TODO(Danya): Generalize to hsql with dictionary input.
        where_clause = []
        if start_unix_epoch:
            hdbg.dassert_isinstance(
                start_unix_epoch,
                int,
            )
            where_clause.append(f"timestamp >= {start_unix_epoch}")
        if end_unix_epoch:
            hdbg.dassert_isinstance(
                end_unix_epoch,
                int,
            )
            where_clause.append(f"timestamp <= {end_unix_epoch}")
        if start_unix_epoch and end_unix_epoch:
            hdbg.dassert_lte(
                start_unix_epoch,
                end_unix_epoch,
                msg="Start unix epoch should be smaller than end.",
            )
        # Add 'exchange_id IN (...)' clause.
        where_clause.append(self._create_in_operator(exchange_ids, "exchange_id"))
        # Add 'currency_pair IN (...)' clause.
        where_clause.append(
            self._create_in_operator(currency_pairs, "currency_pair")
        )
        # Build whole query.
        query = select_query + " AND ".join(where_clause)
        if limit:
            query += f" LIMIT {limit}"
        return query

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],  # Converts to unix epoch
        *,
        full_symbol_col_name: Optional[str] = None,
        **kwargs: Dict[str, Any],
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
        self, full_symbol: imvcdcfusy.FullSymbol, mode: str
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
        exchange, currency_pair = imvcdcfusy.parse_full_symbol(full_symbol)
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
