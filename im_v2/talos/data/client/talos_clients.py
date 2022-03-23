"""
Import as:

import im_v2.talos.data.client.talos_clients as imvtdctacl
"""

import abc
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.data.client.full_symbol as imvcdcfusy

_LOG = logging.getLogger(__name__)


# #############################################################################
# TalosClient
# #############################################################################


class TalosClient(icdc.ImClient, abc.ABC):
    """
    Contain common code for all the `Talos` clients, e.g.,

    - getting `Talos` universe
    """

    def __init__(self) -> None:
        """
        Constructor.
        """
        resample_1min = True
        vendor = "talos"
        super().__init__(resample_1min, vendor)

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        # TODO(Danya): CmTask1420.
        return []


# #############################################################################
# TalosParquetByTileClient
# #############################################################################


class TalosParquetByTileClient(TalosClient, icdc.ImClientReadingOneSymbol):
    """
    Read historical data for 1 `Talos` asset stored as Parquet dataset.

    It can read data from local or S3 filesystem as backend.
    """

    def __init__(
        self,
        root_dir: str,
        *,
        data_snapshot: str = "latest",
        aws_profile: Optional[str] = None,
    ) -> None:
        """
        Load `Talos` data from local or S3 filesystem.

        :param root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path (e.g., "s3://cryptokaizen-data/historical") to `Talos` data
        :param data_snapshot: version of the loaded data to use
        :param aws_profile: AWS profile name (e.g., "ck")
        """
        super().__init__()
        self._root_dir = root_dir
        self._data_snapshot = data_snapshot
        self._aws_profile = aws_profile

    @staticmethod
    def should_be_online() -> None:
        raise NotImplementedError

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def _read_data_for_one_symbol(
        self,
        full_symbol: icdc.FullSymbol,
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        # Split full symbol into exchange and currency pair.
        exchange_id, currency_pair = icdc.parse_full_symbol(full_symbol)
        # Get path to a dir with all the data for specified exchange id.
        exchange_dir_path = os.path.join(
            self._root_dir, "talos", self._data_snapshot, exchange_id
        )
        # Read raw crypto price data.
        _LOG.info(
            "Reading data for `Talos`, exchange id='%s', currencies='%s'...",
            exchange_id,
            currency_pair,
        )
        # Initialize list of filters.
        filters = [("currency_pair", "==", currency_pair)]
        if start_ts:
            # Add filtering by start timestamp if specified.
            start_ts = hdateti.convert_timestamp_to_unix_epoch(start_ts)
            filters.append(("timestamp", ">=", start_ts))
        if end_ts:
            # Add filtering by end timestamp if specified.
            end_ts = hdateti.convert_timestamp_to_unix_epoch(end_ts)
            filters.append(("timestamp", "<=", end_ts))
        if filters:
            # Add filters to kwargs if any were set.
            kwargs["filters"] = filters
        # Specify column names to load.
        columns = ["open", "high", "low", "close", "volume"]
        # Load data.
        data = hparque.from_parquet(
            exchange_dir_path,
            columns=columns,
            filters=filters,
            aws_profile=self._aws_profile,
        )
        data.index.name = None
        return data


class RealTimeSqlTalosClient(TalosClient, icdc.ImClient):
    """
    Retrieve real-time Talos data from DB using SQL queries.
    """

    def __init__(
        self,
        db_connection: hsql.DbConnection,
        table_name: str,
    ) -> None:
        super().__init__()
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

    @staticmethod
    def _apply_talos_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Talos-specific normalization:

        - Convert `timestamp` column to a UTC timestamp and set index
        - Drop extra columns (e.g. `id` created by the DB).
        """
        raise NotImplementedError

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

    def _read_data(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        *,
        full_symbol_col_name: str = "full_symbol",
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        raise NotImplementedError

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
        hdbg.dassert_isinstance(
            start_unix_epoch,
            int,
        )
        hdbg.dassert_isinstance(
            end_unix_epoch,
            int,
        )
        hdbg.dassert_lte(
            start_unix_epoch,
            end_unix_epoch,
            msg="Start unix epoch should be smaller than end.",
        )
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
            where_clause.append(f"timestamp >= {start_unix_epoch}")
        if end_unix_epoch:
            where_clause.append(f"timestamp <= {end_unix_epoch}")
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
        full_symbol_col_name: str = "full_symbol",  # This is the column to appear in the output.
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
        # TODO(Danya): Convert timestamps to int when reading.
        # TODO(Danya): add a full symbol column to the output
        raise NotImplementedError

    @staticmethod
    def _build_select_query(
        query: str,
        exchange_id: str,
        currency_pair: str,
        start_unix_epoch: int,
        end_unix_epoch: int,
    ) -> str:
        """
        Append a WHERE clause to the query.
        """
        # TODO(Danya): Depending on the implementation, can be moved out to helpers.
        raise NotImplementedError

    @staticmethod
    def _apply_talos_normalization(data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Talos-specific normalization:

        - Convert `timestamp` column to a UTC timestamp and set index
        - Drop extra columns (e.g. `id` created by the DB).
        """
        raise NotImplementedError
