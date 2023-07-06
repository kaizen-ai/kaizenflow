"""
Import as:

import im_v2.talos.data.client.talos_clients as imvtdctacl
"""

import collections
import logging
import os
from typing import Dict, List, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.common.universe as ivcu

_LOG = logging.getLogger(__name__)


# #############################################################################
# TalosHistoricalPqByTileClient
# #############################################################################


# TODO(Grisha): "Update Talos code CmTask #1967".
class TalosHistoricalPqByTileClient(icdc.HistoricalPqByTileClient):
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
        - [10:06:00, 10:08:00] retrieves data for [10:06:00, 10:08:00]
    """

    def __init__(
        self,
        universe_version: str,
        root_dir: str,
        partition_mode: str,
        data_snapshot: str,
        *,
        aws_profile: Optional[str] = None,
        resample_1min: bool = False,
    ) -> None:
        """
        Constructor.

        See the parent class for parameters description.

        :param data_snapshot: same format used in `get_data_snapshot()`
        """
        vendor = "talos"
        infer_exchange_id = False
        super().__init__(
            vendor,
            universe_version,
            root_dir,
            partition_mode,
            infer_exchange_id,
            aws_profile=aws_profile,
            resample_1min=resample_1min,
        )
        self._data_snapshot = data_snapshot
        # TODO(Sonya): Consider moving it to the base class as the `dataset` param.
        self._dataset = "ohlcv"

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    # TODO(Dan): Implement usage of `columns` parameter.
    @staticmethod
    def _get_columns_for_query(
        full_symbol_col_name: str, columns: Optional[List[str]]
    ) -> List[str]:
        """
        See description in the parent class.
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
        See description in the parent class.
        """
        # Convert to string, see the parent class for details.
        df["exchange_id"] = df["exchange_id"].astype(str)
        df["currency_pair"] = df["currency_pair"].astype(str)
        # Add full symbol column.
        df[full_symbol_col_name] = ivcu.build_full_symbol(
            df["exchange_id"], df["currency_pair"]
        )
        # Keep only necessary columns.
        columns = [full_symbol_col_name, "open", "high", "low", "close", "volume"]
        df = df[columns]
        return df

    def _get_root_dirs_symbol_filters(
        self, full_symbols: List[ivcu.FullSymbol], full_symbol_col_name: str
    ) -> Dict[str, hparque.ParquetFilter]:
        """
        Build a dict with exchange root dirs of the `Talos` data as keys and
        filtering conditions on corresponding currency pairs as values.

        E.g.,
        ```
        {
            "s3://.../20210924/ohlcv/talos/binance": (
                "currency_pair", "in", ["ADA_USDT", "BTC_USDT"]
            ),
            "s3://.../20210924/ohlcv/talos/coinbase": (
                "currency_pair", "in", ["BTC_USDT", "ETH_USDT"]
            ),
        }
        ```
        """
        root_dir = os.path.join(
            self._root_dir, self._data_snapshot, self._dataset, self._vendor
        )
        # Split full symbols into exchange id and currency pair tuples, e.g.,
        # [('binance', 'ADA_USDT'),
        # ('coinbase', 'BTC_USDT')].
        full_symbol_tuples = [
            ivcu.parse_full_symbol(full_symbol) for full_symbol in full_symbols
        ]
        # Store full symbols as a dictionary, e.g., `{exchange_id1: [currency_pair1, currency_pair2]}`.
        # `Defaultdict` provides a default value for the key that does not exists that prevents from
        # getting `KeyError`.
        symbol_dict = collections.defaultdict(list)
        for exchange_id, *currency_pair in full_symbol_tuples:
            symbol_dict[exchange_id].extend(currency_pair)
        # Build a dict with exchange root dirs as keys and Parquet filters by
        # the corresponding currency pairs as values.
        root_dir_symbol_filter_dict = {
            os.path.join(root_dir, exchange_id): (
                "currency_pair",
                "in",
                currency_pairs,
            )
            for exchange_id, currency_pairs in symbol_dict.items()
        }
        return root_dir_symbol_filter_dict


# #############################################################################
# TalosSqlRealTimeImClient
# #############################################################################


class TalosSqlRealTimeImClient(icdc.SqlRealTimeImClient):
    """
    Retrieve real-time Talos data from DB using SQL queries.
    """

    def __init__(
        self,
        universe_version: str,
        resample_1min: bool,
        db_connection: hsql.DbConnection,
        table_name: str,
        mode: str = "data_client",
    ) -> None:
        """
        2 modes are available, depending on the purpose of the loaded data:
        `data_client` and `market_data`.

        `data_client` mode loads data compatible with other clients, including
         historical ones, and is used for most prod and research tasks.

        `market_data` mode enforces an output compatible with `MarketData` class.
        This mode is required when loading data to use inside a model.
        """
        vendor = "talos"
        super().__init__(
            vendor,
            universe_version,
            db_connection,
            table_name,
            resample_1min=resample_1min,
        )
        self._mode = mode

    @staticmethod
    def should_be_online() -> bool:
        """
        The real-time system for Talos should always be online.
        """
        return True

    def _apply_normalization(
        self,
        data: pd.DataFrame,
        *,
        full_symbol_col_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Apply Talos-specific normalization.

         `data_client` mode:
        - Convert `timestamp` column to a UTC timestamp and set index.
        - Keep `open`, `high`, `low`, `close`, `volume` columns.

        `market_data` mode:
        - Add `start_timestamp` column in UTC timestamp format.
        - Add `end_timestamp` column in UTC timestamp format.
        - Add `asset_id` column which is result of mapping full_symbol to integer.
        - Drop extra columns.
        - The output looks like:
        ```
        open  high  low   close volume  start_timestamp          end_timestamp            asset_id
        0.825 0.826 0.825 0.825 18427.9 2022-03-16 2:46:00+00:00 2022-03-16 2:47:00+00:00 3303714233
        0.825 0.826 0.825 0.825 52798.5 2022-03-16 2:47:00+00:00 2022-03-16 2:48:00+00:00 3303714233
        ```
        """
        # Convert timestamp column with Unix epoch to timestamp format.
        data["timestamp"] = data["timestamp"].apply(
            hdateti.convert_unix_epoch_to_timestamp
        )
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
        if self._mode == "data_client":
            pass
        elif self._mode == "market_data":
            # TODO (Danya): Move this transformation to MarketData.
            # Add `asset_id` column using mapping on `full_symbol` column.
            data["asset_id"] = data[full_symbol_col_name].apply(
                ivcu.string_to_numerical_id
            )
            # Convert to int64 to keep NaNs alongside with int values.
            data["asset_id"] = data["asset_id"].astype(pd.Int64Dtype())
            # Generate `start_timestamp` from `end_timestamp` by substracting delta.
            delta = pd.Timedelta("1M")
            data["start_timestamp"] = data["timestamp"].apply(
                lambda pd_timestamp: (pd_timestamp - delta)
            )
            # Columns that should left in the table.
            market_data_ohlcv_columns = [
                "start_timestamp",
                "asset_id",
            ]
            # Concatenate two lists of columns.
            ohlcv_columns = ohlcv_columns + market_data_ohlcv_columns
        else:
            hdbg.dfatal(
                "Invalid mode='%s'. Correct modes: 'market_data', 'data_client'"
                % self._mode
            )
        data = data.set_index("timestamp")
        # Verify that dataframe contains OHLCV columns.
        hdbg.dassert_is_subset(ohlcv_columns, data.columns)
        # Rearrange the columns.
        data = data.loc[:, ohlcv_columns]
        return data
