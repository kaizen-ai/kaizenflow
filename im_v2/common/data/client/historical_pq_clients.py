"""
Import as:

import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
"""

import abc
import logging
from typing import List, Optional

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hprint as hprint
import im_v2.common.data.client.base_im_clients as imvcdcbimcl
import im_v2.common.data.client.full_symbol as imvcdcfusy

_LOG = logging.getLogger(__name__)


class HistoricalPqByTileClient(
    imvcdcbimcl.ImClientReadingMultipleSymbols, abc.ABC
):
    """
    Provide historical data stored as Parquet by-asset.
    """

    def __init__(
        self,
        vendor: str,
        full_symbol_col_name: str,
        root_dir_name: str,
        partition_mode: str,
    ):
        """
        Constructor.

        :param full_symbol_col_name: column name storing the `full_symbol`
        :param root_dir_name: directory storing the tiled Parquet data
        :param partition_mode: how the data is partitioned, e.g., "by_year_month"
        """
        super().__init__(vendor)
        self._full_symbol_col_name = full_symbol_col_name
        self._root_dir_name = root_dir_name
        self._partition_mode = partition_mode

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        *,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Same as abstract method.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts full_symbol_col_name columns"
            )
        )
        # Build the Parquet filtering condition.
        hdbg.dassert_container_type(full_symbols, list, str)
        # Convert symbols to asset ids and ensure that they are in string format.
        # TODO(Nikola): There is also `self._asset_id_to_full_symbol_mapping`
        #   from base constructor... use that one instead?
        asset_ids = self.get_asset_ids_from_full_symbols(full_symbols)
        asset_ids = list(map(str, asset_ids))
        asset_and_filter = (self._full_symbol_col_name, "in", asset_ids)
        filters = hparque.get_parquet_filters_from_timestamp_interval(
            self._partition_mode,
            start_ts,
            end_ts,
            additional_filter=asset_and_filter,
        )
        # Read the data.
        # TODO(gp): Add support for S3 passing aws_profile.
        df = hparque.from_parquet(
            self._root_dir_name,
            columns=columns,
            filters=filters,
            log_level=logging.INFO,
        )
        hdbg.dassert(not df.empty)
        # Convert to datetime.
        df.index = pd.to_datetime(df.index)
        # The asset data can come back from Parquet as:
        # ```
        # Categories(540, int64): [10025, 10036, 10040, 10045, ..., 82711, 82939,
        #                         83317, 89970]
        # ```
        # which confuses `df.groupby()`, so we force that column to str.
        df[self._full_symbol_col_name] = df[self._full_symbol_col_name].astype(
            str
        )
        # Rename column storing `full_symbols`, if needed.
        hdbg.dassert_in(self._full_symbol_col_name, df.columns)
        if full_symbol_col_name != self._full_symbol_col_name:
            hdbg.dassert_not_in(full_symbol_col_name, df.columns)
            df.rename(
                columns={self._full_symbol_col_name: full_symbol_col_name},
                inplace=True,
            )
        # Since we have normalized the data, the index is a timestamp and we can
        # trim the data with index in [start_ts, end_ts] to remove the excess
        # from filtering in terms of days.
        ts_col_name = None
        left_close = True
        right_close = True
        df = hpandas.trim_df(
            df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        return df


# #############################################################################


# TODO(gp): This is very similar to HistoricalPqByTile. Can we unify?
class HistoricalPqByDateClient(
    imvcdcbimcl.ImClientReadingMultipleSymbols, abc.ABC
):
    """
    Read historical data stored as Parquet by-date.
    """

    def __init__(
        self,
        vendor: str,
        full_symbol_col_name: str,
        root_dir_name: str,
        partition_mode: str,
    ):
        """
        Constructor.

        :param full_symbol_col_name: column name storing the `full_symbol`
        :param root_dir_name: directory storing the tiled Parquet data
        :param partition_mode: how the data is partitioned, e.g., "by_date"
        """
        super().__init__(vendor)
        self._full_symbol_col_name = full_symbol_col_name
        self._root_dir_name = root_dir_name
        self._partition_mode = partition_mode

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        *,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Same as abstract method.
        """
        # Build the Parquet filtering condition.
        hdbg.dassert_container_type(full_symbols, list, str)
        filters = hparque.get_parquet_filters_from_timestamp_interval(
            self._partition_mode, start_ts, end_ts
        )
        # Read the data.
        # TODO(gp): Add support for S3 passing aws_profile.
        df = hparque.from_parquet(
            self._root_dir_name,
            columns=columns,
            filters=filters,
            log_level=logging.INFO,
        )
        hdbg.dassert(not df.empty)
        # Convert to datetime.
        df.index = pd.to_datetime(df.index)
        # Rename column storing the asset ids.
        hdbg.dassert_in(self._full_symbol_col_name, df.columns)
        df[self._full_symbol_col_name] = df[self._full_symbol_col_name].astype(
            str
        )
        if full_symbol_col_name != self._full_symbol_col_name:
            hdbg.dassert_not_in(full_symbol_col_name, df.columns)
            df.rename(
                columns={self._full_symbol_col_name: full_symbol_col_name},
                inplace=True,
            )
        # Since we have normalized the data, the index is a timestamp and we can
        # trim the data with index in [start_ts, end_ts] to remove the excess
        # from filtering in terms of days.
        ts_col_name = None
        left_close = True
        right_close = True
        df = hpandas.trim_df(
            df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        return df
