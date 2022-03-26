"""
Import as:

import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
"""

import abc
import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import im_v2.common.data.client.base_im_clients as imvcdcbimcl

_LOG = logging.getLogger(__name__)


class HistoricalPqByTileClient(
    imvcdcbimcl.ImClientReadingMultipleSymbols, abc.ABC
):
    """
    Provide historical data stored as Parquet by-tile.
    """

    def __init__(
        self,
        vendor: str,
        root_dir: str,
        resample_1min: bool,
        partition_mode: str,
        *,
        aws_profile: Optional[str] = None,
    ):
        """
        Constructor.

        :param root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path (e.g., "s3://cryptokaizen-data/historical")
            to the tiled Parquet data
        :param partition_mode: how the data is partitioned, e.g., "by_year_month"
        :param aws_profile: AWS profile name (e.g., "ck")
        """
        super().__init__(vendor, resample_1min)
        self._root_dir = root_dir
        self._partition_mode = partition_mode
        self._aws_profile = aws_profile

    def get_metadata(self) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    def get_universe(self) -> List[icdc.FullSymbol]:
        """
        See description in the parent class.
        """
        return []

    @staticmethod
    def _get_columns_for_query() -> Optional[List[str]]:
        """
        Get columns for Parquet data query.

        For base implementation the columns are `None`
        """
        return None

    @staticmethod
    def _apply_transformations(
        df: pd.DataFrame, full_symbol_col_name: str
    ) -> pd.DataFrame:
        """
        Apply transformations to loaded data.
        """
        # The asset data can come back from Parquet as:
        # ```
        # Categories(540, int64): [10025, 10036, 10040, 10045, ..., 82711, 82939,
        #                         83317, 89970]
        # ```
        # which confuses `df.groupby()`, so we force that column to str.
        df[full_symbol_col_name] = df[full_symbol_col_name].astype(str)
        return df

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[icdc.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        hdbg.dassert_container_type(full_symbols, list, str)
        # Implement logging and add it to kwargs.
        _LOG.debug(
            hprint.to_str("full_symbols start_ts end_ts full_symbol_col_name")
        )
        kwargs["log_level"] = logging.INFO
        # Build root dir to the data and Parquet filtering condition.
        root_dir, symbol_filter = self._get_root_dir_and_symbol_filter(
            full_symbols, full_symbol_col_name
        )
        # Build list of filters for a query and add them to kwargs.
        filters = hparque.get_parquet_filters_from_timestamp_interval(
            self._partition_mode,
            start_ts,
            end_ts,
            additional_filter=symbol_filter,
        )
        kwargs["filters"] = filters
        # Get columns and add them to kwargs if they were not specified.
        if "columns" not in kwargs:
            columns = self._get_columns_for_query()
            kwargs["columns"] = columns
        # Add AWS profile to kwargs.
        kwargs["aws_profile"] = self._aws_profile
        # Read data.
        df = hparque.from_parquet(root_dir, **kwargs)
        hdbg.dassert(not df.empty)
        # TODO(Dan): Discuss if we should always convert index to timestamp
        #  or make a function so it may change based on the vendor.
        # Convert to datetime.
        df.index = pd.to_datetime(df.index)
        # TODO(gp): IgHistoricalPqByTileClient used a ctor param to rename a column.
        #  Not sure if this is still needed.
        #        # Rename column storing `full_symbols`, if needed.
        #        hdbg.dassert_in(self._full_symbol_col_name, df.columns)
        #        if full_symbol_col_name != self._full_symbol_col_name:
        #            hdbg.dassert_not_in(full_symbol_col_name, df.columns)
        #            df.rename(
        #                columns={self._full_symbol_col_name: full_symbol_col_name},
        #                inplace=True,
        #            )
        # Transform data.
        df = self._apply_transformations(df, full_symbol_col_name)
        # Since we have normalized the data, the index is a timestamp, and we can
        # trim the data with index in [start_ts, end_ts] to remove the excess
        # from filtering in terms of days.
        ts_col_name = None
        left_close = True
        right_close = True
        df = hpandas.trim_df(
            df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        return df

    def _get_root_dir_and_symbol_filter(
        self, full_symbols: List[icdc.FullSymbol], full_symbol_col_name: str
    ) -> Tuple[str, hparque.ParquetFilter]:
        """
        Get a root dir to the data and filtering condition on full symbol
        column.
        """
        # The root dir of the data is the one passed from the constructor.
        root_dir = self._root_dir
        # Add a filter on full symbols.
        symbol_filter = (full_symbol_col_name, "in", full_symbols)
        return root_dir, symbol_filter


# #############################################################################


# TODO(gp): This is very similar to HistoricalPqByTile. Can we unify?
class HistoricalPqByDateClient(
    imvcdcbimcl.ImClientReadingMultipleSymbols, abc.ABC
):
    """
    Read historical data stored as Parquet by-date.
    """

    # TODO(gp): Do not pass a read_func but use an abstract method.
    def __init__(self, full_symbol_col_name: str, read_func):
        self._full_symbol_col_name = full_symbol_col_name
        self._read_func = read_func

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[icdc.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        **kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Same as abstract method.
        """
        # The data is stored by date so we need to convert the timestamps into
        # dates and then trim the excess.
        # Compute the start_date.
        if start_ts is not None:
            start_date = start_ts.date()
        else:
            start_date = None
        # Compute the end_date.
        if end_ts is not None:
            end_date = end_ts.date()
        else:
            end_date = None
        # Get the data for [start_date, end_date].
        # TODO(gp): Use an abstract_method.
        tz_zone = "UTC"
        df = self._read_func(
            full_symbols,
            start_date,
            end_date,
            normalize=True,
            tz_zone=tz_zone,
            **kwargs,
        )
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
