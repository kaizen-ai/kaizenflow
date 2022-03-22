"""
Import as:

import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
"""

import abc
import logging
import os
from typing import Any, Dict, List, Optional

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
        symbol_col_name: str,
        root_dir_name: str,
        partition_mode: str,
        *,
        version: str = "latest",
        aws_profile: Optional[str] = None,
    ):
        """
        Constructor.

        :param symbol_col_name: column name storing particular symbol data, e.g.,
            "full_symbol" or "currency_pair"
        :param root_dir_name: directory storing the tiled Parquet data
        :param partition_mode: how the data is partitioned, e.g., "by_year_month"
        :param version: version of the loaded data to use
        :param aws_profile: AWS profile name (e.g., "ck")
        """
        super().__init__(vendor)
        self._symbol_col_name = symbol_col_name
        self._root_dir_name = root_dir_name
        self._partition_mode = partition_mode
        self._version = version
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

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[icdc.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        *,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        See description in the parent class.
        """
        _LOG.debug(
            hprint.to_str(
                "full_symbols start_ts end_ts full_symbol_col_name columns"
            )
        )
        hdbg.dassert_container_type(full_symbols, list, str)
        # Build the Parquet filtering condition.
        if self._symbol_col_name == full_symbol_col_name:
            # If dataset is partitioned by full symbol column, add a filter for it.
            root_dir = self._root_dir_name
            symbol_filter = (self._symbol_col_name, "in", full_symbols)
        else:
            # If dataset is partitioned by not full symbols then it is done by
            # currency pairs, so separate them from exchange ids.
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
            # Extend a root dir to the specified exchange id dir.
            root_dir = os.path.join(
                self._root_dir_name, self._vendor, self._version, exchange_ids[0]
            )
            # Add a filter.
            symbol_filter = (self._symbol_col_name, "in", currency_pairs)
        # Build list of filters for a query.
        filters = hparque.get_parquet_filters_from_timestamp_interval(
            self._partition_mode,
            start_ts,
            end_ts,
            additional_filter=symbol_filter,
        )
        # Read the data.
        df = hparque.from_parquet(
            root_dir,
            columns=columns,
            filters=filters,
            log_level=logging.INFO,
            aws_profile=self._aws_profile,
        )
        hdbg.dassert(not df.empty)
        # Convert to datetime.
        df.index = pd.to_datetime(df.index)
        if full_symbol_col_name in df.columns:
            # The asset data can come back from Parquet as:
            # ```
            # Categories(540, int64): [10025, 10036, 10040, 10045, ..., 82711, 82939,
            #                         83317, 89970]
            # ```
            # which confuses `df.groupby()`, so we force that column to str.
            df[full_symbol_col_name] = df[full_symbol_col_name].astype(str)
        else:
            # TODO(Dan): Refactor full symbol column creation while creating Talos client.
            # Build full symbols column if it is not present in the data.
            df[full_symbol_col_name] = (
                df["exchange_id"].astype(str)
                + "::"
                + df[self._symbol_col_name].astype(str)
            )
        # Since we have normalized the data, the index is a timestamp, and we can
        # trim the data with index in [start_ts, end_ts] to remove the excess
        # from filtering in terms of days.
        # TODO(Dan): Refactor timestamp column naming while creating Talos client.
        ts_col_name = None
        if df.index.name in df.columns:
            # Remove a column that has the same name that a timestamp index
            # (e.g. in `Talos` data).
            df = df.drop(df.index.name, axis=1)
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
