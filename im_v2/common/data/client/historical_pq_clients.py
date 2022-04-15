"""
Import as:

import im_v2.common.data.client.historical_pq_clients as imvcdchpcl
"""

import abc
import logging
from typing import Any, Dict, List, Optional

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
    Provide historical data stored as Parquet by-tile.
    """

    def __init__(
        self,
        # TODO(gp): We could use *args, **kwargs as params for ImClient,
        # same for the child classes.
        vendor: str,
        resample_1min: bool,
        root_dir: str,
        partition_mode: str,
        infer_exchange_id: bool,
        *,
        aws_profile: Optional[str] = None,
        full_symbol_col_name: Optional[str] = None,
    ):
        """
        Constructor.

        See the parent class for parameters description.

        :param root_dir: either a local root path (e.g., "/app/im") or
            an S3 root path (e.g., "s3://cryptokaizen-data/historical")
            to the tiled Parquet data
        :param partition_mode: how the data is partitioned, e.g., "by_year_month"
        :param infer_exchange_id: use the last part of a dir to indicate the exchange
            originating the data. This allows to merging multiple Parquet files on
            exchange. See CmTask #1533 "Add exchange to the ParquetDataset partition".
        :param aws_profile: AWS profile name (e.g., "ck")
        """
        super().__init__(
            vendor, resample_1min, full_symbol_col_name=full_symbol_col_name
        )
        hdbg.dassert_isinstance(root_dir, str)
        self._root_dir = root_dir
        self._infer_exchange_id = infer_exchange_id
        self._partition_mode = partition_mode
        self._aws_profile = aws_profile

    @staticmethod
    def get_universe() -> List[imvcdcfusy.FullSymbol]:
        """
        See description in the parent class.
        """
        return []

    @staticmethod
    def get_metadata() -> pd.DataFrame:
        """
        See description in the parent class.
        """
        raise NotImplementedError

    # TODO(Grisha): factor out the column names in the child classes, see `CCXT`, `Talos`.
    @staticmethod
    def _get_columns_for_query() -> Optional[List[str]]:
        """
        Get columns for Parquet data query.

        For base implementation the columns are `None`
        """
        return None

    # TODO(Grisha): factor out the common code in the child classes, see CmTask #1696
    # "Refactor HistoricalPqByTileClient and its child classes".
    @staticmethod
    def _apply_transformations(
        df: pd.DataFrame,
        full_symbol_col_name: str,
        **kwargs: Any,
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
        full_symbols: List[imvcdcfusy.FullSymbol],
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
        # Get columns and add them to kwargs if they were not specified.
        if "columns" not in kwargs:
            columns = self._get_columns_for_query()
            kwargs["columns"] = columns
        # Add AWS profile to kwargs.
        kwargs["aws_profile"] = self._aws_profile
        # Build root dirs to the data and Parquet filtering condition.
        root_dir_symbol_filter_dict = self._get_root_dirs_symbol_filters(
            full_symbols, full_symbol_col_name
        )
        #
        res_df_list = []
        for root_dir, symbol_filter in root_dir_symbol_filter_dict.items():
            # Build list of filters for a query and add them to kwargs.
            filters = hparque.get_parquet_filters_from_timestamp_interval(
                self._partition_mode,
                start_ts,
                end_ts,
                additional_filters=[symbol_filter],
            )
            kwargs["filters"] = filters
            # Read Parquet data from a root dir.
            root_dir_df = hparque.from_parquet(root_dir, **kwargs)
            hdbg.dassert_lte(1, root_dir_df.shape[0])
            # Convert index to datetime.
            root_dir_df.index = pd.to_datetime(root_dir_df.index)
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
            transformation_kwargs: Dict = {}
            if self._infer_exchange_id:
                # Infer `exchange_id` from a file path if it is not present in data.
                # E.g., `s3://cryptokaizen-data/historical/ccxt/latest/binance` -> `binance`.
                transformation_kwargs["exchange_id"] = root_dir.split("/")[-1]
            # Transform data.
            root_dir_df = self._apply_transformations(
                root_dir_df, full_symbol_col_name, **transformation_kwargs
            )
            #
            res_df_list.append(root_dir_df)
        # Combine data from all root dirs into a single DataFrame.
        res_df = pd.concat(res_df_list, axis=0)
        # Since we have normalized the data, the index is a timestamp, and we can
        # trim the data with index in [start_ts, end_ts] to remove the excess
        # from filtering in terms of days.
        ts_col_name = None
        left_close = True
        right_close = True
        res_df = hpandas.trim_df(
            res_df, ts_col_name, start_ts, end_ts, left_close, right_close
        )
        return res_df

    # TODO(Grisha): try to unify child classes with the base class, see CmTask #1696
    # "Refactor HistoricalPqByTileClient and its child classes".
    # TODO(Grisha): remove the hack that allows to read data for multiple exchanges in
    # the child classes, see CmTask #1533 "Add exchange to the ParquetDataset partition".
    # TODO(Grisha): param `full_symbol_col_name` is not used in the child classes,
    # see CmTask #1696 "Refactor HistoricalPqByTileClient and its child classes".
    def _get_root_dirs_symbol_filters(
        self, full_symbols: List[imvcdcfusy.FullSymbol], full_symbol_col_name: str
    ) -> Dict[str, hparque.ParquetFilter]:
        """
        Get dict with root dir to data as keys and corresponding symbol filters
        as values.

        Since in the base class filtering is done by full symbols, the root dir
        will be common for all of them so the output has only one key-value pair.

        E.g.,
        ```
        {
            "s3://cryptokaizen-data/historical/ccxt/latest": (
                "full_symbol", "in", ["binance::ADA_USDT", "ftx::BTC_USDT"]
            )
        }
        ```
        """
        # The root dir of the data is the one passed from the constructor.
        root_dir = self._root_dir
        # Add a filter on full symbols.
        symbol_filter = (full_symbol_col_name, "in", full_symbols)
        # Build a dict.
        res_dict = {root_dir: symbol_filter}
        return res_dict


# #############################################################################


# TODO(gp): This is very similar to HistoricalPqByTile. Can we unify?
class HistoricalPqByDateClient(
    imvcdcbimcl.ImClientReadingMultipleSymbols, abc.ABC
):
    """
    Read historical data stored as Parquet by-date.
    """

    # TODO(gp): Do not pass a read_func but use an abstract method.
    def __init__(
        self,
        vendor: str,
        resample_1min: bool,
        read_func,
        *,
        full_symbol_col_name: Optional[str] = None,
    ):
        super().__init__(
            vendor, resample_1min, full_symbol_col_name=full_symbol_col_name
        )
        self._read_func = read_func

    def _read_data_for_multiple_symbols(
        self,
        full_symbols: List[imvcdcfusy.FullSymbol],
        start_ts: Optional[pd.Timestamp],
        end_ts: Optional[pd.Timestamp],
        full_symbol_col_name: str,
        **kwargs: Any,
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
