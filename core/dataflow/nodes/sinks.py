"""
Import as:

import core.dataflow.nodes.sinks as cdtfnosin
"""

import collections
import itertools
import logging
import os
from typing import Any, Dict, Iterable, Tuple

import pandas as pd

import core.dataflow.core as cdtfcore
import core.dataflow.nodes.base as cdtfnobas
import core.dataflow.utils as cdtfutil
import core.finance as cofinanc
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.io_ as hio

_LOG = logging.getLogger(__name__)


class WriteDf(cdtfnobas.FitPredictNode):
    def __init__(
        self,
        nid: cdtfcore.NodeId,
        dir_name: str,
    ) -> None:
        super().__init__(nid)
        hdbg.dassert_isinstance(dir_name, str)
        self._dir_name = dir_name

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._write_df(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._write_df(df_in, fit=False)

    def _write_df(
        self, df: pd.DataFrame, fit: bool = True
    ) -> Dict[str, pd.DataFrame]:
        if self._dir_name:
            hdbg.dassert_lt(1, df.index.size)
            # Create the directory if it does not already exist.
            hio.create_dir(self._dir_name, incremental=True)
            # Get the latest `df` index value.
            if isinstance(df.index, pd.DatetimeIndex):
                # NOTE: If needed, we can pass in only the last two elements.
                epochs = cofinanc.compute_epoch(df)
                epoch = epochs.iloc[-1].values[0]
            else:
                raise NotImplementedError
            file_name = f"{epoch}.pq"
            file_name = os.path.join(self._dir_name, file_name)
            hdbg.dassert_not_exists(file_name)
            # Write the file.
            # TODO(Paul): Maybe allow the node to configure the log level.
            hparque.to_parquet(df, file_name, log_level=logging.DEBUG)
        # Collect info.
        info = collections.OrderedDict()
        info["df_out_info"] = cdtfutil.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe through.
        return {"df_out": df}


class WriteCols(cdtfnobas.FitPredictNode):
    def __init__(
        self,
        nid: cdtfcore.NodeId,
        dir_name: str,
        col_mapping: Dict[str, str],
    ) -> None:
        super().__init__(nid)
        hdbg.dassert_isinstance(dir_name, str)
        self._dir_name = dir_name
        hdbg.dassert_isinstance(col_mapping, dict)
        hdbg.dassert_eq(
            len(col_mapping),
            len(set(col_mapping.values())),
            "Target names not unique!",
        )
        if col_mapping:
            hdbg.dassert(dir_name, "Received a `col_mapping` but no `dir_name`.")
        if dir_name:
            hdbg.dassert(
                col_mapping, "Received a `dir_name` but no `col_mapping`."
            )
        self._col_mapping = col_mapping

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._write_cols(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._write_cols(df_in, fit=False)

    def _write_cols(
        self, df: pd.DataFrame, fit: bool = True
    ) -> Dict[str, pd.DataFrame]:
        for key in self._col_mapping.keys():
            hdbg.dassert_in(key, df.columns)
        # TODO(Paul): Factor out the shared code.
        if self._dir_name and self._col_mapping:
            hdbg.dassert_lt(1, df.index.size)
            # Create the directory if it does not already exist.
            hio.create_dir(self._dir_name, incremental=True)
            # Get the latest `df` index value.
            if isinstance(df.index, pd.DatetimeIndex):
                # NOTE: If needed, we can pass in only the last two elements.
                epochs = cofinanc.compute_epoch(df)
                epoch = epochs.iloc[-1].values[0]
            else:
                raise NotImplementedError
            for k, v in self._col_mapping.items():
                srs = df.iloc[-1][k]
                hdbg.dassert_isinstance(srs, pd.Series)
                srs.name = str(epoch) + "_" + v
                file_name = os.path.join(self._dir_name, srs.name + ".csv")
                hdbg.dassert_not_exists(file_name)
                # Write file.
                srs.to_csv(file_name)
        # Collect info.
        info = collections.OrderedDict()
        info["df_out_info"] = cdtfutil.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe through.
        return {"df_out": df}


def read_dfs(dir_name: str) -> Iterable[Tuple[str, pd.DataFrame]]:
    """
    Yield `(file_name, df)` from `dir_name`.

    :param dir_name: directory containing dataframes in parquet format
    :return: iterable of tuples of the form `(file_name, df)`
    """
    # Glob the `.pq` files.
    file_names = sorted(hio.find_files(dir_name, "*.pq"))
    _LOG.info("Number of parquet files found=%s", len(file_names))
    for file_name in file_names:
        # Load the dataframe.
        df = hparque.from_parquet(file_name)
        # Extract the file_name without the base or extension.
        tail = os.path.split(file_name)[1]
        key = os.path.splitext(tail)[0]
        #
        yield key, df


def consolidate_dfs(df_iter: Iterable[Tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """
    Consolidate dataframes in `df_iter` into a single dataframe.

    - The dataframes in the iterable are expected to agree on any overlap
    - A use case is to consolidate dataframes generated incrementally through
      a real-time process

    :param df_iter: iterable of the form (key, dataframe)
    :return: a dataframe consolidated along the index
    """
    # Tee the iterator to process successive dataframes together.
    iter1, iter2 = itertools.tee(df_iter)
    next(iter2)
    df_out = None
    for k2, df2 in iter2:
        k1, df1 = next(iter1)
        _LOG.info("Processing keys `%s` and `%s`", k1, k2)
        # It is expected that `df2` will have at least one index value not in
        # `df1`.
        new_idx_vals = df2.index.difference(df1.index)
        _LOG.debug(
            "Number of index values in `%s` not in `%s` is=%s",
            k2,
            k1,
            len(new_idx_vals),
        )
        dropped_idx_vals = df1.index.difference(df2.index)
        if len(dropped_idx_vals) > 0:
            _LOG.warning(
                "%s index values are in `%s` but not in `%s`",
                len(dropped_idx_vals),
                k1,
                k2,
            )
        # Compare the dataframes on the intersection of their indices.
        shared_idx = df1.index.intersection(df2.index)
        df_diff = df1.loc[shared_idx].compare(df2.loc[shared_idx])
        if df_diff.size > 0:
            _LOG.warning(
                "Dataframes `%s` and `%s` differ on overlap at %s index value(s)",
                k2,
                k1,
                len(df_diff.index),
            )
            _LOG.debug(
                "Index value(s) where dataframes differ=`%s`", str(df_diff.index)
            )
        # Generate a combined dataframe. In the event that the dataframes
        # disagree, on an overlap, we use the latest dataframe as the
        # reference.
        if df_out is None:
            df_out = df2.combine_first(df1)
        else:
            df_out = df2.combine_first(df_out)
    hhpandas.dassert_strictly_increasing_index(df_out)
    return df_out


# #############################################################################


class PlaceTrades(cdtfnobas.FitPredictNode):
    """
    Place trades from a model.
    """

    def __init__(
        self,
        nid: cdtfcore.NodeId,
        execution_mode: bool,
        config: Dict[str, Any],
    ) -> None:
        """
        Parameters have the same meaning as in `process_filled_orders()`.
        """
        super().__init__(nid)
        hdbg.dassert_in(execution_mode, ("batch", "real_time"))
        self._execution_mode = execution_mode
        self._config = config

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._place_trades(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._place_trades(df_in, fit=False)

    def _place_trades(self, df, fit: bool = True) -> Dict[str, pd.DataFrame]:
        import oms.place_orders as oplaorde

        hdbg.dassert_in(self._pred_column, df.columns)
        # TODO(gp): Make sure it's multi-index.
        hdbg.dassert_lt(1, df.index.size)
        hdbg.dassert_isinstance(df.index, pd.DatetimeIndex)
        # Get the latest `df` index value.
        if self._execution_mode == "batch":
            oplaorde.place_trades(df, self._execution_mode, self._config)
        elif self._execution_mode == "real_time":
            oplaorde.place_trades(df[-1], self._execution_mode, self._config)
        else:
            raise "Invalid execution_mode='%s'" % self._execution_mode
        # Compute stats.
        info = collections.OrderedDict()
        info["df_out_info"] = cdtfutil.get_df_info_as_string(df)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
        # Pass the dataframe through.
        return {"df_out": df}
