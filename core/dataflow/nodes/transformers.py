import collections
import datetime
import inspect
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

import core.finance as cfinan
import core.signal_processing as csigna
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

from core.dataflow.nodes.base import (
   ColModeMixin,
   Transformer,
)
from core.dataflow.utils import (
    get_df_info_as_string,
)

# TODO(*): Create a dataflow types file.
_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################
# Transformer nodes
# #############################################################################


class ColumnTransformer(Transformer, ColModeMixin):
    """
    Perform non-index modifying changes of columns.
    """

    def __init__(
            self,
            nid: str,
            transformer_func: Callable[..., pd.DataFrame],
            transformer_kwargs: Optional[Dict[str, Any]] = None,
            # TODO(Paul): May need to assume `List` instead.
            cols: Optional[Iterable[str]] = None,
            col_rename_func: Optional[Callable[[Any], Any]] = None,
            col_mode: Optional[str] = None,
            nan_mode: Optional[str] = None,
    ) -> None:
        """
        :param nid: unique node id
        :param transformer_func: df -> df. The keyword `info` (if present) is
            assumed to have a specific semantic meaning. If present,
                - An empty dict is passed in to this `info`
                - The resulting (populated) dict is included in the node's
                  `_info`
        :param transformer_kwargs: transformer_func kwargs
        :param cols: columns to transform; `None` defaults to all available.
        :param col_rename_func: function for naming transformed columns, e.g.,
            lambda x: "zscore_" + x
        :param col_mode: `merge_all`, `replace_selected`, or `replace_all`.
            Determines what columns are propagated by the node.
        :param nan_mode: `leave_unchanged` or `drop`. If `drop`, applies to all
            columns simultaneously.
        """
        super().__init__(nid)
        if cols is not None:
            dbg.dassert_isinstance(cols, list)
        self._cols = cols
        self._col_rename_func = col_rename_func
        self._col_mode = col_mode
        self._transformer_func = transformer_func
        self._transformer_kwargs = transformer_kwargs or {}
        # Store the list of columns after the transformation.
        self._transformed_col_names = None
        self._nan_mode = nan_mode or "leave_unchanged"
        self._fit_cols = cols

    @property
    def transformed_col_names(self) -> List[str]:
        dbg.dassert_is_not(
            self._transformed_col_names,
            None,
            "No transformed column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return self._transformed_col_names

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df_in = df.copy()
        df = df.copy()
        if self._fit_cols is None:
            self._fit_cols = df.columns.tolist() or self._cols
        if self._cols is None:
            dbg.dassert_set_eq(self._fit_cols, df.columns)
        df = df[self._fit_cols]
        idx = df.index
        if self._nan_mode == "leave_unchanged":
            pass
        elif self._nan_mode == "drop":
            df = df.dropna()
        else:
            raise ValueError(f"Unrecognized `nan_mode` {self._nan_mode}")
        # Initialize container to store info (e.g., auxiliary stats) in the
        # node.
        info = collections.OrderedDict()
        # Perform the column transformation operations.
        # Introspect to see whether `_transformer_func` contains an `info`
        # parameter. If so, inject an empty dict to be populated when
        # `_transformer_func` is executed.
        func_sig = inspect.signature(self._transformer_func)
        if "info" in func_sig.parameters:
            func_info = collections.OrderedDict()
            df = self._transformer_func(
                df, info=func_info, **self._transformer_kwargs
            )
            info["func_info"] = func_info
        else:
            df = self._transformer_func(df, **self._transformer_kwargs)
        df = df.reindex(index=idx)
        # TODO(Paul): Consider supporting the option of relaxing or
        # foregoing this check.
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the same!",
        )
        # Maybe merge transformed columns with a subset of input df columns.
        df = self._apply_col_mode(
            df_in,
            df,
            cols=df.columns.tolist(),
            col_rename_func=self._col_rename_func,
            col_mode=self._col_mode,
        )
        #
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class SeriesTransformer(Transformer, ColModeMixin):
    """
    Perform non-index modifying changes of columns.
    """

    def __init__(
            self,
            nid: str,
            transformer_func: Callable[..., pd.DataFrame],
            transformer_kwargs: Optional[Dict[str, Any]] = None,
            # TODO(Paul): May need to assume `List` instead.
            cols: Optional[Iterable[Union[int, str]]] = None,
            col_rename_func: Optional[Callable[[Any], Any]] = None,
            col_mode: Optional[str] = None,
            nan_mode: Optional[str] = None,
    ) -> None:
        """
        :param nid: unique node id
        :param transformer_func: srs -> df. The keyword `info` (if present) is
            assumed to have a specific semantic meaning. If present,
                - An empty dict is passed in to this `info`
                - The resulting (populated) dict is included in the node's
                  `_info`
        :param transformer_kwargs: transformer_func kwargs
        :param cols: columns to transform; `None` defaults to all available.
        :param col_rename_func: function for naming transformed columns, e.g.,
            lambda x: "zscore_" + x
        :param col_mode: `merge_all`, `replace_selected`, or `replace_all`.
            Determines what columns are propagated by the node.
        :param nan_mode: `leave_unchanged` or `drop`. If `drop`, applies to
            columns individually.
        """
        super().__init__(nid)
        if cols is not None:
            dbg.dassert_isinstance(cols, list)
        self._cols = cols
        self._col_rename_func = col_rename_func
        self._col_mode = col_mode
        self._transformer_func = transformer_func
        self._transformer_kwargs = transformer_kwargs or {}
        # Store the list of columns after the transformation.
        self._transformed_col_names = None
        self._nan_mode = nan_mode or "leave_unchanged"
        self._fit_cols = cols

    @property
    def transformed_col_names(self) -> List[str]:
        dbg.dassert_is_not(
            self._transformed_col_names,
            None,
            "No transformed column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return self._transformed_col_names

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df_in = df.copy()
        df = df.copy()
        if self._fit_cols is None:
            self._fit_cols = df.columns.tolist() or self._cols
        if self._cols is None:
            dbg.dassert_set_eq(self._fit_cols, df.columns)
        df = df[self._fit_cols]
        idx = df.index
        # Initialize container to store info (e.g., auxiliary stats) in the
        # node.
        info = collections.OrderedDict()
        info["func_info"] = collections.OrderedDict()
        func_info = info["func_info"]
        srs_list = []
        for col in df.columns:
            col_info = collections.OrderedDict()
            srs = df[col]
            if self._nan_mode == "leave_unchanged":
                pass
            elif self._nan_mode == "drop":
                srs = srs.dropna()
            else:
                raise ValueError(f"Unrecognized `nan_mode` {self._nan_mode}")
            # Perform the column transformation operations.
            # Introspect to see whether `_transformer_func` contains an `info`
            # parameter. If so, inject an empty dict to be populated when
            # `_transformer_func` is executed.
            func_sig = inspect.signature(self._transformer_func)
            if "info" in func_sig.parameters:
                func_info = collections.OrderedDict()
                srs = self._transformer_func(
                    srs, info=col_info, **self._transformer_kwargs
                )
                func_info[col] = col_info
            else:
                srs = self._transformer_func(srs, **self._transformer_kwargs)
            if self._col_rename_func is not None:
                srs.name = self._col_rename_func(col)
            else:
                srs.name = col
            srs_list.append(srs)
        info["func_info"] = func_info
        df = pd.concat(srs_list, axis=1)
        df = df.reindex(index=idx)
        # TODO(Paul): Consider supporting the option of relaxing or
        # foregoing this check.
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the same!",
        )
        # Maybe merge transformed columns with a subset of input df columns.
        df = self._apply_col_mode(
            df_in,
            df,
            cols=df.columns.tolist(),
            col_rename_func=None,
            col_mode=self._col_mode,
        )
        #
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class MultiindexSeriesTransformer(Transformer):
    """
    Perform non-index modifying changes of columns.

    When operating on multiple columns, this applies the transformer function
    one series at a time. Additionally, NaN-handling is performed "locally"
    (one series at a time, without regard to NaNs in other columns).

    Example: df like
                          close                     vol
                          MN0   MN1    MN2   MN3    MN0    MN1    MN2    MN3
    2010-01-04 10:30:00 -2.62  8.81  14.93 -0.88  100.0  100.0  100.0  100.0
    2010-01-04 11:00:00 -2.09  8.27  16.75 -0.92  100.0  100.0  100.0  100.0
    2010-01-04 11:30:00 -2.52  6.97  12.56 -1.52  100.0  100.0  100.0  100.0
    2010-01-04 12:00:00 -2.54  5.30   8.90 -1.54  100.0  100.0  100.0  100.0
    2010-01-04 12:30:00 -1.91  2.02   4.65 -1.77  100.0  100.0  100.0  100.0

    Then, e.g., to calculate, returns, we could take
      - `in_col_group = "close",`
      - `out_col_group = "ret_0",`
    Notice that the trailing comma makes these tuples.

    The transformer_func and `nan_mode` would operate on the price columns
    individually and return one return column per price column, e.g.,
    generating

                          ret_0                   close                     vol
                          MN0   MN1   MN2   MN3   MN0   MN1    MN2   MN3    MN0    MN1    MN2    MN3
    2010-01-04 10:30:00 -0.02  0.11  0.16 -0.35 -2.62  8.81  14.93 -0.88  100.0  100.0  100.0  100.0
    2010-01-04 11:00:00 -0.20 -0.06  0.12  0.04 -2.09  8.27  16.75 -0.92  100.0  100.0  100.0  100.0
    2010-01-04 11:30:00  0.20 -0.16 -0.25  0.66 -2.52  6.97  12.56 -1.52  100.0  100.0  100.0  100.0
    2010-01-04 12:00:00  0.01 -0.24 -0.29  0.01 -2.54  5.30   8.90 -1.54  100.0  100.0  100.0  100.0
    2010-01-04 12:30:00 -0.25 -0.62 -0.48  0.15 -1.91  2.02   4.65 -1.77  100.0  100.0  100.0  100.0
    """

    def __init__(
            self,
            nid: str,
            in_col_group: Tuple[_COL_TYPE],
            out_col_group: Tuple[_COL_TYPE],
            transformer_func: Callable[..., pd.Series],
            transformer_kwargs: Optional[Dict[str, Any]] = None,
            nan_mode: Optional[str] = None,
    ) -> None:
        """
        For reference, let:

          - N = df.columns.nlevels
          - leaf_cols = df[in_col_group].columns

        :param nid: unique node id
        :param in_col_group: a group of cols specified by the first N - 1
            levels
        :param out_col_group: new output col group names. This specifies the
            names of the first N - 1 levels. The leaf_cols names remain the
            same.
        :param transformer_func: srs -> srs
        :param transformer_kwargs: transformer_func kwargs
        :param nan_mode: `leave_unchanged` or `drop`. If `drop`, applies to
            columns individually.
        """
        super().__init__(nid)
        dbg.dassert_isinstance(in_col_group, tuple)
        dbg.dassert_isinstance(out_col_group, tuple)
        dbg.dassert_eq(
            len(in_col_group),
            len(out_col_group),
            msg="Column hierarchy depth must be preserved.",
        )
        self._in_col_group = in_col_group
        self._out_col_group = out_col_group
        self._transformer_func = transformer_func
        self._transformer_kwargs = transformer_kwargs or {}
        self._nan_mode = nan_mode or "leave_unchanged"
        # The leaf col names are determined from the dataframe at runtime.
        self._leaf_cols = None

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        # After indexing by `self._in_col_group`, we should have a flat column
        # index.
        dbg.dassert_eq(
            len(self._in_col_group),
            df.columns.nlevels - 1,
            "Dataframe multiindex column depth incompatible with config.",
            )
        # Do not allow overwriting existing columns.
        dbg.dassert_not_in(
            self._out_col_group,
            df.columns,
            "Desired column names already present in dataframe.",
        )
        df_in = df
        df = df[self._in_col_group].copy()
        self._leaf_cols = df.columns.tolist()
        idx = df.index
        # Initialize container to store info (e.g., auxiliary stats) in the
        # node..
        info = collections.OrderedDict()
        info["func_info"] = collections.OrderedDict()
        func_info = info["func_info"]
        srs_list = []
        for col in self._leaf_cols:
            col_info = collections.OrderedDict()
            srs = df[col]
            if self._nan_mode == "leave_unchanged":
                pass
            elif self._nan_mode == "drop":
                srs = srs.dropna()
            else:
                raise ValueError(f"Unrecognized `nan_mode` {self._nan_mode}")
            # Perform the column transformation operations.
            # Introspect to see whether `_transformer_func` contains an `info`
            # parameter. If so, inject an empty dict to be populated when
            # `_transformer_func` is executed.
            func_sig = inspect.signature(self._transformer_func)
            if "info" in func_sig.parameters:
                func_info = collections.OrderedDict()
                srs = self._transformer_func(
                    srs, info=col_info, **self._transformer_kwargs
                )
                func_info[col] = col_info
            else:
                srs = self._transformer_func(srs, **self._transformer_kwargs)
            dbg.dassert_isinstance(srs, pd.Series)
            srs.name = col
            srs_list.append(srs)
        info["func_info"] = func_info
        # Combine the series representing leaf col transformations back into a
        # single dataframe.
        df = pd.concat(srs_list, axis=1)
        # Prefix the leaf col names with level(s) specified by "out_col_group".
        df = pd.concat([df], axis=1, keys=self._out_col_group)
        df = df.reindex(index=idx)
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the same!",
        )
        df = df.merge(
            df_in,
            how="outer",
            left_index=True,
            right_index=True,
        )
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class DataframeMethodRunner(Transformer):
    def __init__(
            self,
            nid: str,
            method: str,
            method_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(nid)
        dbg.dassert(method)
        # TODO(Paul): Ensure that this is a valid method.
        self._method = method
        self._method_kwargs = method_kwargs or {}

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        df = getattr(df, self._method)(**self._method_kwargs)
        # Not all methods return DataFrames. We want to restrict to those that
        # do.
        dbg.dassert_isinstance(df, pd.DataFrame)
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class Resample(Transformer):
    def __init__(
            self,
            nid: str,
            rule: Union[pd.DateOffset, pd.Timedelta, str],
            agg_func: str,
            resample_kwargs: Optional[Dict[str, Any]] = None,
            agg_func_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        :param nid: node identifier
        :param rule: resampling frequency
        :param agg_func: a function that is applied to the resampler
        :param resample_kwargs: kwargs for `resample`. Should not include
            `rule` since we handle this separately.
        :param agg_func_kwargs: kwargs for agg_func
        """
        super().__init__(nid)
        self._rule = rule
        self._agg_func = agg_func
        self._resample_kwargs = resample_kwargs or {}
        self._agg_func_kwargs = agg_func_kwargs or {}

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        resampler = csigna.resample(df, rule=self._rule, **self._resample_kwargs)
        df = getattr(resampler, self._agg_func)(**self._agg_func_kwargs)
        #
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class TimeBarResampler(Transformer):
    def __init__(
            self,
            nid: str,
            rule: Union[pd.DateOffset, pd.Timedelta, str],
            return_cols: Optional[list] = None,
            return_agg_func: Optional[str] = None,
            return_agg_func_kwargs: Optional[dict] = None,
            price_cols: Optional[list] = None,
            price_agg_func: Optional[str] = None,
            price_agg_func_kwargs: Optional[list] = None,
            volume_cols: Optional[list] = None,
            volume_agg_func: Optional[str] = None,
            volume_agg_func_kwargs: Optional[list] = None,
    ) -> None:
        """
        Resample time bars with returns, price, volume.

        This function wraps `resample_time_bars()`. Params as in that function.

        :param nid: node identifier
        """
        super().__init__(nid)
        self._rule = rule
        self._return_cols = return_cols
        self._return_agg_func = return_agg_func
        self._return_agg_func_kwargs = return_agg_func_kwargs
        self._price_cols = price_cols
        self._price_agg_func = price_agg_func
        self._price_agg_func_kwargs = price_agg_func_kwargs
        self._volume_cols = volume_cols
        self._volume_agg_func = volume_agg_func
        self._volume_agg_func_kwargs = volume_agg_func_kwargs

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        df = cfinan.resample_time_bars(
            df,
            self._rule,
            return_cols=self._return_cols,
            return_agg_func=self._return_agg_func,
            return_agg_func_kwargs=self._return_agg_func_kwargs,
            price_cols=self._price_cols,
            price_agg_func=self._price_agg_func,
            price_agg_func_kwargs=self._price_agg_func_kwargs,
            volume_cols=self._volume_cols,
            volume_agg_func=self._volume_agg_func,
            volume_agg_func_kwargs=self._volume_agg_func_kwargs,
        )
        #
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class TwapVwapComputer(Transformer):
    def __init__(
            self,
            nid: str,
            rule: Union[pd.DateOffset, pd.Timedelta, str],
            price_col: Any,
            volume_col: Any,
    ) -> None:
        """
        Calculate TWAP and VWAP prices from price and volume columns.

        This function wraps `compute_twap_vwap()`. Params as in that function.

        :param nid: node identifier
        """
        super().__init__(nid)
        self._rule = rule
        self._price_col = price_col
        self._volume_col = volume_col

    def _transform(
            self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        df = cfinan.compute_twap_vwap(
            df,
            self._rule,
            price_col=self._price_col,
            volume_col=self._volume_col,
        )
        #
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info
