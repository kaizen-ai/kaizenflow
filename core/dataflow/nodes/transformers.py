import collections
import datetime
import inspect
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

import core.dataflow.nodes.base as cdnb
import core.dataflow.utils as cdu
import core.finance as cfinan
import core.signal_processing as csigna
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

# TODO(*): Create a dataflow types file.
_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]
_RESAMPLING_RULE_TYPE = Union[pd.DateOffset, pd.Timedelta, str]


class ColumnTransformer(cdnb.Transformer, cdnb.ColModeMixin):
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
        :param transformer_func: df -> df
            - The keyword `info` (if present) means that:
                - An empty dict is passed in to this `info`
                - The resulting (populated) dict is included in the node's
                  `_info`
        :param transformer_kwargs: `transformer_func` kwargs
        :param cols: columns to transform; `None` defaults to all available.
        :param col_rename_func: function for naming transformed columns, e.g.,
            `lambda x: "zscore_" + x`
        :param col_mode: determines what columns are propagated by the node.
            Same values as in `apply_col_mode()`.
        :param nan_mode: determines how to handle NaNs
            - `leave_unchanged` (default): do not process NaNs
            - `drop`: it applies to all columns simultaneously.
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
        # State of the object. This is set by derived classes.
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
        # Handle NaNs.
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
        # Reindex df to align it with the original data.
        df = df.reindex(index=idx)
        # TODO(Paul): Consider supporting the option of relaxing or foregoing this
        #  check.
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the same!",
        )
        # Maybe merge transformed columns with a subset of input df columns.
        df = self._apply_col_mode(
            df_in,
            df,
            cols=df_in.columns.tolist(),
            col_rename_func=self._col_rename_func,
            col_mode=self._col_mode,
        )
        # Update `info`.
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


class SeriesTransformer(cdnb.Transformer, cdnb.ColModeMixin):
    """
    Perform non-index modifying changes of columns.

    TODO(*): Factor out code common with `SeriesToSeriesTransformer`.
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
        for col in self._leaf_cols:
            srs, col_info = _apply_func_to_series(
                df[col],
                self._nan_mode,
                self._transformer_func,
                self._transformer_kwargs,
            )
            dbg.dassert_isinstance(srs, pd.Series)
            srs.name = col
            if col_info is not None:
                func_info[col] = col_info
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
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


class SeriesToDfTransformer(cdnb.Transformer):
    """
    Wrap transformers using the `SeriesToDfColProcessor` pattern.
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
        For reference, let
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
        # Preprocess to extract relevant flat dataframe.
        df_in = df.copy()
        df = cdnb.SeriesToDfColProcessor.preprocess(df, self._in_col_group)
        # Apply `transform()` function column-wise.
        self._leaf_cols = df.columns.tolist()
        # Initialize container to store info (e.g., auxiliary stats) in the
        # node.
        info = collections.OrderedDict()
        info["func_info"] = collections.OrderedDict()
        func_info = info["func_info"]
        dfs = {}
        for col in self._leaf_cols:
            df_out, col_info = _apply_func_to_series(
                df[col],
                self._nan_mode,
                self._transformer_func,
                self._transformer_kwargs,
            )
            dbg.dassert_isinstance(df_out, pd.DataFrame)
            if col_info is not None:
                func_info[col] = col_info
            dfs[col] = df_out
        info["func_info"] = func_info
        # Combine the series representing leaf col transformations back into a
        # single dataframe.
        df = cdnb.SeriesToDfColProcessor.postprocess(dfs, self._out_col_group)
        df = cdu.merge_dataframes(df_in, df)
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


class SeriesToSeriesTransformer(cdnb.Transformer):
    """
    Wrap transformers using the `SeriesToSeriesColProcessor` pattern.

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
        For reference, let
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
        # Preprocess to extract relevant flat dataframe.
        df_in = df.copy()
        df = cdnb.SeriesToSeriesColProcessor.preprocess(df, self._in_col_group)
        # Apply `transform()` function column-wise.
        self._leaf_cols = df.columns.tolist()
        # Initialize container to store info (e.g., auxiliary stats) in the
        # node.
        info = collections.OrderedDict()
        info["func_info"] = collections.OrderedDict()
        func_info = info["func_info"]
        srs_list = []
        for col in self._leaf_cols:
            srs, col_info = _apply_func_to_series(
                df[col],
                self._nan_mode,
                self._transformer_func,
                self._transformer_kwargs,
            )
            dbg.dassert_isinstance(srs, pd.Series)
            srs.name = col
            if col_info is not None:
                func_info[col] = col_info
            srs_list.append(srs)
        info["func_info"] = func_info
        # Combine the series representing leaf col transformations back into a
        # single dataframe.
        df = cdnb.SeriesToSeriesColProcessor.postprocess(
            srs_list, self._out_col_group
        )
        df = cdu.merge_dataframes(df_in, df)
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


def _apply_func_to_series(
    srs: pd.Series,
    nan_mode: str,
    func,
    func_kwargs,
) -> Tuple[Union[pd.Series, pd.DataFrame], Optional[collections.OrderedDict]]:
    """
    Apply `func` to `srs` with `func_kwargs` after first applying `nan_mode`.

    This is mainly a wrapper for propagating `info` back when `func` supports
    the parameter.

    TODO(*): We should consider only having `nan_mode` as one of the
    `func_kwargs`, since now many functions support it directly.
    """
    if nan_mode == "leave_unchanged":
        pass
    elif nan_mode == "drop":
        srs = srs.dropna()
    else:
        raise ValueError(f"Unrecognized `nan_mode` {nan_mode}")
    info = collections.OrderedDict()
    # Perform the column transformation operations.
    # Introspect to see whether `_transformer_func` contains an `info`
    # parameter. If so, inject an empty dict to be populated when
    # `_transformer_func` is executed.
    func_sig = inspect.signature(func)
    if "info" in func_sig.parameters:
        result = func(
            srs,
            info=info,
            **func_kwargs,
        )
    else:
        result = func(srs, **func_kwargs)
        info = None
    return result, info


class DataframeMethodRunner(cdnb.Transformer):
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
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


# #############################################################################
# Resamplers
# #############################################################################


class Resample(cdnb.Transformer):
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
        func = getattr(resampler, self._agg_func)
        df = func(**self._agg_func_kwargs)
        # Update `info`.
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


class TimeBarResampler(cdnb.Transformer):
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
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


class TwapVwapComputer(cdnb.Transformer):
    def __init__(
        self,
        nid: str,
        rule: _RESAMPLING_RULE_TYPE,
        price_col: _COL_TYPE,
        volume_col: _COL_TYPE,
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
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info


class MultiindexTwapVwapComputer(cdnb.Transformer):
    def __init__(
        self,
        nid: str,
        rule: _RESAMPLING_RULE_TYPE,
        price_col_group: Tuple[_COL_TYPE],
        volume_col_group: Tuple[_COL_TYPE],
        out_col_group: Tuple[_COL_TYPE],
    ) -> None:
        """
        Calculate TWAP and VWAP prices from price and volume columns.

        This function wraps `compute_twap_vwap()`. Params as in that function.
        """
        super().__init__(nid)
        self._rule = rule
        self._price_col_group = price_col_group
        self._volume_col_group = volume_col_group
        self._price_col_name = price_col_group[-1]
        self._volume_col_name = volume_col_group[-1]
        self._out_col_group = out_col_group

    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        price_df = cdnb.preprocess_multiindex_cols(df, self._price_col_group)
        volume_df = cdnb.preprocess_multiindex_cols(df, self._volume_col_group)
        dbg.dassert_eq_all(price_df.columns, volume_df.columns)
        dfs = {}
        for col in price_df.columns:
            price_col = price_df[col]
            price_col.name = self._price_col_name
            volume_col = volume_df[col]
            volume_col.name = self._volume_col_name
            df = pd.concat([price_col, volume_col], axis=1)
            df = cfinan.compute_twap_vwap(
                df,
                self._rule,
                price_col=self._price_col_name,
                volume_col=self._volume_col_name,
            )
            dfs[col] = df
        # Insert symbols as a column level.
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        # Swap column levels so that symbols are leaves.
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        if self._out_col_group:
            df = pd.concat([df], axis=1, keys=[self._out_col_group])
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = cdu.get_df_info_as_string(df)
        return df, info
