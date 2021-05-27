import abc
import collections
import copy
import datetime
import functools
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

import helpers.dbg as dbg
from core.dataflow.core import Node
from core.dataflow.utils import (
    get_df_info_as_string,
)

_LOG = logging.getLogger(__name__)


# TODO(*): Create a dataflow types file.
_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]
_TO_LIST_MIXIN_TYPE = Union[List[_COL_TYPE], Callable[[], List[_COL_TYPE]]]


# #############################################################################
# Abstract node classes with sklearn-style interfaces
# #############################################################################


class FitPredictNode(Node, abc.ABC):
    """
    Define an abstract class with sklearn-style `fit` and `predict` functions.

    The class contains an optional state that can be serialized/deserialized with
    `get_fit_state()` and `set_fit_state()`.

    Nodes may store a dictionary of information for each method following the
    method's invocation.
    """

    def __init__(
        self,
        nid: str,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
    ) -> None:
        if inputs is None:
            inputs = ["df_in"]
        if outputs is None:
            outputs = ["df_out"]
        super().__init__(nid=nid, inputs=inputs, outputs=outputs)
        self._info = collections.OrderedDict()

    @abc.abstractmethod
    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        pass

    @abc.abstractmethod
    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        pass

    def get_fit_state(self) -> Dict[str, Any]:
        return {}

    def set_fit_state(self, fit_state: Dict[str, Any]) -> None:
        pass

    def get_info(
        self, method: str
    ) -> Optional[Union[str, collections.OrderedDict]]:
        # TODO(Paul): Add a dassert_getattr function to use here and in core.
        dbg.dassert_isinstance(method, str)
        dbg.dassert(getattr(self, method))
        if method in self._info.keys():
            return self._info[method]
        # TODO(Paul): Maybe crash if there is no info.
        _LOG.warning("No info found for nid=%s, method=%s", self.nid, method)
        return None

    def _set_info(self, method: str, values: collections.OrderedDict) -> None:
        dbg.dassert_isinstance(method, str)
        dbg.dassert(getattr(self, method))
        dbg.dassert_isinstance(values, collections.OrderedDict)
        # Save the info in the node: we make a copy just to be safe.
        self._info[method] = copy.copy(values)


class DataSource(FitPredictNode, abc.ABC):
    """
    A source node that can be configured for cross-validation.
    """

    def __init__(self, nid: str, outputs: Optional[List[str]] = None) -> None:
        if outputs is None:
            outputs = ["df_out"]
        # TODO(gp): This seems a common function. We can factor it out in a
        #  `validate_string_list()`.
        # Do not allow any empty list, repetition, or empty strings.
        dbg.dassert(outputs)
        dbg.dassert_no_duplicates(outputs)
        for output in outputs:
            dbg.dassert_ne(output, "")
        super().__init__(nid, inputs=[], outputs=outputs)
        #
        self.df = None
        self._fit_intervals = None
        self._predict_intervals = None
        self._predict_idxs = None

    def set_fit_intervals(self, intervals: List[Tuple[Any, Any]]) -> None:
        """
        :param intervals: closed time intervals like [start1, end1],
            [start2, end2]. `None` boundary is interpreted as data start/end
        """
        self._validate_intervals(intervals)
        self._fit_intervals = intervals

    # DataSource does not have a `df_in` in either `fit` or `predict` as a
    # typical `FitPredictNode` does.
    # pylint: disable=arguments-differ
    def fit(self) -> Dict[str, pd.DataFrame]:
        """
        :return: training set as df
        """
        if self._fit_intervals is not None:
            idx_slices = [
                self.df.loc[interval[0] : interval[1]].index
                for interval in self._fit_intervals
            ]
            idx = functools.reduce(lambda x, y: x.union(y), idx_slices)
            fit_df = self.df.loc[idx]
        else:
            fit_df = self.df
        fit_df = fit_df.copy()
        dbg.dassert(not fit_df.empty)
        # Update `info`.
        info = collections.OrderedDict()
        info["fit_df_info"] = get_df_info_as_string(fit_df)
        self._set_info("fit", info)
        return {self.output_names[0]: fit_df}

    def set_predict_intervals(self, intervals: List[Tuple[Any, Any]]) -> None:
        """
        :param intervals: closed time intervals like [start1, end1],
            [start2, end2]. `None` boundary is interpreted as data start/end

        TODO(*): Warn if intervals overlap with `fit` intervals.
        TODO(*): Maybe enforce that the intervals be ordered.
        """
        self._validate_intervals(intervals)
        self._predict_intervals = intervals

    # pylint: disable=arguments-differ
    def predict(self) -> Dict[str, pd.DataFrame]:
        """
        :return: test set as df
        """
        if self._predict_intervals is not None:
            idx_slices = [
                self.df.loc[interval[0] : interval[1]].index
                for interval in self._predict_intervals
            ]
            idx = functools.reduce(lambda x, y: x.union(y), idx_slices)
            predict_df = self.df.loc[idx].copy()
        else:
            predict_df = self.df.copy()
        dbg.dassert(not predict_df.empty)
        # Update `info`.
        info = collections.OrderedDict()
        info["predict_df_info"] = get_df_info_as_string(predict_df)
        self._set_info("predict", info)
        return {self.output_names[0]: predict_df}

    def get_df(self) -> pd.DataFrame:
        dbg.dassert_is_not(self.df, None, "No DataFrame found!")
        return self.df

    # TODO(gp): This is a nice function to move to `dataflow/utils.py`.
    @staticmethod
    def _validate_intervals(intervals: List[Tuple[Any, Any]]) -> None:
        dbg.dassert_isinstance(intervals, list)
        for interval in intervals:
            dbg.dassert_eq(len(interval), 2)
            if interval[0] is not None and interval[1] is not None:
                dbg.dassert_lte(interval[0], interval[1])


class Transformer(FitPredictNode, abc.ABC):
    """
    Single-input single-output node calling a stateless transformation.

    The transformation is user-defined and called before `fit()` and
    `predict()`.
    """

    # TODO(Paul): Consider giving users the option of renaming the single
    #  input and single output (but verify there is only one of each).
    def __init__(self, nid: str) -> None:
        super().__init__(nid)

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_no_duplicates(df_in.columns)
        # Transform the input df.
        df_out, info = self._transform(df_in)
        dbg.dassert_no_duplicates(df_out.columns)
        # Update `info`.
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_no_duplicates(df_in.columns)
        # Transform the input df.
        df_out, info = self._transform(df_in)
        dbg.dassert_no_duplicates(df_out.columns)
        # Update `info`.
        self._set_info("predict", info)
        return {"df_out": df_out}

    @abc.abstractmethod
    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        """
        :return: df, info
        """


# #############################################################################
# Plumbing nodes
# #############################################################################


class YConnector(FitPredictNode):
    """
    Create an output dataframe from two input dataframes.
    """

    # TODO(Paul): Support different input/output names.
    def __init__(
        self,
        nid: str,
        connector_func: Callable[..., pd.DataFrame],
        connector_kwargs: Optional[Any] = None,
    ) -> None:
        """
        :param nid: unique node id
        :param connector_func:
            * Merge
            ```
            connector_func = lambda df_in1, df_in2, **connector_kwargs:
                df_in1.merge(df_in2, **connector_kwargs)
            ```
            * Reindexing
            ```
            connector_func = lambda df_in1, df_in2, connector_kwargs:
                df_in1.reindex(index=df_in2.index, **connector_kwargs)
            ```
            * User-defined functions
            ```
            # my_func(df_in1, df_in2, **connector_kwargs)
            connector_func = my_func
            ```
        :param connector_kwargs: kwargs associated with `connector_func`
        """
        super().__init__(nid, inputs=["df_in1", "df_in2"])
        self._connector_func = connector_func
        self._connector_kwargs = connector_kwargs or {}
        self._df_in1_col_names = None
        self._df_in2_col_names = None

    def get_df_in1_col_names(self) -> List[str]:
        """
        Allow introspection on column names of input dataframe #1.
        """
        return self._get_col_names(self._df_in1_col_names)

    def get_df_in2_col_names(self) -> List[str]:
        """
        Allow introspection on column names of input dataframe #2.
        """
        return self._get_col_names(self._df_in2_col_names)

    # pylint: disable=arguments-differ
    def fit(
        self, df_in1: pd.DataFrame, df_in2: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        df_out, info = self._apply_connector_func(df_in1, df_in2)
        self._set_info("fit", info)
        return {"df_out": df_out}

    # pylint: disable=arguments-differ
    def predict(
        self, df_in1: pd.DataFrame, df_in2: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        df_out, info = self._apply_connector_func(df_in1, df_in2)
        self._set_info("predict", info)
        return {"df_out": df_out}

    def _apply_connector_func(
        self, df_in1: pd.DataFrame, df_in2: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        self._df_in1_col_names = df_in1.columns.tolist()
        self._df_in2_col_names = df_in2.columns.tolist()
        # TODO(Paul): Add meaningful info.
        df_out = self._connector_func(df_in1, df_in2, **self._connector_kwargs)
        info = collections.OrderedDict()
        info["df_merged_info"] = get_df_info_as_string(df_out)
        return df_out, info

    @staticmethod
    def _get_col_names(col_names: List[str]) -> List[str]:
        dbg.dassert_is_not(
            col_names,
            None,
            "No column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return col_names


class ColModeMixin:
    """
    Select columns to propagate in output dataframe.

    TODO(*): Refactor this so that it has clear pre and post processing stages.
    """

    def _apply_col_mode(
        self,
        df_in: pd.DataFrame,
        df_out: pd.DataFrame,
        cols: Optional[List[Any]] = None,
        col_rename_func: Optional[Callable[[Any], Any]] = None,
        col_mode: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Merge transformed dataframe with original dataframe.

        :param df_in: original dataframe
        :param df_out: transformed dataframe
        :param cols: columns in `df_in` that were transformed to obtain `df_out`
            - `None` defaults to all columns in `df_out`
        :param col_mode: Determines what columns are propagated.
            - "merge_all" (default): perform an outer merge between the
            - "replace_selected":
            - "replace_all": all columns are propagated
        :param col_rename_func: function for naming transformed columns, e.g.,
            `lambda x: "zscore_" + x`
            - `None` defaults to identity transform
        :return: dataframe with columns selected by `col_mode`
        """
        dbg.dassert_isinstance(df_in, pd.DataFrame)
        dbg.dassert_isinstance(df_out, pd.DataFrame)
        dbg.dassert(cols is None or isinstance(cols, list))
        cols = cols or df_out.columns.tolist()
        #
        col_rename_func = col_rename_func or (lambda x: x)
        dbg.dassert_isinstance(col_rename_func, collections.Callable)
        #
        col_mode = col_mode or "merge_all"
        # Rename transformed columns.
        df_out = df_out.rename(columns=col_rename_func)
        self._transformed_col_names = df_out.columns.tolist()
        # Select columns to return.
        if col_mode == "merge_all":
            shared_columns = df_out.columns.intersection(df_in.columns)
            dbg.dassert(
                shared_columns.empty,
                "Transformed column names `%s` conflict with existing column "
                "names `%s`.",
                df_out.columns,
                df_in.columns,
            )
            df_out = df_in.merge(
                df_out, how="outer", left_index=True, right_index=True
            )
        elif col_mode == "replace_selected":
            df_in_not_transformed_cols = df_in.columns.drop(cols)
            dbg.dassert(
                df_in_not_transformed_cols.intersection(df_out.columns).empty,
                "Transformed column names `%s` conflict with existing column "
                "names `%s`.",
                df_out.columns,
                df_in_not_transformed_cols,
            )
            df_out = df_in.drop(columns=cols).merge(
                df_out, left_index=True, right_index=True
            )
        elif col_mode == "replace_all":
            pass
        else:
            dbg.dfatal("Unsupported column mode `%s`", col_mode)
        dbg.dassert_no_duplicates(df_out.columns.tolist())
        return df_out


# #############################################################################
# Column processing helpers
# #############################################################################


class GroupedColDfToDfColProcessor:
    """
    Provides dataflow processing wrappers for dataframe-to-dataframe functions.

    Examples:
    1.  Suppose we want to learn one model per instrument given a dataframe
        `df` with multilevel columns
        ```
        feat1           feat2           y
        MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
        ```
        Then, to `preprocess()` we pass in a list of tuples, i.e.,
        `col_groups = [("feat1",), ("feat2",), ("y",)]. The function
        `preprocess()` returns a dictionary keyed by `MN0`, ..., `MN3`, with
        values consisting of dataframes with columns
        ```
        feat1 feat2 y
        ```
        Suppose the learning step returns a dataframe with column "y_hat" (one
        dataframe for each input dataframe). We then apply `postprocess()` to
        the dictionary of results, taking `col_group = (,)`, to obtain a single
        dataframe with multilevel columns
        ```
        y_hat
        MN0 MN1 MN2 MN3
        ```
    """

    @staticmethod
    def preprocess(
        df: pd.DataFrame,
        col_groups: List[Tuple[_COL_TYPE]],
    ) -> Dict[_COL_TYPE, pd.DataFrame]:
        # Sanity check list-related properties of `col_groups`.
        dbg.dassert_isinstance(col_groups, list)
        dbg.dassert_lt(0, len(col_groups), msg="Tuple `col_group` must be nonempty.")
        dbg.dassert_no_duplicates(col_groups)
        #
        dbg.dassert_isinstance(df, pd.DataFrame)
        # Sanity check each column group tuple.
        for col_group in col_groups:
            dbg.dassert_isinstance(col_group, tuple)
            dbg.dassert_eq(
                len(col_group),
                df.columns.nlevels - 1,
                f"Dataframe multiindex column depth incompatible with {col_group}",
            )
        # Determine output dataframe column names.
        out_col_names = [col_group[-1] for col_group in col_groups]
        _LOG.info("out_col_names=%s", out_col_names)
        dbg.dassert_no_duplicates(out_col_names)
        # Determine keys.
        keys = df[col_groups[0]].columns.to_list()
        _LOG.debug("keys=%s", keys)
        # Ensure all groups have the same keys.
        for col_group in col_groups:
            col_group_keys = df[col_group].columns.to_list()
            dbg.dassert_set_eq(keys, col_group_keys)
        # Swap levels so that keys are top level.
        df_out = df.swaplevel(i=-2, j=-1, axis=1)
        # Sort by keys
        df_out.sort_index(axis=1, level=-2, inplace=True)
        # TODO(Paul): Add more comments and add tests.
        roots = list(set([col_group[:-2] for col_group in col_groups]))
        _LOG.info("col group roots=%s", roots)
        dfs = {}
        for key in keys:
            local_dfs = []
            for root in roots:
                local_df = df_out[root + (key,)]
                local_df = local_df[out_col_names]
                local_dfs.append(local_df)
            local_df = pd.concat(local_dfs, axis=1)
            dfs[key] = local_df
        return dfs

    @staticmethod
    def postprocess(
        dfs: Dict[_COL_TYPE, pd.DataFrame],
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        raise NotImplementedError


class CrossSectionalDfToDfColProcessor:
    """
    Provides dataflow processing wrappers for cross-sectional transformations.

    These helpers are useful when we want to apply an operation such as
    principal component projection or residualization to a family of
    instruments.

    Examples:
    1.  Suppose we want to perform a principal component projection of
        `MN0`, ..., `MN3` of the `ret_0` group of a dataframe `df` with
        multilevel columns as follows:
        ```
        ret_0           close
        MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
        ```
        Then we invoke `preprocess()` with `col_group = "ret_0"`.
        The principal component projection operates on a dataframe with columns
        ```
        MN0 MN1 MN2 MN3
        ```
        and returns a dataframe with columns
        ```
        0 1 2 3
        ```
        We apply `postprocess()` to this dataframe with `col_group = "pca'`
        to obtain
        ```
        pca
        0 1 2 3
        ```
    2.  If we perform residualization on `df` as given above instead of
        principcal component projection, then column names are preserved
        after the residualization, and we may apply `postprocess()` with
        `col_group = "residual"` to obtian
        ```
        residual
        MN0 MN1 MN2 MN3
        ```
    """

    @staticmethod
    def preprocess(
        df: pd.DataFrame,
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        """
        As in `_preprocess_cols()`.
        """
        return _preprocess_cols(df, col_group)

    @staticmethod
    def postprocess(
        df: pd.DataFrame,
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        """
        Create a multi-indexed column dataframe from a single-indexed one.

        :param df: a single-level column dataframe
        :param col_group: a tuple of indices to insert
        :return: a multi-indexed column dataframe. If `df` has columns
            `MN0 MN1 MN2 MN3` and `col_group = "pca"`, then the output
            dataframe has columns
            ```
            pca
            MN0 MN1 MN2 MN3
            ```
        """
        # Perform sanity checks on dataframe.
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert_no_duplicates(df.columns)
        dbg.dassert_eq(
            1,
            df.columns.nlevels,
        )
        #
        dbg.dassert_isinstance(col_group, tuple)
        #
        if col_group:
            df = pd.concat([df], axis=1, keys=[col_group])
        return df


class SeriesToDfColProcessor:
    """
    Provides dataflow processing wrappers for series-to-dataframe functions.

    Examples of functions to wrap include:
        - series decompositions (e.g., STL, Fourier coefficients, wavelet
          levels)
        - multiple lags
        - volatility modeling

    Examples:
    1.  Suppose we want to add two lags of the columns `MN0`, ..., `MN3`
        of the `ret_0` group of a dataframe `df` with multilevel columns as
        follows:
        ```
        ret_0           close
        MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
        ```
        Then we invoke `preprocess()` with `col_group = "ret_0"`.
        Two lags are computed for each column of the dataframe with columns
        ```
        MN0 MN1 MN2 MN3
        ```
        The results of the lag computation are represented by a dictionary with
        keys `MN0`, ..., `MN3` and values consisting of dataframes with columns
        ```
        lag_1 lag_2
        ```
        We apply `postprocess()` to this dataframe with `col_group = ()` (an
        empty tuple) to obtain
        ```
        lag_1           lag_2
        MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
        ```
    """

    @staticmethod
    def preprocess(
        df: pd.DataFrame,
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        """
        As in `_preprocess_cols()`.
        """
        return _preprocess_cols(df, col_group)

    @staticmethod
    def postprocess(
        dfs: Dict[_COL_TYPE, pd.DataFrame],
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        """
        Create a multi-indexed column dataframe from keys, values, `col_group`.

        :param dfs: dataframes indexed by symbol.
        :param col_group: column levels to prefix `df` columns with
        :return: multi-level column dataframe
            - leaf columns are symbols
            - the next column level is defined by the columns of the dataframes
              in `dfs` (which are to be the same).
            - the initial levels are given by `col_group`
        """
        dbg.dassert_isinstance(dfs, dict)
        # Perform sanity checks on dataframe.
        # TODO(*): Check non-emptiness of dict, dataframes.
        for symbol, df in dfs.items():
            dbg.dassert_isinstance(df, pd.DataFrame)
            dbg.dassert_no_duplicates(df.columns)
            dbg.dassert_eq(
                1,
                df.columns.nlevels,
            )
        #
        dbg.dassert_isinstance(col_group, tuple)
        # Insert symbols as a column level.
        df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        # Swap column levels so that symbols are leaves.
        df = df.swaplevel(i=0, j=1, axis=1)
        df.sort_index(axis=1, level=0, inplace=True)
        if col_group:
            df = pd.concat([df], axis=1, keys=[col_group])
        return df


class SeriesToSeriesColProcessor:
    """
    Provides dataflow processing wrappers for series-to-series functions.

    Examples of functions to wrap include:
        - signal filters (e.g., smooth moving averages, z-scoring, outlier
          processing)
        - rolling features (e.g., moments, centered moments)
    """

    @staticmethod
    def preprocess(
        df: pd.DataFrame,
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        """
        As in `_preprocess_cols()`.
        """
        return _preprocess_cols(df, col_group)

    @staticmethod
    def postprocess(
        srs: List[pd.Series],
        col_group: Tuple[_COL_TYPE],
    ) -> pd.DataFrame:
        """
        Create a multi-indexed column dataframe from `srs` and `col_group`.

        :param srs: a list of symbols uniquely named (by symbol)
        :param col_group: column levels to add
        :return: multi-indexed column dataframe with series names as leaf
            columns
        """
        # Perform basic type checks.
        dbg.dassert_isinstance(srs, list)
        for series in srs:
            dbg.dassert_isinstance(series, pd.Series)
        dbg.dassert_isinstance(col_group, tuple)
        # Create dataframe from series.
        df = pd.concat(srs, axis=1)
        # Ensure that there are no duplicates.
        dbg.dassert_no_duplicates(df.columns)
        if col_group:
            df = pd.concat([df], axis=1, keys=[col_group])
        return df


def _preprocess_cols(
    df: pd.DataFrame,
    col_group: Tuple[_COL_TYPE],
) -> pd.DataFrame:
    """
    Extract a single-level column dataframe from a multi-indexed one.

    Typically, the last column index level corresponds to an instrument.

    :param df: multi-indexed column dataframe, e.g.,
        ```
        ret_0           close
        MN0 MN1 MN2 MN3 MN0 MN1 MN2 MN3
        ```
    :param col_group: tuple specifying all but leaf instruments, which are
        selected implicitly. E.g., `col_group = "ret_0"` extracts
        `(ret_0, MN0)` through `(ret_0, MN3)`.
    :return: a single-level column dataframe. E.g., a dataframe with
        columns
        ```
        MN0 MN1 MN2 MN3
        ```
        extracted from the `ret_0` group.
    """
    # Perform `col_group` sanity checks.
    dbg.dassert_isinstance(col_group, tuple)
    dbg.dassert_lt(0, len(col_group), msg="Tuple `col_group` must be nonempty.")
    #
    dbg.dassert_isinstance(df, pd.DataFrame)
    # Do not allow duplicate columns.
    dbg.dassert_no_duplicates(df.columns)
    # Ensure compatibility between dataframe column levels and col groups.
    dbg.dassert_eq(
        len(col_group),
        df.columns.nlevels - 1,
        "Dataframe multiindex column depth incompatible with config.",
    )
    # Select single-column-level dataframe and return.
    if col_group:
        df = df[col_group].copy()
    return df
