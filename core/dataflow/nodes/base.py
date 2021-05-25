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
    convert_to_list,
    get_df_info_as_string,
    merge_dataframes,
    validate_df_indices,
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


class MultiColModeMixin:
    """
    Support pre and post-processing for multiindexed column dataframes.
    """

    def _preprocess_df(
        self,
        in_col_group: Tuple[_COL_TYPE],
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Select `in_col_group` columns from `df` and perform sanity-checks.

        :param in_col_group: a group of cols specified by the first N - 1
            levels
        :param df: dataframe with a multiindexed column
        :return: single-level column index dataframe
        """
        # Perform col group checks.
        dbg.dassert_isinstance(in_col_group, tuple)
        dbg.dassert_lt(
            0, len(in_col_group), msg="Tuple `in_col_group` must be nonempty."
        )
        dbg.dassert_isinstance(df, pd.DataFrame)
        # Do not allow duplicate columns.
        dbg.dassert_no_duplicates(df.columns)
        # Ensure compatibility between dataframe column levels and col groups.
        dbg.dassert_eq(
            len(in_col_group),
            df.columns.nlevels - 1,
            "Dataframe multiindex column depth incompatible with config.",
        )
        # Select single-column-level dataframe and return.
        return df[in_col_group].copy()

    def _postprocess_df(
        self,
        out_col_group: Tuple[_COL_TYPE],
        df_in: pd.DataFrame,
        df_out: pd.DataFrame,
    ) -> pd.DataFrame:
        """

        :param out_col_group: new output col group names. This specifies the
            names of the first N - 1 levels. The leaf_cols names remain the
            same.
        :param df_in: original dataframe received by node
        :param df_out: single-level column indexed dataframe generated by node
        :return: a merge of `df_in` and `df_out`, using `out_col_group` keys as
            prefixes for `df_out` columns
        """
        dbg.dassert_isinstance(out_col_group, tuple)
        if out_col_group:
            df_out = pd.concat([df_out], axis=1, keys=[out_col_group])
        df = merge_dataframes(df_in, df_out)
        return df


class RegFreqMixin:
    """
    Require input dataframe to have a well-defined frequency and unique cols.
    """

    @staticmethod
    def _validate_input_df(df: pd.DataFrame) -> None:
        """
        Assert if df violates constraints, otherwise return `None`.
        """
        validate_df_indices(df)


class ToListMixin:
    """
    Support callables that return lists.
    """

    @staticmethod
    def _to_list(to_list: _TO_LIST_MIXIN_TYPE) -> List[_COL_TYPE]:
        convert_to_list(to_list)
