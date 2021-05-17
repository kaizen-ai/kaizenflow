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
from core.dataflow.utils import get_df_info_as_string

_LOG = logging.getLogger(__name__)


# TODO(*): Create a dataflow types file.
_COL_TYPE = Union[int, str]
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################
# Abstract Node classes with sklearn-style interfaces
# #############################################################################


class FitPredictNode(Node, abc.ABC):
    """
    Define an abstract class with sklearn-style `fit` and `predict` functions.

    Nodes may store a dictionary of information for each method
    following the method's invocation.
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
        # Do not allow any empty list.
        dbg.dassert(outputs)
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
            fit_df = self.df.loc[idx].copy()
        else:
            fit_df = self.df.copy()
        info = collections.OrderedDict()
        info["fit_df_info"] = get_df_info_as_string(fit_df)
        self._set_info("fit", info)
        dbg.dassert(not fit_df.empty)
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
        info = collections.OrderedDict()
        info["predict_df_info"] = get_df_info_as_string(predict_df)
        self._set_info("predict", info)
        dbg.dassert(not predict_df.empty)
        return {self.output_names[0]: predict_df}

    def get_df(self) -> pd.DataFrame:
        dbg.dassert_is_not(self.df, None, "No DataFrame found!")
        return self.df

    @staticmethod
    def _validate_intervals(intervals: List[Tuple[Any, Any]]) -> None:
        dbg.dassert_isinstance(intervals, list)
        for interval in intervals:
            dbg.dassert_eq(len(interval), 2)
            if interval[0] is not None and interval[1] is not None:
                dbg.dassert_lte(interval[0], interval[1])


class Transformer(FitPredictNode, abc.ABC):
    """
    Stateless Single-Input Single-Output node.
    """

    # TODO(Paul): Consider giving users the option of renaming the single
    # input and single output (but verify there is only one of each).
    def __init__(self, nid: str) -> None:
        super().__init__(nid)

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_no_duplicates(df_in.columns)
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("fit", info)
        dbg.dassert_no_duplicates(df_out.columns)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_no_duplicates(df_in.columns)
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("predict", info)
        dbg.dassert_no_duplicates(df_out.columns)
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
    Selects columns to propagate in output dataframe.
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
        :param cols: columns in `df_in` that were transformed to obtain
            `df_out`. `None` defaults to all columns in `df_out`
        :param col_mode: `None`, "merge_all", "replace_selected", or
            "replace_all". Determines what columns are propagated. `None`
            defaults to "merge all". If "merge_all", perform an outer merge
        :param col_rename_func: function for naming transformed columns, e.g.,
            lambda x: "zscore_" + x. `None` defaults to identity transform
        :return: dataframe with columns selected by `col_mode`
        """
        dbg.dassert_isinstance(df_in, pd.DataFrame)
        dbg.dassert_isinstance(df_out, pd.DataFrame)
        dbg.dassert(cols is None or isinstance(cols, list))
        cols = cols or df_out.columns.tolist()
        col_rename_func = col_rename_func or (lambda x: x)
        dbg.dassert_isinstance(col_rename_func, collections.Callable)
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


