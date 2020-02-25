import abc
import collections
import copy
import datetime
import inspect
import io
import logging
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import gluonts.model.deepar as gmd
import gluonts.trainer as gt
import numpy as np
import pandas as pd

import core.backtest as bcktst
import core.data_adapters as adpt
import core.finance as fin
import core.statistics as stats
import helpers.dbg as dbg

# TODO(*): This is an exception to the rule waiting for PartTask553.
from core.dataflow.core import DAG, Node

_LOG = logging.getLogger(__name__)


_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# #############################################################################
# DAG visiting
# #############################################################################


def extract_info(dag: DAG, methods: List[str]) -> collections.OrderedDict:
    """
    Extract node info from each DAG node.

    :param dag: Dag. Node info is populated upon running.
    :param methods: Node method infos to extract
    :return: nested OrderedDict
    """
    dbg.dassert_isinstance(dag, DAG)
    dbg.dassert_isinstance(methods, list)
    info = collections.OrderedDict()
    for nid in dag.dag.nodes():
        node_info = collections.OrderedDict()
        for method in methods:
            method_info = dag.get_node(nid).get_info(method)
            node_info[method] = copy.copy(method_info)
        info[nid] = copy.copy(node_info)
    return info


# #############################################################################
# Abstract Node classes with sklearn-style interfaces
# #############################################################################


class FitPredictNode(Node, abc.ABC):
    """
    Define an abstract class with sklearn-style `fit` and `predict` functions.

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
        self._fit_idxs = None
        self._predict_idxs = None

    def set_fit_idxs(self, fit_idxs: pd.DatetimeIndex) -> None:
        """
        :param fit_idxs: indices of the df to use for fitting
        """
        self._fit_idxs = fit_idxs

    # DataSource does not have a `df_in` in either `fit` or `predict` as a
    # typical `FitPredictNode` does.
    # pylint: disable=arguments-differ
    def fit(self) -> Dict[str, pd.DataFrame]:
        """
        :return: training set as df
        """
        if self._fit_idxs is not None:
            fit_df = self.df.loc[self._fit_idxs].copy()
        else:
            fit_df = self.df.copy()
        info = collections.OrderedDict()
        info["fit_df_info"] = get_df_info_as_string(fit_df)
        self._set_info("fit", info)
        return {self.output_names[0]: fit_df}

    def set_predict_idxs(self, predict_idxs: pd.DatetimeIndex) -> None:
        """
        :param predict_idxs: indices of the df to use for predicting
        """
        self._predict_idxs = predict_idxs

    # pylint: disable=arguments-differ
    def predict(self) -> Dict[str, pd.DataFrame]:
        """
        :return: test set as df
        """
        if self._predict_idxs is not None:
            predict_df = self.df.loc[self._predict_idxs].copy()
        else:
            predict_df = self.df.copy()
        info = collections.OrderedDict()
        info["predict_df_info"] = get_df_info_as_string(predict_df)
        self._set_info("predict", info)
        return {self.output_names[0]: predict_df}

    def get_df(self) -> pd.DataFrame:
        dbg.dassert_is_not(self.df, None, "No DataFrame found!")
        return self.df


class Transformer(FitPredictNode, abc.ABC):
    """
    Stateless Single-Input Single-Output node.
    """

    # TODO(Paul): Consider giving users the option of renaming the single
    # input and single output (but verify there is only one of each).
    def __init__(self, nid: str) -> None:
        super().__init__(nid)

    @abc.abstractmethod
    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        """
        :return: df, info
        """

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("predict", info)
        return {"df_out": df_out}


# #############################################################################
# Data source nodes
# #############################################################################


class ReadDataFromDf(DataSource):
    def __init__(self, nid: str, df: pd.DataFrame) -> None:
        super().__init__(nid)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


class DiskDataSource(DataSource):
    def __init__(
        self,
        nid: str,
        file_path: str,
        timestamp_col: Optional[str],
        start_date: Optional[_PANDAS_DATE_TYPE] = None,
        end_date: Optional[_PANDAS_DATE_TYPE] = None,
        **kwargs: Any,
    ) -> None:
        """
        Create data source node reading CSV or parquet data from disk.

        :param nid: node identifier
        :param file_path: path to the file
        :param timestamp_col: name of the timestamp column. If `None`, assume
            that index contains timestamps
        :param start_date: data start date in timezone of the dataset, included
        :param end_date: data end date in timezone of the dataset, included
        :param kwargs: kwargs for the data reading function
        """
        super().__init__(nid)
        self._file_path = file_path
        self._timestamp_col = timestamp_col
        self._start_date = start_date
        self._end_date = end_date
        self._kwargs = kwargs

    def _read_data(self) -> None:
        ext = os.path.splitext(self._file_path)[-1]
        if ext == ".csv":
            if "index_col" not in self._kwargs:
                self._kwargs["index_col"] = 0
            read_data = pd.read_csv
        elif ext == ".pq":
            read_data = pd.read_parquet
        else:
            raise ValueError("Invalid file extension='%s'" % ext)
        self.df = read_data(self._file_path, **self._kwargs)

    def _process_data(self) -> None:
        if self._timestamp_col is not None:
            self.df.set_index(self._timestamp_col, inplace=True)
        self.df.index = pd.to_datetime(self.df.index)
        self.df.sort_index(inplace=True)
        dbg.dassert_no_duplicates(self.df.index)
        # fmt: off
        self.df = self.df.loc[self._start_date:self._end_date]
        # fmt: on
        dbg.dassert(not self.df.empty, "Dataframe is empty")

    def _lazy_load(self) -> None:
        if self.df is not None:
            return
        self._read_data()
        self._process_data()

    def fit(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()


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
        self._set_info("fit", info)
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


# #############################################################################
# Transformer nodes
# #############################################################################


class ColumnTransformer(Transformer):
    """
    Perform non-index modifying changes of columns.
    """

    def __init__(
        self,
        nid: str,
        transformer_func: Callable[..., pd.DataFrame],
        # TODO(Paul): Tighten this type annotation.
        transformer_kwargs: Optional[Any] = None,
        # TODO(Paul): May need to assume `List` instead.
        cols: Optional[Iterable[str]] = None,
        col_rename_func: Optional[Callable[[Any], Any]] = None,
        col_mode: Optional[str] = None,
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
        """
        super().__init__(nid)
        if cols is not None:
            dbg.dassert_isinstance(cols, list)
        self._cols = cols
        if col_rename_func is not None:
            dbg.dassert_isinstance(col_rename_func, collections.Callable)
        self._col_rename_func = col_rename_func
        self._col_mode = col_mode or "merge_all"
        self._transformer_func = transformer_func
        self._transformer_kwargs = transformer_kwargs or {}
        # Store the list of columns after the transformation.
        self._transformed_col_names = None

    def transformed_col_names(self) -> List[str]:
        dbg.dassert_is_not(
            self._transformed_col_names,
            None,
            "No transformed column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return self._transformed_col_names

    # TODO(Paul): Add type hints (or rely on parent class?).
    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df_in = df.copy()
        df = df.copy()
        if self._cols is not None:
            df = df[self._cols]
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
        # TODO(Paul): Consider supporting the option of relaxing or
        # foregoing this check.
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the same!",
        )
        # Maybe rename transformed columns.
        if self._col_rename_func is not None:
            df.rename(columns=self._col_rename_func, inplace=True)
        # Store names of transformed columns.
        self._transformed_col_names = df.columns.tolist()
        # Maybe merge transformed columns with a subset of input df columns.
        if self._col_mode == "merge_all":
            dbg.dassert(
                df.columns.intersection(df_in.columns).empty,
                "Transformed column names `%s` conflict with existing column "
                "names `%s`.",
                df.columns,
                df_in.columns,
            )
            df = df_in.merge(df, left_index=True, right_index=True)
        elif self._col_mode == "replace_selected":
            dbg.dassert(
                df.columns.intersection(df_in[self._cols]).empty,
                "Transformed column names `%s` conflict with existing column "
                "names `%s`.",
                df.columns,
                self._cols,
            )
            df = df_in.drop(self._cols, axis=1).merge(
                df, left_index=True, right_index=True
            )
        elif self._col_mode == "replace_all":
            pass
        else:
            dbg.dfatal("Unsupported column mode `%s`", self._col_mode)
        #
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class DataframeMethodRunner(Transformer):
    def __init__(
        self, nid: str, method: str, method_kwargs: Optional[Any] = None
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


class FilterAth(Transformer):
    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        df = fin.filter_ath(df)
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
    ) -> None:
        """
        :param nid: node identifier
        :param rule: resampling frequency passed into
            pd.DataFrame.resample
        :param agg_func: a function that is applied to the resampler
        """
        super().__init__(nid)
        self._rule = rule
        self._agg_func = agg_func

    def _transform(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, collections.OrderedDict]:
        df = df.copy()
        resampler = df.resample(rule=self._rule, closed="left", label="right")
        df = getattr(resampler, self._agg_func)()
        #
        info: collections.OrderedDict[str, Any] = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


# #############################################################################
# Models
# #############################################################################


class ContinuousSkLearnModel(FitPredictNode):
    """
    Fit and predict an sklearn model.
    """

    def __init__(
        self,
        nid: str,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        y_vars: Union[List[str], Callable[[], List[str]]],
        steps_ahead: int,
        model_kwargs: Optional[Any] = None,
    ) -> None:
        """
    Specify the data and sklearn modeling parameters.

    Assumptions:
        :param nid: unique node id
        :param model_func: an sklearn model
        :param x_vars: indexed by knowledge datetimes
            - `x_vars` may contain lags of `y_vars`
        :param y_vars: indexed by knowledge datetimes
            - e.g., in the case of returns, this would correspond to `ret_0`
        :param steps_ahead: number of steps ahead for which a prediction is to
            be generated. E.g.,
                - if `steps_ahead == 0`, then the predictions are
                  are contemporaneous with the observed response (and hence
                  inactionable)
                - if `steps_ahead == 1`, then the model attempts to predict
                  `y_vars` for the next time step
                - The model is only trained to predict the target `steps_ahead`
                  steps ahead (and not all intermediate steps)
        :param model_kwargs: parameters to forward to the sklearn model (e.g.,
            regularization constants)
        """
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._model = None
        self._steps_ahead = steps_ahead
        dbg.dassert_lte(
            0, self._steps_ahead, "Non-causal prediction attempted! Aborting..."
        )

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self._validate_input_df(df_in)
        df = df_in.copy()
        # Obtain index slice for which forward targets exist.
        dbg.dassert_lt(self._steps_ahead, df.index.size)
        idx = df.index[: -self._steps_ahead]
        # Prepare x_vars in sklearn format.
        x_vars = self._to_list(self._x_vars)
        x_fit = adpt.transform_to_sklearn(df.loc[idx], x_vars)
        # Prepare forward y_vars in sklearn format.
        fwd_y_df = self._get_fwd_y_df(df).loc[idx]
        fwd_y_fit = adpt.transform_to_sklearn(fwd_y_df, fwd_y_df.columns.tolist())
        # Define and fit model.
        self._model = self._model_func(**self._model_kwargs)
        self._model = self._model.fit(x_fit, fwd_y_fit)
        # Generate insample predictions and put in dataflow dataframe format.
        fwd_y_hat = self._model.predict(x_fit)
        #
        fwd_y_hat_vars = [
            ContinuousSkLearnModel._insert_to_string(y, "hat")
            for y in fwd_y_df.columns
        ]
        fwd_y_hat = adpt.transform_from_sklearn(idx, fwd_y_hat_vars, fwd_y_hat)
        # TODO(Paul): Summarize model perf or make configurable.
        # TODO(Paul): Consider separating model eval from fit/predict.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        info["insample_perf"] = self._model_perf(fwd_y_df, fwd_y_hat)
        self._set_info("fit", info)
        # Return targets and predictions.
        return {
            "df_out": fwd_y_df.merge(fwd_y_hat, left_index=True, right_index=True)
        }

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self._validate_input_df(df_in)
        df = df_in.copy()
        idx = df.index
        # Transform x_vars to sklearn format.
        x_vars = self._to_list(self._x_vars)
        x_predict = adpt.transform_to_sklearn(df, x_vars)
        # Use trained model to generate predictions.
        dbg.dassert_is_not(
            self._model, None, "Model not found! Check if `fit` has been run."
        )
        fwd_y_hat = self._model.predict(x_predict)
        # Put predictions in dataflow dataframe format.
        fwd_y_df = self._get_fwd_y_df(df)
        fwd_y_hat_vars = [
            ContinuousSkLearnModel._insert_to_string(y, "hat")
            for y in fwd_y_df.columns.tolist()
        ]
        fwd_y_hat = adpt.transform_from_sklearn(idx, fwd_y_hat_vars, fwd_y_hat)
        # Generate basic perf stats.
        info = collections.OrderedDict()
        info["model_perf"] = self._model_perf(fwd_y_df, fwd_y_hat)
        self._set_info("predict", info)
        # Return targets and predictions.
        return {
            "df_out": fwd_y_df.merge(fwd_y_hat, left_index=True, right_index=True)
        }

    @staticmethod
    def _validate_input_df(df: pd.DataFrame) -> None:
        """
        Assert if df violates constraints, otherwise return `None`.
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert(df.index.freq)
        return None

    def _get_fwd_y_df(self, df):
        """
        Return dataframe of `steps_ahead` forward y values.
        """
        y_vars = self._to_list(self._y_vars)
        mapper = lambda y: y + "_%i" % self._steps_ahead
        [mapper(y) for y in y_vars]
        # TODO(Paul): Ensure that `fwd_y_vars` and `y_vars` do not overlap.
        fwd_y_df = df[y_vars].shift(-self._steps_ahead).rename(columns=mapper)
        return fwd_y_df

    # TODO(Paul): Add type hints.
    # TODO(Paul): Consider omitting this (and relying on downstream
    #     processing to e.g., adjust for number of hypotheses tested).
    @staticmethod
    def _model_perf(
        y: pd.DataFrame, y_hat: pd.DataFrame
    ) -> collections.OrderedDict:
        info = collections.OrderedDict()
        # info["hitrate"] = pip._compute_model_hitrate(self.model, x, y)
        pnl_rets = y.multiply(
            y_hat.rename(columns=lambda x: x.replace("_hat", ""))
        )
        info["pnl_rets"] = pnl_rets
        info["sr"] = fin.compute_sharpe_ratio(
            pnl_rets.resample("1B").sum(), time_scaling=252
        )
        return info

    # TODO(Paul): Make this a mixin to use with all modeling nodes.
    @staticmethod
    def _to_list(to_list: Union[List[str], Callable[[], List[str]]]) -> List[str]:
        """
        Return a list given its input.

        - If the input is a list, the output is the same list.
        - If the input is a function that returns a list, then the output of
          the function is returned.

        How this might arise in practice:
          - A ColumnTransformer returns a number of x variables, with the
            number dependent upon a hyperparameter expressed in config
          - The column names of the x variables may be derived from the input
            dataframe column names, not necessarily known until graph execution
            (and not at construction)
          - The ColumnTransformer output columns are merged with its input
            columns (e.g., x vars and y vars are in the same DataFrame)
        Post-merge, we need a way to distinguish the x vars and y vars.
        Allowing a callable here allows us to pass in the ColumnTransformer's
        method `transformed_col_names` and defer the call until graph
        execution.
        """
        if callable(to_list):
            to_list = to_list()
        if isinstance(to_list, list):
            return to_list
        raise TypeError("Data type=`%s`" % type(to_list))

    @staticmethod
    def _insert_to_string(text: str, insertion: str) -> str:
        split = text.rsplit("_", 1)
        return "_".join(split[:-1] + [insertion, split[-1]])


class SkLearnModel(FitPredictNode):
    def __init__(
        self,
        nid: str,
        model_func: Callable[..., Any],
        model_kwargs: Optional[Any] = None,
        x_vars=Union[List[str], Callable[[], List[str]]],
        y_vars=Union[List[str], Callable[[], List[str]]],
    ) -> None:
        super().__init__(nid)
        self._model_func = model_func
        self._model_kwargs = model_kwargs or {}
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._model = None

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_isinstance(df_in, pd.DataFrame)
        dbg.dassert(
            df_in[df_in.isna().any(axis=1)].index.empty,
            "NaNs detected at index `%s`",
            str(df_in[df_in.isna().any(axis=1)].head().index),
        )
        df = df_in.copy()
        idx = df.index
        x_vars, x_fit, y_vars, y_fit = self._to_sklearn_format(df)
        self._model = self._model_func(**self._model_kwargs)
        self._model = self._model.fit(x_fit, y_fit)
        y_hat = self._model.predict(x_fit)
        #
        x_fit, y_fit, y_hat = self._from_sklearn_format(
            idx, x_vars, x_fit, y_vars, y_fit, y_hat
        )
        # TODO(Paul): Summarize model perf or make configurable.
        # TODO(Paul): Consider separating model eval from fit/predict.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        self._set_info("fit", info)
        return {"df_out": y_hat}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_isinstance(df_in, pd.DataFrame)
        df = df_in.copy()
        idx = df.index
        x_vars, x_predict, y_vars, y_predict = self._to_sklearn_format(df)
        dbg.dassert_is_not(
            self._model, None, "Model not found! Check if `fit` has been run."
        )
        y_hat = self._model.predict(x_predict)
        x_predict, y_predict, y_hat = self._from_sklearn_format(
            idx, x_vars, x_predict, y_vars, y_predict, y_hat
        )
        info = collections.OrderedDict()
        info["model_perf"] = self._model_perf(x_predict, y_predict, y_hat)
        self._set_info("predict", info)
        return {"df_out": y_hat}

    # TODO(Paul): Add type hints.
    @staticmethod
    def _model_perf(
        x: pd.DataFrame, y: pd.DataFrame, y_hat: pd.DataFrame
    ) -> collections.OrderedDict:
        _ = x
        info = collections.OrderedDict()
        # info["hitrate"] = pip._compute_model_hitrate(self.model, x, y)
        pnl_rets = y.multiply(y_hat.rename(columns=lambda x: x.strip("_hat")))
        info["pnl_rets"] = pnl_rets
        info["sr"] = fin.compute_sharpe_ratio(
            pnl_rets.resample("1B").sum(), time_scaling=252
        )
        return info

    def _to_sklearn_format(
        self, df: pd.DataFrame
    ) -> Tuple[List[str], np.array, List[str], np.array]:
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        x_vals, y_vals = adpt.transform_to_sklearn_old(df, x_vars, y_vars)
        return x_vars, x_vals, y_vars, y_vals

    @staticmethod
    def _from_sklearn_format(
        idx: pd.Index,
        x_vars: List[str],
        x_vals: np.array,
        y_vars: List[str],
        y_vals: np.array,
        y_hat: np.array,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        x = adpt.transform_from_sklearn(idx, x_vars, x_vals)
        y = adpt.transform_from_sklearn(idx, y_vars, y_vals)
        y_h = adpt.transform_from_sklearn(
            idx, [y + "_hat" for y in y_vars], y_hat
        )
        return x, y, y_h

    @staticmethod
    def _to_list(to_list: Union[List[str], Callable[[], List[str]]]) -> List[str]:
        """
        Return a list given its input.

        - If the input is a list, the output is the same list.
        - If the input is a function that returns a list, then the output of
          the function is returned.

        How this might arise in practice:
          - A ColumnTransformer returns a number of x variables, with the
            number dependent upon a hyperparameter expressed in config
          - The column names of the x variables may be derived from the input
            dataframe column names, not necessarily known until graph execution
            (and not at construction)
          - The ColumnTransformer output columns are merged with its input
            columns (e.g., x vars and y vars are in the same DataFrame)
        Post-merge, we need a way to distinguish the x vars and y vars.
        Allowing a callable here allows us to pass in the ColumnTransformer's
        method `transformed_col_names` and defer the call until graph
        execution.
        """
        if callable(to_list):
            to_list = to_list()
        if isinstance(to_list, list):
            return to_list
        raise TypeError("Data type=`%s`" % type(to_list))


class ContinuousDeepArModel(FitPredictNode):
    """
    A dataflow node for a DeepAR model.

    This node trains a DeepAR model using only one time series
    - By using only one time series, we are not taking advantage of the
      "global" modeling capabilities of gluonts or DeepAR
    - This may be somewhat mitigated by the fact that the single time series
      that we provide will typically contain on the order of 10E5 or more time
      points
    - In training, DeepAR randomly cuts the time series provided, and so
      unless there are obvious cut-points we want to take advantage of, it may
      be best to let DeepAR cut
    - If certain cut-points are naturally more appropriate in our problem
      domain, an event study modeling approach may be more suitable

    See https://arxiv.org/abs/1704.04110 for a description of the DeepAR model.

    For additional context and best-practices, see
    https://github.com/ParticleDev/commodity_research/issues/966
    """

    def __init__(
        self,
        nid: str,
        y_vars: Union[List[str], Callable[[], List[str]]],
        trainer_kwargs: Optional[Any] = None,
        estimator_kwargs: Optional[Any] = None,
        x_vars: Union[List[str], Callable[[], List[str]]] = None,
        num_traces: int = 100,
    ) -> None:
        """
        Initialize dataflow node for gluon-ts DeepAR model.

        :param nid: unique node id
        :param y_vars: Used in autoregression
        :param trainer_kwargs: See
          - https://gluon-ts.mxnet.io/api/gluonts/gluonts.trainer.html#gluonts.trainer.Trainer
          - https://github.com/awslabs/gluon-ts/blob/master/src/gluonts/trainer/_base.py
        :param estimator_kwargs: See
          - https://gluon-ts.mxnet.io/api/gluonts/gluonts.model.deepar.html
          - https://github.com/awslabs/gluon-ts/blob/master/src/gluonts/model/deepar/_estimator.py
        :param x_vars: Covariates. Could be, e.g., features associated with a
            point-in-time event. Must be known throughout the prediction
            window at the time the prediction is made. May be omitted.
        :num_traces: Number of sample paths / traces to generate per
            prediction. The mean of the traces is used as the prediction.
        """
        super().__init__(nid)
        self._estimator_kwargs = estimator_kwargs
        # To avoid passing a class through config, handle `Trainer()`
        # parameters separately from `estimator_kwargs`.
        self._trainer_kwargs = trainer_kwargs
        self._trainer = gt.Trainer(**self._trainer_kwargs)
        dbg.dassert_not_in("trainer", self._estimator_kwargs)
        #
        self._estimator_func = gmd.DeepAREstimator
        # NOTE: Covariates (x_vars) are not required by DeepAR.
        #   - This could be useful for, e.g., predicting future values of
        #     what would normally be predictors
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._num_traces = num_traces
        self._estimator = None
        self._predictor = None
        #
        dbg.dassert_in("prediction_length", self._estimator_kwargs)
        self._prediction_length = self._estimator_kwargs["prediction_length"]
        dbg.dassert_lt(0, self._prediction_length)
        dbg.dassert_not_in(
            "freq",
            self._estimator_kwargs,
            "`freq` to be autoinferred from `df_in`; do not specify",
        )

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self._validate_input_df(df_in)
        df = df_in.copy()
        # Obtain index slice for which forward targets exist.
        dbg.dassert_lt(self._prediction_length, df.index.size)
        df_fit = df.iloc[: -self._prediction_length]
        #
        if self._x_vars is not None:
            x_vars = self._to_list(self._x_vars)
        else:
            x_vars = None
        y_vars = self._to_list(self._y_vars)
        # Transform dataflow local timeseries dataframe into gluon-ts format.
        gluon_train = adpt.transform_to_gluon(
            df_fit, x_vars, y_vars, df_fit.index.freq.freqstr
        )
        # Instantiate the (DeepAR) estimator and train the model.
        self._estimator = self._estimator_func(
            trainer=self._trainer,
            freq=df_fit.index.freq.freqstr,
            **self._estimator_kwargs,
        )
        self._predictor = self._estimator.train(gluon_train)
        # Predict. Generate predictions over all of `df_in` (not just on the
        #     restricted slice `df_fit`).
        fwd_y_hat, fwd_y = bcktst.generate_predictions(
            predictor=self._predictor,
            df=df,
            y_vars=y_vars,
            prediction_length=self._prediction_length,
            num_samples=self._num_traces,
            x_vars=x_vars,
        )
        # Store info.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        self._set_info("fit", info)
        return {
            "df_out": fwd_y.merge(fwd_y_hat, left_index=True, right_index=True)
        }

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self._validate_input_df(df_in)
        df = df_in.copy()
        if self._x_vars is not None:
            x_vars = self._to_list(self._x_vars)
        else:
            x_vars = None
        y_vars = self._to_list(self._y_vars)
        gluon_train = adpt.transform_to_gluon(
            df, x_vars, y_vars, df.index.freq.freqstr
        )
        # Instantiate the (DeepAR) estimator and train the model.
        self._estimator = self._estimator_func(
            trainer=self._trainer,
            freq=df.index.freq.freqstr,
            **self._estimator_kwargs,
        )
        self._predictor = self._estimator.train(gluon_train)
        #
        fwd_y_hat, fwd_y = bcktst.generate_predictions(
            predictor=self._predictor,
            df=df,
            y_vars=y_vars,
            prediction_length=self._prediction_length,
            num_samples=self._num_traces,
            x_vars=x_vars,
        )
        # Store info.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        self._set_info("predict", info)
        return {
            "df_out": fwd_y.merge(fwd_y_hat, left_index=True, right_index=True)
        }

    @staticmethod
    def _validate_input_df(df: pd.DataFrame) -> None:
        """
        Assert if df violates constraints, otherwise return `None`.
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert(df.index.freq)
        return None

    def _get_fwd_y_df(self, df):
        """
        Return dataframe of `steps_ahead` forward y values.
        """
        y_vars = self._to_list(self._y_vars)
        mapper = lambda y: y + "_%i" % self._prediction_length
        [mapper(y) for y in y_vars]
        # TODO(Paul): Ensure that `fwd_y_vars` and `y_vars` do not overlap.
        fwd_y_df = (
            df[y_vars].shift(-self._prediction_length).rename(columns=mapper)
        )
        return fwd_y_df

    @staticmethod
    def _to_list(to_list: Union[List[str], Callable[[], List[str]]]) -> List[str]:
        """
        As in `SkLearnNode` version.

        TODO(Paul): Think about factoring this method out into a parent/mixin.
        """
        if callable(to_list):
            to_list = to_list()
        if isinstance(to_list, list):
            return to_list
        raise TypeError("Data type=`%s`" % type(to_list))


class DeepARGlobalModel(FitPredictNode):
    """
    A dataflow node for a DeepAR model.

    See https://arxiv.org/abs/1704.04110 for a description of the DeepAR model.

    For additional context and best-practices, see
    https://github.com/ParticleDev/commodity_research/issues/966
    """

    def __init__(
        self,
        nid: str,
        trainer_kwargs: Optional[Any] = None,
        estimator_kwargs: Optional[Any] = None,
        x_vars=Union[List[str], Callable[[], List[str]]],
        y_vars=Union[List[str], Callable[[], List[str]]],
    ) -> None:
        """
        Initialize dataflow node for gluon-ts DeepAR model.

        :param nid: unique node id
        :param trainer_kwargs: See
          - https://gluon-ts.mxnet.io/api/gluonts/gluonts.trainer.html#gluonts.trainer.Trainer
          - https://github.com/awslabs/gluon-ts/blob/master/src/gluonts/trainer/_base.py
        :param estimator_kwargs: See
          - https://gluon-ts.mxnet.io/api/gluonts/gluonts.model.deepar.html
          - https://github.com/awslabs/gluon-ts/blob/master/src/gluonts/model/deepar/_estimator.py
        :param x_vars: Covariates. Could be, e.g., features associated with a
            point-in-time event. Must be known throughout the prediction
            window at the time the prediction is made.
        :param y_vars: Used in autoregression
        """
        super().__init__(nid)
        self._estimator_kwargs = estimator_kwargs
        # To avoid passing a class through config, handle `Trainer()`
        # parameters separately from `estimator_kwargs`.
        self._trainer_kwargs = trainer_kwargs
        self._trainer = gt.Trainer(**self._trainer_kwargs)
        dbg.dassert_not_in("trainer", self._estimator_kwargs)
        #
        self._estimator_func = gmd.DeepAREstimator
        # NOTE: Covariates (x_vars) are not required by DeepAR.
        # TODO(Paul): Allow this model to accept y_vars only.
        #   - This could be useful for, e.g., predicting future values of
        #     what would normally be predictors
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._estimator = None
        self._predictor = None
        # We determine `prediction_length` automatically and therefore do not
        # allow it to be set by the user.
        dbg.dassert_not_in("prediction_length", self._estimator_kwargs)
        #
        dbg.dassert_in("freq", self._estimator_kwargs)
        self._freq = self._estimator_kwargs["freq"]

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Fit model to multiple series reflected in multiindexed `df_in`.


        `prediction_length` is autoinferred from the max index of `t_j`, e.g.,
        each `df_in` is assumed to include the index `0` for, e.g.,
        "event time", and indices are assumed to be consecutive integers. So
        if there are time points

            t_{-2} < t_{-1} < t_0 < t_1 < t_2

        then `prediction_length = 2`.
        """
        dbg.dassert_isinstance(df_in, pd.DataFrame)
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        df = df_in.copy()
        # Transform dataflow local timeseries dataframe into gluon-ts format.
        gluon_train = adpt.transform_to_gluon(df, x_vars, y_vars, self._freq)
        # Set the prediction length to the length of the local timeseries - 1.
        #   - To predict for time t_j at time t_i, t_j > t_i, we need to know
        #     x_vars up to and including time t_j
        #   - For this model, multi-step predictions are equivalent to
        #     iterated single-step predictions
        self._prediction_length = df.index.get_level_values(0).max()
        # Instantiate the (DeepAR) estimator and train the model.
        self._estimator = self._estimator_func(
            prediction_length=self._prediction_length,
            trainer=self._trainer,
            **self._estimator_kwargs,
        )
        self._predictor = self._estimator.train(gluon_train)
        # Apply model predictions to the training set (so that we can evaluate
        # in-sample performance).
        #   - Include all data points up to and including zero (the event time)
        pd.IndexSlice
        gluon_test = adpt.transform_to_gluon(
            df, x_vars, y_vars, self._freq, self._prediction_length
        )
        fit_predictions = list(self._predictor.predict(gluon_test))
        # Transform gluon-ts predictions into a dataflow local timeseries
        # dataframe.
        # TODO(Paul): Gluon has built-in functionality to take the mean of
        #     traces, and we might consider using it instead.
        y_hat_traces = adpt.transform_from_gluon_forecasts(fit_predictions)
        # TODO(Paul): Store the traces / dispersion estimates.
        # Average over all available samples.
        y_hat = y_hat_traces.mean(level=[0, 1])
        # Map multiindices to align our prediction indices with those used
        # by the passed-in local timeseries dataframe.
        # TODO(Paul): Do this mapping earlier before removing the traces.
        aligned_idx = y_hat.index.map(
            lambda x: (x[0] + 1, x[1] - pd.Timedelta(f"1{self._freq}"),)
        )
        y_hat.index = aligned_idx
        y_hat.name = y_vars[0] + "_hat"
        y_hat.index.rename(df.index.names, inplace=True)
        # Store info.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        # TODO(Paul): Consider storing only the head of each list in `info`
        #     for debugging purposes.
        # info["gluon_train"] = list(gluon_train)
        # info["gluon_test"] = list(gluon_test)
        # info["fit_predictions"] = fit_predictions
        self._set_info("fit", info)
        return {"df_out": y_hat.to_frame()}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_isinstance(df_in, pd.DataFrame)
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        df = df_in.copy()
        # Transform dataflow local timeseries dataframe into gluon-ts format.
        pd.IndexSlice
        gluon_test = adpt.transform_to_gluon(
            df, x_vars, y_vars, self._freq, self._prediction_length,
        )
        predictions = list(self._predictor.predict(gluon_test))
        # Transform gluon-ts predictions into a dataflow local timeseries
        # dataframe.
        # TODO(Paul): Gluon has built-in functionality to take the mean of
        #     traces, and we might consider using it instead.
        y_hat_traces = adpt.transform_from_gluon_forecasts(predictions)
        # TODO(Paul): Store the traces / dispersion estimates.
        # Average over all available samples.
        y_hat = y_hat_traces.mean(level=[0, 1])
        # Map multiindices to align our prediction indices with those used
        # by the passed-in local timeseries dataframe.
        # TODO(Paul): Do this mapping earlier before removing the traces.
        aligned_idx = y_hat.index.map(
            lambda x: (x[0] + 1, x[1] - pd.Timedelta(f"1{self._freq}"),)
        )
        y_hat.index = aligned_idx
        y_hat.name = y_vars[0] + "_hat"
        y_hat.index.rename(df.index.names, inplace=True)
        # Store info.
        info = collections.OrderedDict()
        info["model_x_vars"] = x_vars
        # TODO(Paul): Consider storing only the head of each list in `info`
        #     for debugging purposes.
        # info["gluon_train"] = list(gluon_train)
        # info["gluon_test"] = list(gluon_test)
        # info["fit_predictions"] = fit_predictions
        self._set_info("predict", info)
        return {"df_out": y_hat.to_frame()}

    @staticmethod
    def _to_list(to_list: Union[List[str], Callable[[], List[str]]]) -> List[str]:
        """
        As in `SkLearnNode` version.

        TODO(Paul): Think about factoring this method out into a parent/mixin.
        """
        if callable(to_list):
            to_list = to_list()
        if isinstance(to_list, list):
            return to_list
        raise TypeError("Data type=`%s`" % type(to_list))


# #############################################################################
# Cross-validation
# #############################################################################


def _get_source_idxs(dag: DAG, mode: Optional[str] = None) -> Dict[str, pd.Index]:
    """
    Warm up source nodes and extract dataframe indices.

    :param mode: Determines how source indices extracted from dataframes
        - `default`: return index as-is
        - `dropna`: drop NaNs (any) and then return index
        - `ffill_dropna`: forward fill NaNs, drop leading NaNs, then return
                          index
    """
    mode = mode or "default"
    # Warm up source nodes to get dataframes from which we can generate splits.
    source_nids = dag.get_sources()
    for nid in source_nids:
        dag.run_leq_node(nid, "fit")
    # Collect source dataframe indices.
    source_idxs = {}
    for nid in source_nids:
        df = dag.get_node(nid).get_df()
        if mode == "default":
            source_idxs[nid] = df.index
        elif mode == "dropna":
            source_idxs[nid] = df.dropna().index
        elif mode == "ffill_dropna":
            source_idxs[nid] = df.fillna(method="ffill").dropna().index
        else:
            raise ValueError("Unsupported mode `%s`" % mode)
    return source_idxs


# TODO(Paul): Formalize what this returns and what can be done with it.
def cross_validate(
    dag: DAG,
    split_func: Callable,
    split_func_kwargs: Dict,
    idx_mode: Optional[str] = None,
) -> collections.OrderedDict:
    """
    Generate splits, run train/test, collect info.

    :param idx_mode: same meaning as mode in `_get_source_idxs`
    :return: DAG info for each split, keyed by split
    """
    # Get dataframe indices of source nodes.
    source_idxs = _get_source_idxs(dag, mode=idx_mode)
    composite_idx = stats.combine_indices(source_idxs.values())
    # Generate cross-validation splits from
    splits = split_func(composite_idx, **split_func_kwargs)
    _LOG.debug(stats.convert_splits_to_string(splits))
    #
    result_bundle = collections.OrderedDict()
    # TODO(Paul): rename train/test to fit/predict.
    for i, (train_idxs, test_idxs) in enumerate(splits):
        split_info = collections.OrderedDict()
        for nid, idx in source_idxs.items():
            node_info = collections.OrderedDict()
            node_train_idxs = idx.intersection(train_idxs)
            dbg.dassert_lte(1, node_train_idxs.size)
            node_info["fit_idxs"] = node_train_idxs
            dag.get_node(nid).set_fit_idxs(node_train_idxs)
            #
            node_test_idxs = idx.intersection(test_idxs)
            dbg.dassert_lte(1, node_test_idxs.size)
            node_info["predict_idxs"] = node_test_idxs
            dag.get_node(nid).set_predict_idxs(node_test_idxs)
            #
            split_info[nid] = node_info
        dag.run_dag("fit")
        dag.run_dag("predict")
        #
        split_info["stages"] = extract_info(dag, ["fit", "predict"])
        result_bundle["split_" + str(i)] = split_info
    return result_bundle


def process_result_bundle(result_bundle: Dict) -> collections.OrderedDict:
    info = collections.OrderedDict()
    split_names = []
    model_coeffs = []
    model_x_vars = []
    pnl_rets = []
    for split in result_bundle.keys():
        split_names.append(split)
        # model_coeffs.append(
        #    result_bundle[split]["stages"]["model"]["fit"]["model_coeffs"]
        # )
        model_x_vars.append(
            result_bundle[split]["stages"]["model"]["fit"]["model_x_vars"]
        )
        pnl_rets.append(
            result_bundle[split]["stages"]["model"]["predict"]["model_perf"][
                "pnl_rets"
            ]
        )
    model_df = pd.DataFrame(
        model_coeffs, index=split_names, columns=model_x_vars[0]
    )
    pnl_rets = pd.concat(pnl_rets)
    sr = fin.compute_sharpe_ratio(pnl_rets.resample("1B").sum(), time_scaling=252)
    info["model_df"] = copy.copy(model_df)
    info["pnl_rets"] = copy.copy(pnl_rets)
    info["sr"] = copy.copy(sr)
    return info


def get_df_info_as_string(df: pd.DataFrame) -> str:
    buffer = io.StringIO()
    df.info(buf=buffer)
    return buffer.getvalue()
