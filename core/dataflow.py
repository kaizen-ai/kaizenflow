"""
Import as:

import core.dataflow as dtf
"""

import abc
import collections
import copy
import io
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import networkx as nx
import pandas as pd

import core.finance as fin
import core.statistics as stats
import helpers.dbg as dbg

# TODO(*): This is an exception to the rule waiting for PartTask553.
from core.dataflow_core import DAG, Node

_LOG = logging.getLogger(__name__)


# #############################################################################
# DAG visiting
# #############################################################################


def draw(graph, flip_across_vertical=False, seed=1):
    kpos = nx.kamada_kawai_layout(graph)
    if flip_across_vertical:
        kpos = {node: (-x, y) for (node, (x, y)) in kpos.items()}
    pos = nx.spring_layout(graph, pos=kpos, seed=seed)
    nx.draw_networkx(
        graph, pos=pos, node_size=3000, arrowsize=30, width=1.5
    )


def extract_info(dag, methods):
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

    def __init__(self, nid, inputs=None, outputs=None):
        if inputs is None:
            inputs = ["df_in"]
        if outputs is None:
            outputs = ["df_out"]
        super().__init__(nid=nid, inputs=inputs, outputs=outputs)
        self._info = collections.OrderedDict()

    @abc.abstractmethod
    def fit(self, df_in):
        pass

    @abc.abstractmethod
    def predict(self, df_in):
        pass

    def get_info(self, method):
        # TODO(Paul): Add a dassert_getattr function to use here and in core.
        dbg.dassert_isinstance(method, str)
        dbg.dassert(getattr(self, method))
        if method in self._info.keys():
            return self._info[method]
        # TODO(Paul): Maybe crash if there is no info.
        _LOG.warning("No info found for nid=%s, method=%s", self.nid, method)
        return None

    def _set_info(self, method, values):
        dbg.dassert_isinstance(method, str)
        dbg.dassert(getattr(self, method))
        dbg.dassert_isinstance(values, collections.OrderedDict)
        # Save the info in the node: we make a copy just to be safe.
        self._info[method] = copy.copy(values)


class DataSource(FitPredictNode, abc.ABC):
    """
    A source node that can be configured for cross-validation.
    """

    def __init__(self, nid, outputs=None):
        if outputs is None:
            outputs = ["df_out"]
        # Do not allow any empty list.
        dbg.dassert(outputs)
        super().__init__(nid, inputs=[], outputs=outputs)
        #
        self.df = None
        self._fit_idxs = None
        self._predict_idxs = None

    def set_fit_idxs(self, fit_idxs):
        """
        :param fit_idxs: indices of the df to use for fitting
        """
        self._fit_idxs = fit_idxs

    # DataSource does not have a `df_in` in either `fit` or `predict` as a
    # typical `FitPredictNode` does.
    # pylint: disable=arguments-differ
    def fit(self):
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

    def set_predict_idxs(self, predict_idxs):
        """
        :param predict_idxs: indices of the df to use for predicting
        """
        self._predict_idxs = predict_idxs

    # pylint: disable=arguments-differ
    def predict(self):
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

    def get_df(self):
        dbg.dassert_is_not(self.df, None, "No DataFrame found!")
        return self.df


class Transformer(FitPredictNode, abc.ABC):
    """
    Stateless Single-Input Single-Output node.
    """

    # TODO(Paul): Consider giving users the option of renaming the single
    # input and single output (but verify there is only one of each).
    def __init__(self, nid):
        super().__init__(nid)

    @abc.abstractmethod
    def _transform(self, df):
        """
        :return: df, info
        """

    def fit(self, df_in):
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in):
        # Transform the input df.
        df_out, info = self._transform(df_in)
        self._set_info("predict", info)
        return {"df_out": df_out}


# #############################################################################
# Data source nodes
# #############################################################################


class ReadDataFromDf(DataSource):
    def __init__(self, nid, df):
        super().__init__(nid)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


# #############################################################################
# Plumbing nodes
# #############################################################################


class Merger(FitPredictNode):
    """
    Performs a merge of two inputs.
    """

    # TODO(Paul): Support different input/output names.
    def __init__(self, nid: str, merge_kwargs: Optional[Any] = None) -> None:
        """
        Configure dataframe merging policy.

        :param nid: unique node id
        :param merge_kwargs: arguments to pd.merge
        """
        super().__init__(nid, inputs=["df_in1", "df_in2"])
        self._merge_kwargs = merge_kwargs or {}
        self._df_in1_col_names = None
        self._df_in2_col_names = None

    def df_in1_col_names(self) -> List[str]:
        dbg.dassert_is_not(
            self._df_in1_col_names,
            None,
            "No column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return self._df_in1_col_names

    def df_in2_col_names(self) -> List[str]:
        # TODO(Paul): Factor out.
        dbg.dassert_is_not(
            self._df_in2_col_names,
            None,
            "No column names. This may indicate "
            "an invocation prior to graph execution.",
        )
        return self._df_in2_col_names

    # pylint: disable=arguments-differ
    def fit(self, df_in1, df_in2):
        df_out, info = self._merge(df_in1, df_in2)
        self._set_info("fit", info)
        return {"df_out": df_out}

    # pylint: disable=arguments-differ
    def predict(self, df_in1, df_in2):
        df_out, info = self._merge(df_in1, df_in2)
        self._set_info("fit", info)
        return {"df_out": df_out}

    def _merge(self, df_in1, df_in2):
        self._df_in1_col_names = df_in1.columns.tolist()
        self._df_in2_col_names = df_in2.columns.tolist()
        # TODO(Paul): Add meaningful info.
        df_out = df_in1.merge(df_in2, **self._merge_kwargs)
        info = collections.OrderedDict()
        info["df_merged_info"] = get_df_info_as_string(df_out)
        return df_out, info


# #############################################################################
# Transformer nodes
# #############################################################################


class ColumnTransformer(Transformer):
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
        Perform non-index modifying changes of columns.

        :param nid: unique node id
        :param transformer_func: df -> df
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
    def _transform(self, df):
        df_in = df.copy()
        df = df.copy()
        if self._cols is not None:
            df = df[self._cols]
        # Perform the column transformation operations.
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
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class DataframeMethodRunner(Transformer):
    def __init__(
        self, nid: str, method: str, method_kwargs: Optional[Any] = None
    ):
        super().__init__(nid)
        dbg.dassert(method)
        # TODO(Paul): Ensure that this is a valid method.
        self._method = method
        self._method_kwargs = method_kwargs or {}

    def _transform(self, df):
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
    def _transform(self, df):
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
    ):
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
        idx, x_vars, x_fit, y_vars, y_fit = self._to_sklearn_format(df)
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
        idx, x_vars, x_predict, y_vars, y_predict = self._to_sklearn_format(df)
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
    def _model_perf(x, y, y_hat):
        _ = x
        info = collections.OrderedDict()
        # info["hitrate"] = pip._compute_model_hitrate(self.model, x, y)
        pnl_rets = y.multiply(y_hat.rename(columns=lambda x: x.strip("_hat")))
        info["pnl_rets"] = pnl_rets
        info["sr"] = fin.compute_sharpe_ratio(
            pnl_rets.resample("1B").sum(), time_scaling=252
        )
        return info

    # TODO(Paul): Add type hints.
    def _to_sklearn_format(self, df):
        idx = df.index
        df = df.reset_index()
        # TODO(Paul): replace with class name
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        x_vals = df[x_vars]
        y_vals = df[y_vars]
        return idx, x_vars, x_vals, y_vars, y_vals

    # TODO(Paul): Add type hints.
    @staticmethod
    def _from_sklearn_format(idx, x_vars, x_vals, y_vars, y_vals, y_hat):
        x = pd.DataFrame(x_vals.values, index=idx, columns=x_vars)
        y = pd.DataFrame(y_vals.values, index=idx, columns=y_vars)
        y_h = pd.DataFrame(y_hat, index=idx, columns=[y + "_hat" for y in y_vars])
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


# #############################################################################
# Cross-validation
# #############################################################################


def _get_source_idxs(dag: DAG, mode: Optional[str] = None) -> Dict[str, pd.Index]:
    """
    Warm up source nodes and extract dataframe indices.
    """
    if mode is None:
        mode = "default"
    # Warm up source nodes to get dataframes from which we can generate splits.
    source_nids = dag.get_sources()
    for nid in source_nids:
        dag.run_leq_node(nid, "fit")
    # Collect source dataframe indices.
    source_idxs = {}
    for nid in source_nids:
        if mode == "default":
            source_idxs[nid] = dag.get_node(nid).get_df().index
        elif mode == "dropna":
            source_idxs[nid] = dag.get_node(nid).get_df().dropna().index
        elif mode == "ffill":
            source_idxs[nid] = dag.get_node(nid).get_df().fillna(method="ffill").index
        elif mode == "ffill_dropna":
            source_idxs[nid] = dag.get_node(nid).get_df().fillna(method="ffill").dropna().index
        else:
            raise ValueError("Unsupported mode `%s`", mode)
    return source_idxs


# TODO(Paul): Formalize what this returns and what can be done with it.
def cross_validate(dag, split_func, split_func_kwargs, idx_mode=None):
    """
    Generate splits, run train/test, collect info.

    :return: DAG info for each split, keyed by split.
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


def process_result_bundle(result_bundle):
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


def get_df_info_as_string(df):
    buffer = io.StringIO()
    df.info(buf=buffer)
    return buffer.getvalue()
