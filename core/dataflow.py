import abc
import collections
import copy
import io
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import networkx as nx
import pandas as pd

import core.finance as fin
import helpers.dbg as dbg
import rolling_model.pipeline as pip
from core.dataflow_core import DAG  # pylint: disable=unused-import
from core.dataflow_core import Node

_LOG = logging.getLogger(__name__)


# #############################################################################
# DAG visiting
# #############################################################################


def draw(graph):
    pos = nx.kamada_kawai_layout(graph)
    flipped_pos = {node: (-x, y) for (node, (x, y)) in pos.items()}
    nx.draw_networkx(
        graph, pos=flipped_pos, node_size=3000, arrowsize=30, width=1.5
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


class SkLearnNode(Node, abc.ABC):
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
        self._info[method] = copy.copy(values)


class DataSource(SkLearnNode, abc.ABC):
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
    # typical `SkLearnNode` does.
    # pylint: disable=arguments-differ
    def fit(self):
        """
        :return: training set as df
        """
        if self._fit_idxs is not None:
            fit_df = self.df.loc[self._fit_idxs]
        else:
            fit_df = self.df
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
            predict_df = self.df.loc[self._predict_idxs]
        else:
            predict_df = self.df
        info = collections.OrderedDict()
        info["predict_df_info"] = get_df_info_as_string(predict_df)
        self._set_info("predict", info)
        return {self.output_names[0]: predict_df}

    def get_df(self):
        dbg.dassert_is_not(self.df, None, "No DataFrame found!")
        return self.df


class Transformer(SkLearnNode, abc.ABC):
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
        # Save the info in the node: we make a copy just to be safe.
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in):
        # Transform the input df.
        df_out, info = self._transform(df_in)
        # Save the info in the node: we make a copy just to be safe.
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


class Merger(SkLearnNode):
    """
    Performs a merge of two inputs.
    """

    # TODO(Paul): Support different input/output names.
    def __init__(self, nid: str, merge_kwargs: Optional[Any] = None) -> None:
        super().__init__(nid, inputs=["df_in1", "df_in2"])
        if merge_kwargs is not None:
            self._merge_kwargs = merge_kwargs
        else:
            self._merge_kwargs = {}
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

    def fit(self, df_in1, df_in2):
        df_out, info = self._merge(df_in1, df_in2)
        # Save the info in the node: we make a copy just to be safe.
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in1, df_in2):
        df_out, info = self._merge(df_in1, df_in2)
        # Save the info in the node: we make a copy just to be safe.
        self._set_info("fit", info)
        return {"df_out": df_out}

    def _merge(self, df_in1, df_in2):
        self._df_in1_col_names = df_in1.columns.tolist()
        self._df_in2_col_names = df_in2.columns.tolist()
        # TODO((Paul): Add meaningful info.
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
        Performs non-index modifying changes of columns.

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
        if col_mode is None:
            self._col_mode = "merge_all"
        else:
            self._col_mode = col_mode
        self._transformer_func = transformer_func
        if transformer_kwargs is not None:
            self._transformer_kwargs = transformer_kwargs
        else:
            # TODO(Paul): Revisit case where input val is None.
            self._transformer_kwargs = {}
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
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


# #############################################################################
# Models
# #############################################################################


class SkLearnModel(SkLearnNode):
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
        if model_kwargs is not None:
            self._model_kwargs = model_kwargs
        else:
            self._model_kwargs = {}
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._model = None

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_isinstance(df_in, pd.DataFrame)
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
        # Drop NaNs and prepare the index for sklearn.
        df = df.dropna()
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
        Returns a list given its input.

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


# TODO(Paul): Formalize what this returns and what can be done with it.
def cross_validate(config, source_nid, sink_nid, dag):
    """
    Generates splits, runs train/test, collects info.

    :return: DAG info for each split, keyed by split.
    """
    # Warm up source node to get a dataframe from which we can generate splits.
    source_node = dag.get_node(source_nid)
    dag.run_leq_node(source_nid, "fit")
    df = source_node.get_df()
    splits = pip.get_time_series_rolling_folds(df, config["cv_n_splits"])
    #
    result_bundle = collections.OrderedDict()
    #
    for i, (train_idxs, test_idxs) in enumerate(splits):
        split_info = collections.OrderedDict()
        split_info["fit_idxs"] = train_idxs
        split_info["predict_idxs"] = test_idxs
        #
        source_node.set_fit_idxs(train_idxs)
        dag.run_leq_node(sink_nid, "fit")
        #
        source_node.set_predict_idxs(test_idxs)
        dag.run_leq_node(sink_nid, "predict")
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


# TODO(Paul): Move these to `helpers/iterator.py` and add tests.
def get_nested_dict_iterator(nested, path=None):
    """
    Return nested dictionary iterator.

    :param nested: nested dictionary
    :param path: path to top of tree
    :return: path to leaf node, value
    """
    if path is None:
        path = []
    for key, value in nested.items():
        local_path = path + [key]
        if isinstance(value, collections.abc.Mapping):
            yield from get_nested_dict_iterator(value, local_path)
        else:
            yield local_path, value


def extract_leaf_values(nested, key):
    """
    Extract leaf values with key matching `key`.

    :param nested: nested dictionary
    :param key: leaf key value to match
    :return: dict with key = path as tuple, value = leaf value
    """
    d = {}
    for item in get_nested_dict_iterator(nested):
        if item[0][-1] == key:
            d[tuple(item[0])] = item[1]
    return d


def flatten_nested_dict(nested):
    d = {}
    for item in get_nested_dict_iterator(nested):
        d[".".join(item[0])] = item[1]
    return d


def get_df_info_as_string(df):
    buffer = io.StringIO()
    df.info(buf=buffer)
    return buffer.getvalue()
