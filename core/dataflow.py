import abc
import collections
import copy
import io
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import networkx as nx
import pandas as pd
from sklearn import linear_model

import core.features as ftrs
import core.finance as fin
import helpers.dbg as dbg
import rolling_model.pipeline as pip
import vendors.kibot.utils as kut
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

    # TODO(Paul): Decide what to do about the fact that we override the
    # superclass function interface.
    def fit(self):
        """
        :return: training set as df
        """
        if self._fit_idxs:
            fit_df = self.df.iloc[self._fit_idxs]
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

    def predict(self):
        """
        :return: test set as df
        """
        if self._predict_idxs:
            predict_df = self.df.iloc[self._predict_idxs]
        else:
            predict_df = self.df
        info = collections.OrderedDict()
        info["predict_df_info"] = get_df_info_as_string(predict_df)
        self._set_info("predict", info)
        return {self.output_names[0]: predict_df}

    def get_df(self):
        dbg.dassert_is_not(self.df, None)
        return self.df


class Transformer(SkLearnNode, abc.ABC):
    """
    Stateless Single-Input Single-Output node.
    """

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


class ReadDataFromKibot(DataSource):
    def __init__(self, nid, file_name, nrows):
        super().__init__(nid)
        # dbg.dassert_exists(file_name)
        self._file_name = file_name
        self._nrows = nrows
        #
        self.df = None

    def _lazy_load(self):
        if self.df is None:
            self.df = kut.read_data(self._file_name, self._nrows)

    def fit(self):
        """
        :return: training set as df
        """
        self._lazy_load()
        return super().fit()


# #############################################################################
# Transformer nodes
# #############################################################################


# TODO(Paul): Consider having more kinds of `Transformer` objects, e.g.,
#   those that transform cols [or subset] -> cols, and those that change the
#   index. Different broad considerations may apply (e.g., in the col case, we
#   want the option to propagate the original columns or not).


# TODO(Paul): Add a method to get the output column names.
class ColumnTransformer(Transformer):
    def __init__(
        self,
        nid: str,
        transformer_func: Callable[..., pd.DataFrame],
        # TODO(Paul): Tighten this type annotation.
        transformer_kwargs: Optional[Any] = None,
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
        self._cols = cols
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
        self._transformed_col_names = None

    def transformed_col_names(self):
        # TODO(Paul): Consider raising if `None`.
        return self._transformed_col_names

    def _transform(self, df):
        df_in = df.copy()
        df = df.copy()
        if self._cols is not None:
            df = df[self._cols]
        # Perform the column transformation operations.
        df.columns
        df = self._transformer_func(df, **self._transformer_kwargs)
        dbg.dassert(
            df.index.equals(df_in.index),
            "Input/output indices differ but are expected to be the " "same!",
        )
        # Maybe rename transformed columns.
        if self._col_rename_func is not None:
            dbg.dassert_isinstance(self._col_rename_func, collections.Callable)
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
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df = df.copy()
        df = fin.filter_ath(df)
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class Resample(Transformer):
    def __init__(self, nid, rule):
        super().__init__(nid)
        self._rule = rule

    def _transform(self, df):
        df = df.copy()
        df = df.resample(rule=self._rule, closed="left", label="right").sum()
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class ComputeLaggedFeatures(Transformer):
    def __init__(self, nid, y_var, delay_lag, num_lags):
        super().__init__(nid)
        self.y_var = y_var
        self.delay_lag = delay_lag
        self.num_lags = num_lags

    def get_x_vars(self):
        x_vars = ftrs.get_lagged_feature_names(
            self.y_var, self.delay_lag, self.num_lags
        )
        return x_vars

    # TODO(Paul): don't change the index in this node; remove this method
    def get_datetime_col(self):
        return "datetime"

    def _transform(self, df):
        # Make a copy to be safe.
        df = df.copy()
        df = ftrs.reindex_to_integers(df)
        df_out, info = ftrs.compute_lagged_features(
            df, self.y_var, self.delay_lag, self.num_lags
        )
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df_out)
        return df_out, info


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

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = df_in.copy()
        datetime_idx = df.index
        #
        df = df.dropna()
        df = df.reset_index(inplace=True)
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        #
        x_fit = df[x_vars]
        y_fit = df[y_vars]
        self._model = self._model_func(**self._model_kwargs)
        self._model = self._model.fit(x_fit, y_fit)
        y_hat = self._model.predict(x_fit)
        #
        x_fit = pd.DataFrame(x_fit.values, index=datetime_idx, columns=x_vars)
        y_fit = pd.DataFrame(y_fit.values, index=datetime_idx, columns=y_vars)
        y_hat = pd.DataFrame(
            y_hat.values, index=datetime_idx, columns=[y + "_hat" for y in y_vars]
        )
        # TODO(Paul): Summarize model perf or make configurable.
        # TODO(Paul): Consider separating model eval from fit/predict.
        info = collections.OrderedDict()
        self._set_info("fit", info)
        return {"df_out": y_hat}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = df_in.copy()
        datetime_idx = df.index
        #
        df = df.dropna()
        df = df.reset_index(inplace=True)
        x_vars = self._to_list(self._x_vars)
        y_vars = self._to_list(self._y_vars)
        #
        x_predict = df[x_vars]
        y_predict = df[y_vars]
        y_hat = self._model.predict(x_predict)
        x_predict = pd.DataFrame(
            x_predict.values, index=datetime_idx, columns=x_vars
        )
        y_predict = pd.DataFrame(
            y_predict.values, index=datetime_idx, columns=y_vars
        )
        y_hat = pd.DataFrame(
            y_hat.values, index=datetime_idx, columns=[y + "_hat" for y in y_vars]
        )
        info = collections.OrderedDict()
        self._set_info("predict", info)
        return {"df_out": y_hat}

    def _model_perf(self, x, y, y_hat):
        info = collections.OrderedDict()
        pnl_rets = y.multiply(y_hat)
        info["pnl_rets"] = pnl_rets
        info["sr"] = fin.compute_sharpe_ratio(
            pnl_rets.resample("1B").sum(), time_scaling=252
        )
        return info

    def _to_list(
        self, to_list: Union[List[str], Callable[[], List[str]]]
    ) -> List[str]:
        if isinstance(to_list, list):
            return to_list
        return to_list()


class Model(SkLearnNode):
    # TODO(GP): y_var before x_vars? Probably should switch.
    def __init__(self, nid, datetime_col, y_var, x_vars):
        super().__init__(nid)
        self.y_var = y_var
        self.x_vars = x_vars
        self.datetime_col = datetime_col

    def fit(self, df_in):
        df_in = df_in.copy()
        reg = linear_model.LinearRegression()
        df_in = df_in.dropna()
        datetimes = df_in.index.values
        df_in = df_in.reset_index()
        if callable(self.x_vars):
            x_vars = self.x_vars()
        else:
            x_vars = self.x_vars
        x_train = df_in[x_vars]
        y_train = df_in[self.y_var]
        self.model = reg.fit(x_train, y_train)
        y_hat = self.model.predict(x_train)
        x_train = pd.DataFrame(x_train.values, index=datetimes, columns=x_vars)
        y_train = pd.Series(y_train.values, index=datetimes, name=self.y_var)
        y_hat = pd.Series(y_hat, index=datetimes, name=self.y_var + "_hat")
        #
        info = collections.OrderedDict()
        info["model_coeffs"] = [self.model.intercept_] + self.model.coef_.tolist()
        info["model_x_vars"] = ["intercept"] + x_vars
        info["stats"] = self._stats(df_in)
        info["model_perf"] = self._model_perf(x_train, y_train, y_hat)
        self._set_info("fit", info)
        return {"df_out": y_hat}

    def predict(self, df_in):
        df_in = df_in.dropna()
        datetimes = df_in.index.values
        df_in = df_in.reset_index()
        if callable(self.x_vars):
            x_vars = self.x_vars()
        else:
            x_vars = self.x_vars
        x_test = df_in[x_vars]
        y_test = df_in[self.y_var]
        y_hat = self.model.predict(x_test)
        x_test = pd.DataFrame(x_test.values, datetimes, columns=x_vars)
        y_test = pd.Series(y_test.values, datetimes, name=self.y_var)
        y_hat = pd.Series(y_hat, datetimes, name=self.y_var + "_hat")
        #
        info = collections.OrderedDict()
        info["stats"] = self._stats(df_in)
        info["model_perf"] = self._model_perf(x_test, y_test, y_hat)
        self._set_info("predict", info)
        return {"df_out": y_hat}

    # TODO: Use this to replace "_add_split_stats".
    def _stats(self, df):
        info = collections.OrderedDict()
        # info["min_datetime"] = min(df)
        # info["max_datetime"] = max(df)
        info["count"] = df.shape[0]
        return info

    def _model_perf(self, x, y, y_hat):
        info = collections.OrderedDict()
        info["hitrate"] = pip._compute_model_hitrate(self.model, x, y)
        pnl_rets = y * y_hat
        info["pnl_rets"] = pnl_rets
        info["sr"] = fin.compute_sharpe_ratio(
            pnl_rets.resample("1B").sum(), time_scaling=252
        )
        return info


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
    splits = pip.get_splits(config, df)
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
        model_coeffs.append(
            result_bundle[split]["stages"]["model"]["fit"]["model_coeffs"]
        )
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
