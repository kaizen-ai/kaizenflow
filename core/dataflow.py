import abc
import collections
import copy
import io
import logging

import networkx as nx
import pandas as pd
from sklearn import linear_model

import core.features as ftrs
import core.finance as fin
import core.signal_processing as sigp
import helpers.dbg as dbg
import rolling_model.pipeline as pip
import amp.vendors.kibot.utils as kut
from core.dataflow_core import DAG as DAG  # pylint: disable=unused-import
from core.dataflow_core import Node as Node

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
        else:
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
        raise NotImplementedError

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

# TODO(Paul): Write a Node builder to automatically generate these from
# functions.


# TODO(gp): Pass "ret_0" and "open" through constructor.
class PctReturns(Transformer):
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df = df.copy()
        # TODO(Paul): Factor out these info calls.
        df["ret_0"] = df["open"].pct_change()
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df)
        return df, info


class Zscore(Transformer):
    def __init__(self, nid, tau, demean, delay, cols=None):
        super().__init__(nid)
        self._tau = tau
        self._demean = demean
        self._delay = delay
        self._cols = cols

    def _transform(self, df):
        # Copy input to merge with output before returning.
        df_in = df.copy()
        # Restrict columns if requested.
        df = df.copy()
        if self._cols is not None:
            df = df[self._cols]
        # Z-score and name columns.
        df_out = sigp.rolling_zscore(df,
                                     tau=self._tau,
                                     demean=self._demean,
                                     delay=self._delay)
        df_out.rename(columns=lambda x: "z" + x, inplace=True)
        dbg.dassert(df_out.columns.intersection(df_in.columns).empty,
                    "Input dataframe has shared column names with zscored "
                    "dataframe.")
        # Merge input dataframe with z-scored columns.
        df_out = df_in.merge(df_out, left_index=True, right_index=True)
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df_out)
        return df_out, info


class FilterAth(Transformer):
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df_out = fin.filter_ath(df)
        #
        info = collections.OrderedDict()
        info["df_transformed_info"] = get_df_info_as_string(df_out)
        return df_out, info


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


class Model(SkLearnNode):
    # TODO(GP): y_var before x_vars? Probably should switch.
    def __init__(self, nid, datetime_col, y_var, x_vars):
        super().__init__(nid)
        self.y_var = y_var
        self.x_vars = x_vars
        self.datetime_col = datetime_col

    def fit(self, df_in):
        reg = linear_model.LinearRegression()
        x_train = df_in[self.x_vars]
        y_train = df_in[self.y_var]
        datetimes = df_in[self.datetime_col].values
        self.model = reg.fit(x_train, y_train)
        y_hat = self.model.predict(x_train)
        x_train = pd.DataFrame(
            x_train.values, index=datetimes, columns=self.x_vars
        )
        y_train = pd.Series(y_train.values, index=datetimes, name=self.y_var)
        y_hat = pd.Series(y_hat, index=datetimes, name=self.y_var + "_hat")
        #
        info = collections.OrderedDict()
        info["model_coeffs"] = [self.model.intercept_] + self.model.coef_.tolist()
        info["model_x_vars"] = ["intercept"] + self.x_vars
        info["stats"] = self._stats(df_in)
        info["model_perf"] = self._model_perf(x_train, y_train, y_hat)
        self._set_info("fit", info)
        return {"df_out": y_hat}

    def predict(self, df_in):
        x_test = df_in[self.x_vars]
        y_test = df_in[self.y_var]
        y_hat = self.model.predict(x_test)
        datetimes = df_in[self.datetime_col].values
        x_test = pd.DataFrame(x_test.values, datetimes, columns=self.x_vars)
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
    sr = fin.compute_sharpe_ratio(pnl_rets.resample("1B").sum(),
                                  time_scaling=252)
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
