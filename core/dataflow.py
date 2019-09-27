import collections
import copy
import itertools
import logging

import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from sklearn import linear_model

# Import style usually to be avoided, but in this case it may make sense.
from core.dataflow_core import *
import core.features as ftrs
import core.finance as fin
import helpers.dbg as dbg
import rolling_model.pipeline as pip
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


# #############################################################################
# Visualization
# #############################################################################


def draw(graph):
    nx.draw_networkx(graph,
                     pos=nx.spectral_layout(graph),
                     node_size=3000,
                     arrowsize=30,
                     width=1.5)


# #############################################################################
# DataFrame manipulation nodes
# #############################################################################


# TODO(Paul): Make the train/test idx behavior a mixin
class ReadData(Node):
    def __init__(self, nid):
        super().__init__(nid, outputs=["output"])
        #
        self.df = None
        self._train_idxs = None
        self._test_idxs = None

    def set_train_idxs(self, train_idxs):
        """
        :param train_idxs: indices of the df to use for fitting
        """
        self._train_idxs = train_idxs

    def fit(self):
        """
        :return: training set as df
        """
        if self._train_idxs:
            train_df = self.df.iloc[self._train_idxs]
        else:
            train_df = self.df
        return {self.output_names[0]: train_df}

    def set_test_idxs(self, test_idxs):
        """
        :param test_idxs: indices of the df to use for predicting
        """
        self._test_idxs = test_idxs

    def predict(self):
        """
        :return: test set as df
        """
        if self._test_idxs:
            test_df = self.df.iloc[self._test_idxs]
        else:
            test_df = self.df
        return {self.output_names[0]: test_df}

    def get_df(self):
        dbg.dassert_is_not(self.df, None)
        return self.df


class ReadDataFromDf(ReadData):
    def __init__(self, nid, df):
        super().__init__(nid)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


class ReadDataFromKibot(ReadData):
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


# TODO(Paul): Abstract? Should we?
class StatelessSisoNode(Node):
    """
    Stateless Single-Input Single-Output node.
    """
    def __init__(self, nid):
        super().__init__(nid, inputs=["input"], outputs=["output"])

    def _transform(self, df):
        """
        :return: df, info
        """
        raise NotImplementedError

    def fit(self, input):
        # Transform the input df.
        df_out, info = self._transform(input)
        # Save the info in the node: we make a copy just to be safe.
        self.fit_info = copy.copy(info)
        return {"output": df_out}

    def predict(self, input):
        # Transform the input df.
        df_out, info = self._transform(input)
        # Save the info in the node: we make a copy just to be safe.
        self.predict_info = copy.copy(info)
        return {"output": df_out}


# TODO(Paul): Write a Node builder to automatically generate these from
# functions.
# TODO(gp): Pass "ret_0" and "open" through constructor.
class PctReturns(StatelessSisoNode):
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df = df.copy()
        df["ret_0"] = df["open"].pct_change()
        info = None
        return df, info


class Zscore(StatelessSisoNode):
    def __init__(self, nid, style, com):
        super().__init__(nid)
        self.style = style
        self.com = com

    def _transform(self, df):
        # df_out = sigp.rolling_zscore(df, self.tau)
        df_out = pip.zscore(df, self.style, self.com)
        info = None
        return df_out, info


class FilterAth(StatelessSisoNode):
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df_out = fin.filter_ath(df)
        info = None
        return df_out, info


# TODO(Paul, GP): Confusing interface. If we only sent out the transformed
# variables, then we wouldn't need `get_x_vars`.
class ComputeLaggedFeatures(StatelessSisoNode):
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

    def _transform(self, df):
        # Make a copy to be safe.
        df = df.copy()
        df = ftrs.reindex_to_integers(df)
        df_out, info = ftrs.compute_lagged_features(
            df, self.y_var, self.delay_lag, self.num_lags
        )
        return df_out, info


class Model(Node):
    # TODO(GP): y_var before x_vars? Probably should switch.
    def __init__(self, nid, y_var, x_vars):
        super().__init__(nid, inputs=["input"], outputs=["output"])
        self.y_var = y_var
        self.x_vars = x_vars

    def fit(self, input):
        """
        A model fit doesn't return anything since it's a sink.

        # TODO: Consider making `fit` pass-through in terms of dataflow.
        """
        df = input
        reg = linear_model.LinearRegression()
        x_train = df[self.x_vars]
        y_train = df[self.y_var]
        self.model = reg.fit(x_train, y_train)
        return {"output": None}

    def predict(self, input):
        df = input
        x_test = df[self.x_vars]
        y_test = df[self.y_var]
        #
        info = collections.OrderedDict()
        info["hitrate"] = pip._compute_model_hitrate(self.model, x_test, y_test)
        hat_y = self.model.predict(x_test)
        pnl_rets = y_test * hat_y
        info["pnl_rets"] = pnl_rets
        self.predict_info = copy.copy(info)
        return {"output": hat_y}


def cross_validate(config, source_nid, sink_nid, dag):
    source_node = dag.get_node(source_nid)
    df = source_node.get_df()
    splits = pip.get_splits(config, df)
    #
    for i, (train_idxs, test_idxs) in enumerate(splits):
        source_node.set_train_idxs(train_idxs)
        dag.run_leq_node(sink_nid, "fit")
        #
        source_node.set_test_idxs(test_idxs)
        dag.run_leq_node(sink_nid, "predict")
    return dag.get_node(sink_nid)
