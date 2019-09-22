import copy
import logging

import pandas as pd

import core.features as ftrs
import core.signal_processing as sigp
import helpers.dbg as dbg
import helpers.printing as prnt
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


# TODO(GP, Paul): Separate higher-level dataflow classes from source-specific
# inherited extensions.
class Node:
    """
    Represent a computation that depends on other nodes.
    - the constructor takes all the parameters and the metadata that are
      needed to build the node
    - `connect()` is used to connect nodes into a graph that can be executed
       at run-time
    - `fit()` is used to fit the node using the output of the parent nodes
       after they are fit
    - `predict()` is similar to `fit()` phase

    A node can be fit only once. In order to be fit again (e.g., during a
    forward walk backtest) its state needs to be reset with calling `reset()`.
    A node can be used to predict multiple times using the result of the
    previous fit.
    """

    def __init__(self, name, num_inputs=1):
        _LOG.debug("name=%s num_inputs=%s", name, num_inputs)
        dbg.dassert_isinstance(name, str)
        self._name = name
        self._is_connected = False
        #
        dbg.dassert_lte(0, num_inputs)
        self._num_inputs = num_inputs
        # List of parent nodes.
        self._input_nodes = []
        # Initialize the resettable state.
        self._reset()

    @property
    def name(self):
        return self._name

    def connect(self, *nodes):
        """
        Connect this node to a list of Nodes.
        """
        _LOG.debug(
            "name=%s nodes=%d (%s)",
            self._name,
            len(nodes),
            self._node_names(nodes),
        )
        if self._is_connected:
            msg = "Node '%s': already connected to %s" % (
                self._name,
                ", ".join(map(str, self._input_nodes)),
            )
            dbg.dassert(msg)
        dbg.dassert_eq(
            len(nodes),
            self._num_inputs,
            "Node '%s': invalid number of " "connections",
            self._name,
        )
        for node in nodes:
            dbg.dassert_isinstance(node, Node)
            self._input_nodes.append(node)
        self._is_connected = True

    def fit(self):
        """
        Accumulate the outputs of each parent nodes after `fit` is called on
        them.
        """
        _LOG.debug("name=%s", self._name)
        dbg.dassert(not self._is_fit, "Node '%s': already fit", self._name)
        for node in self._input_nodes:
            self._fit_input_values.append(node.fit())
        self._is_fit = True

    def predict(self):
        """
        Accumulate the outputs of each parent nodes after `predict` is called on
        them.
        """
        # We can predict multiple times, so every time we need to re-evaluate
        # the parents from scratch.
        self._predict_inputs_values = []
        for node in self._input_nodes:
            self._predict_inputs_values.append(node.predict())

    def __str__(self):
        """
        Return a string representing the node, e.g.,:
            name=n1, type=core.dataflow.Node, num_inputs=0, is_connected=True, ...
        """

        # TODO(gp): Specify also the format like %s.
        info = [
            ("name", self._name),
            ("type", prnt.type_to_string(type(self))),
            ("num_inputs", self._num_inputs),
            ("is_connected", self._is_connected),
            ("is_fit", self._is_fit),
        ]
        ret = self._to_string(info)
        return ret

    def dag_to_string(self):
        """
        Return a string representing all the dag that has this node as output, e.g.,:
          name=n3, type=core.dataflow.Node, num_inputs=2, ...
            name=n2, type=core.dataflow.Node, num_inputs=0, ...
            name=n1, type=core.dataflow.Node, num_inputs=0, ...
        """
        ret = []
        ret.append(str(self))
        for n in self._input_nodes:
            ret.append(prnt.space(n.dag_to_string()))
        ret = "\n".join(ret)
        return ret

    # //////////////////////////////////////////////////////////////////////////

    def _reset(self):
        """
        Reset part of the state between two successive fit operations.
        """
        _LOG.debug("name=%s", self._name)
        #
        self._fit_input_values = []
        self._is_fit = False
        #
        self._predict_input_values = []
        self._output_values = None

    @staticmethod
    def _to_string(info):
        """
        Use info like [("name", ...), ("type", ...), ...] to build a string
        representation, e.g.,:
            name=n2, type=core.dataflow.Node, ...
        """
        ret = ", ".join(["%s=%s" % (i[0], i[1]) for i in info])
        return ret

    @staticmethod
    def _node_names(nodes):
        """
        Return a comma separated string of names for a list of names, e.g.,
            "n1,n3,n2"
        """
        ret = ",".join([n.name for n in nodes])
        return ret


# TODO(gp): Extend this to nodes with more than one input.
class StatelessNodeWithOneInput(Node):
    def __init__(self, name):
        super().__init__(name, num_inputs=1)

    def _transform(self, df):
        """
        :return: df, info
        """
        raise NotImplementedError

    def fit(self):
        super().fit()
        # Transform the input df.
        df_in = self._fit_input_values[0]
        df_out, info = self._transform(df_in)
        # Save the info in the node: we make a copy just to be safe.
        self.fit_info = copy.copy(info)
        return df_out

    def predict(self):
        super().predict()
        # Transform the input df.
        df_in = self._predict_inputs_values[0]
        df_out, info = self._transform(df_in)
        # Save the info in the node: we make a copy just to be safe.
        self.predict_info = copy.copy(info)
        return df_out


# ##############################################################################


class ReadData(Node):
    def __init__(self, name):
        super().__init__(name, num_inputs=0)
        #
        self.df = None
        self._reset()

    # TODO(gp): Not sure about this approach. We want to reuse the node during
    #  multiple experiments (e.g., cross-validation) so we can't use the ctor
    #  for this.
    def set_train_idxs(self, train_idxs):
        """
        :param train_idxs: indices of the df to use for fitting
        """
        self._train_idxs = train_idxs

    def fit(self):
        """
        :return: training set as df
        """
        super().fit()
        dbg.dassert_is_not(self._train_idxs, None)
        train_df = self.df.iloc[self._train_idxs]
        return train_df

    def set_test_idxs(self, test_idxs):
        """
        :param test_idxs: indices of the df to use for predicting
        """
        self._test_idxs = test_idxs

    def predict(self):
        """
        :return: test set as df
        """
        super().predict()
        dbg.dassert_is_not(self._test_idxs, None)
        test_df = self.df.iloc[self._test_idxs]
        return test_df

    def _reset(self):
        super()._reset()
        _LOG.debug("name=%s", self._name)
        self._train_idxs = None
        self._test_idxs = None


class ReadDataFromDf(ReadData):
    def __init__(self, name, df):
        super().__init__(name)
        dbg.dassert_isinstance(df, pd.DataFrame)
        self.df = df


class KibotReadData(ReadData):
    def __init__(self, name, file_name, nrows):
        super().__init__(name)
        dbg.dassert_exists(file_name)
        self._file_name = file_name
        self._nrows = nrows
        #
        self.df = None

    def _lazy_load(self):
        if not self.df:
            self.df = kut.read_data_memcached(self._file_name, self._nrows)

    # TODO(gp): Make it streamable so that it reads only the needed data, if
    # possible.
    def fit(self):
        """
        :return: training set as df
        """
        self._lazy_load()
        super().fit()

    def __str__(self):
        ret = []
        info = [("file_name", self._file_name), ("nrows", self._nrows)]
        ret.append(self._to_string(info))
        # Get the subclass representation.
        ret.append(super().__str__())
        ret = "| ".join(ret)
        return ret


# ##############################################################################


class Zscore(StatelessNodeWithOneInput):
    def __init__(self, name, tau):
        super().__init__(name)
        self.tau = tau

    def _transform(self, df):
        df_out = sigp.rolling_zscore(df, self.tau)
        info = None
        return df_out, info


class ComputeLaggedFeatures(StatelessNodeWithOneInput):
    def __init__(self, name, y_var, delay_lag, num_lags):
        super().__init__(name, num_inputs=1)
        self.y_var = y_var
        self.delay_lag = delay_lag
        self.num_lags = num_lags

    def _transform(self, df):
        # Make a copy to be safe.
        df = df.copy()
        df = ftrs.reindex_to_integers(df)
        df_out, info = ftrs.compute_lagged_features(
            df, self.y_var, self.delay_lag, self.num_lags
        )
        return df_out, info


# class Model(Node):
#
#     def __init__(self, name, y_var, x_vars):
#         self._params = None
#
#     def connect(self, input1):
#         super().connect(input1)
#
#     def fit(self, df):
#         """
#         A model doesn't return anything since it's a sink.
#         """
#         return None
#
#     def predict(self, df):
#         return df + self._params
