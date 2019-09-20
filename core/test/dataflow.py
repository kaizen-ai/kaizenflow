import logging

import core.signal_processing as sigp
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class Node:

    def __init__(self, name: str, num_inputs : int =1):
        dbg.dassert_isinstance(name, str)
        self._name = name
        #
        dbg.dassert_lte(0, num_inputs)
        self._num_inputs = num_inputs
        # List of parent nodes.
        self._input_nodes = []
        self._reset()

    def connect(self, *nodes):
        if self._is_connected:
            msg = "%s: already connected to %s" %(self._name, ", " \
                                                                      "".join(
                self._input_nodes))
            _LOG.error(msg)
            raise ValueError(msg)
        dbg.dassert_eq(len(nodes), self._num_inputs, "%s: invalid number of "
                                                     "connections", self._name)
        for node in nodes:
            dbg.dassert_isinstance(node, Node)
            self._input_nodes.append(node)
        self._is_connected = True

    def fit(self):
        if self._is_fit:
            msg = "%s: already fit" % self._name
            _LOG.error(msg)
        for node in self._input_nodes:
            self._fit_inputs_values.append(node.fit())
        self._is_fit = True

    def predict(self):
        # We can predict multiple times so everytime we need to re-evaluate.
        self._predict_inputs_values = []
        for node in self._input_nodes:
            self._predict_inputs_values.append(node.predict())

    def _reset(self):
        self._is_connected = False
        #
        self._fit_input_values = []
        self._is_fit = False
        #
        self._predict_input_values = []
        self._output_values = None


# ##############################################################################

import vendors.kibot.utils as kut


class ReadData(Node):

    def __init__(self, name, file_name, nrows):
        super(self).__init__(name, num_inputs=0)
        dbg.dassert_exists(file_name)
        self.file_name = file_name
        self.nrows = nrows
        #
        self.df = None

    def _lazy_load(self):
        if not self.df:
            self.df = kut.read_data_memcached(self.file_name, self.nrows)

    # TODO(gp): Make it streamable so that it reads only the needed data if
    # possible.
    def fit(self, train_idxs):
        """
        :param train_idxs: indices of the df to use for fitting
        :return: training set as df
        """
        super(self).fit()
        self._lazy_load()
        train_df = self.df.iloc[train_idxs]
        return train_df

    def predict(self, test_idxs):
        """
        :param test_idxs: indices of the df to use for predicting
        :return: test set as df
        """
        super(self).predict()
        test_df = self.df.iloc[test_idxs]
        return test_df


class Zscore(Node):

    def __init__(self, name, tau):
        super(self).__init__(name, num_inputs=1)
        self.tau = tau

    def _transform(self, df):
        df_out = sigp.rolling_zscore(
                df,
                self.tau,
        )
        return df_out

    def fit(self):
        super(self).fit()
        df_in = self._fit_inputs_values[0]
        df_out = self._transform(df_in)
        return df_out

    def predict(self):
        super(self).predict()
        df_in = self._predict_inputs_values[0]
        df_out = self._transform(df_in)
        return df_out


# class ComputeFeatures(Node):
#
#     def __init__(self, name, target_y, num_lags):
#         pass
#
#     def connect(self, input1):
#         super(self).connect(input1)
#
#     def get_x_vars(self):
#         x_vars = ["x0", "x1"]
#         return x_var
#
#     def fit(self, df):
#         df_out = df
#         x_vars = ["x0", "x1"]
#         return df_out
#
#
# class Model(Node):
#
#     def __init__(self, name, y_var, x_vars):
#         self._params = None
#
#     def connect(self, input1):
#         super(self).connect(input1)
#
#     def fit(self, df):
#         """
#         A model doesn't return anything since it's a sink.
#         """
#         return None
#
#     def predict(self, df):
#         return df + self._params
