import collections
import copy
import itertools
import logging

import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from sklearn import linear_model

import core.features as ftrs
import core.finance as fin
import helpers.dbg as dbg
import rolling_model.pipeline as pip
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


# #############################################################################
# Core node classes
# #############################################################################


class AbstractNode:
    """
    Abstract node class for creating DAG pipelines of functions.

    Common use case: Nodes wrap functions with a common method (e.g., `fit`).

    This class provides some convenient introspection (input/output names)
    accessors and, importantly, a unique identifier (`nid`) for building
    graphs of nodes. The `nid` is also useful for config purposes.

    For nodes requiring fit/transform, we can subclass / provide a mixin with
    the desired methods.
    """
    def __init__(self, nid, inputs=None, outputs=None):
        """
        :param nid: node identifier. Should be unique in a graph.
        :param inputs: list-like string names of input_names.
        """
        dbg.dassert_isinstance(nid, str)
        if not nid:
            _LOG.warning("Empty string chosen for unique nid!")
        self._nid = nid
        self._inputs = self._init_validation_helper(inputs)
        self._outputs = self._init_validation_helper(outputs)

    def _init_validation_helper(self, l):
        if l is None:
            return []
        for item in l:
            dbg.dassert_isinstance(item, str)
        return l

    @property
    def nid(self):
        return self._nid

    @property
    def input_names(self):
        return self._inputs

    @property
    def output_names(self):
        return self._outputs

    def _info(self, **kwargs):
        _LOG.info("input_names: %s", self.input_names)
        _LOG.info("output_names: %s", self.output_names)
        _LOG.info("nid: %s", self._nid)
        dummy_output = {}
        for output in self.output_names:
            dummy_output[output] = None
        return dummy_output


class Node(AbstractNode):
    """
    Concrete node that also stores its output when run.
    """
    def __init__(self, nid, inputs=None, outputs=None):
        """
        :param nid: node identifier. Should be unique in a graph.
        :param inputs: list-like string names of input_names.
        :param outputs: list-like string names of output_names. The node is assumed
            to store the last output.
        """
        super().__init__(nid=nid, inputs=inputs, outputs=outputs)
        self._output_vals = {}

    def store_output(self, method, name, value):
        dbg.dassert_in(name, self.output_names,
                       "%s is not an output of node %s!", name, self.nid)
        if method not in self._output_vals:
            self._output_vals[method] = {}
        self._output_vals[method][name] = value

    def get_output(self, method, name):
        dbg.dassert_in(name, self.output_names,
                       "%s is not an output of node %s!", name, self.nid)
        dbg.dassert_in(method, self._output_vals.keys(),
                       "%s of node %s has no output!", method, self.nid)
        return self._output_vals[method][name]

    def get_outputs(self, method):
        dbg.dassert_in(method, self._output_vals.keys())
        return self._output_vals[method]


# #############################################################################
# Graph class for creating and executing a DAG of nodes.
# #############################################################################


# TODO(Paul): Consider renaming `DAG` once we enforce the invariant.
class DAG:
    """
    Class for building pipeline graphs using Nodes.

    The Graph is directed and should be a DAG.
    TODO(Paul): enforce this when trying to added edges.

    The Graph manages node execution.
    """
    def __init__(self):
        self._dag = nx.DiGraph()

    @property
    def dag(self):
        return self._dag

    def add_node(self, node):
        """
        Adds `node` to the graph.

        Relies upon the unique nid for identifying the node.

        :param node: Node object
        """
        dbg.dassert_isinstance(node, Node,
                               "Only graphs of class `Node` are supported!")
        self._dag.add_node(node.nid, stage=node)

    def get_node(self, nid):
        """
        Convenience node accessor.

        :param nid: unique string node id
        :return: Node object
        """
        return self._dag.nodes[nid]['stage']

    # TODO(Paul): Automatically infer edge labels when possible (e.g., SISO).
    def connect(self, parent, child):
        """
        Adds a directed edge from parent node output to child node input.

        Raises if the requested edge is invalid.

        If this is called multiple times on the same nid's but with different
        output/input pairs, the additional input/output pairs are simply added
        to the existing edge (the previous ones are not overwritten).

        :param parent: tuple of the form (nid, output)
        :param child: tuple of the form (nid, input)
        """
        dbg.dassert_in(parent[1], self.get_node(parent[0]).output_names)
        dbg.dassert_in(child[1], self.get_node(child[0]).input_names)
        kwargs = {child[1]: parent[1]}
        self._dag.add_edge(parent[0], child[0], **kwargs)
        if not nx.is_directed_acyclic_graph(self.dag):
            _LOG.warning("Creating edge %s -> %s failed because it creates a cycle!",
                         parent[0], child[0])
            self._dag.remove_edge(parent[0], child[0])

    def _run_node(self, nid, method):
        """
        Runs a single node.

        This method DOES NOT run (or re-run) ancestors of `nid`.
        """
        _LOG.debug("Node nid=`%s` executing method `%s`...", nid, method)
        kwargs = {}
        for pre in self._dag.predecessors(nid):
            kvs = self._dag.edges[[pre, nid]]
            pre_node = self.get_node(pre)
            for k, v in kvs.items():
                # Retrieve output from store.
                kwargs[k] = pre_node.get_output(method, v)
        _LOG.debug("kwargs are %s", kwargs)
        node = self.get_node(nid)
        output = getattr(node, method)(**kwargs)
        for out in node.output_names:
            node.store_output(method, out, output[out])
        return self.get_node(nid).get_outputs(method)

    def run_dag(self, method):
        """
        Executes entire pipeline.

        Nodes are run according to a topological sort.

        :param method: Method of class `Node` (or subclass) to be executed for
            the entire DAG.
        """
        sinks = []
        for nid in nx.topological_sort(self._dag):
            if any(True for _ in self._dag.predecessors(nid)):
                sinks.append(nid)
            self._run_node(method=method, nid=nid)
        return [self.get_node(sink).get_outputs(method) for sink in sinks]

    def run_leq_node(self, nid, method):
        """
        Executes pipeline only up to (and including) `node` and returns output.

        "leq" refers to the partial ordering on the vertices. This method
        runs a node if and only if there is a directed path from the node to
        `nid`. Nodes are run according to a topological sort.
        """
        ancestors = filter(lambda x: x in nx.ancestors(self._dag, nid),
                           nx.topological_sort(self._dag))
        nids = itertools.chain(ancestors, [nid])
        for n in nids:
            self._run_node(n, method)
        return self.get_node(nid).get_outputs(method)


# TODO(Paul): Move (most of) these to a separate library
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
        self._output_vals
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


# TODO(Paul): Rename SISO to Siso
class StatelessSISONode(Node):
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
class PctReturns(StatelessSISONode):
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df = df.copy()
        df["ret_0"] = df["open"].pct_change()
        info = None
        return df, info


class Zscore(StatelessSISONode):
    def __init__(self, nid, style, com):
        super().__init__(nid)
        self.style = style
        self.com = com

    def _transform(self, df):
        # df_out = sigp.rolling_zscore(df, self.tau)
        df_out = pip.zscore(df, self.style, self.com)
        info = None
        return df_out, info


class FilterAth(StatelessSISONode):
    def __init__(self, nid):
        super().__init__(nid)

    def _transform(self, df):
        df_out = fin.filter_ath(df)
        info = None
        return df_out, info


# TODO(Paul, GP): Confusing interface. If we only sent out the transformed
# variables, then we wouldn't need `get_x_vars`.
class ComputeLaggedFeatures(StatelessSISONode):
    def __init__(self, nid, y_var, delay_lag, num_lags):
        super().__init__(nid)
        self.y_var = y_var
        self.delay_lag = delay_lag
        self.num_lags = num_lags

    def get_x_vars(self):
        x_vars = ftrs.get_lagged_feature_names(self.y_var, self.delay_lag,
                                               self.num_lags)
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
    # NOTE: y_var before x_vars?
    def __init__(self, nid, y_var, x_vars):
        super().__init__(nid, inputs=['input'], outputs=['output'])
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
        return {'output': None}

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
        return {'output': hat_y}


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