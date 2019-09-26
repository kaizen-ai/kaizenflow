import itertools
import logging

import networkx as nx
import pandas as pd

import helpers.dbg as dbg
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


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
        dbg.dassert_in(method, self._output_vals.keys())
        return self._output_vals[method][name]

    def get_outputs(self, method):
        dbg.dassert_in(method, self._output_vals.keys())
        return self._output_vals[method]


class Graph:
    """
    Class for building pipeline graphs using Nodes.

    The Graph is directed and should be a DAG.
    TODO(Paul): enforce this when trying to added edges.

    The Graph manages node execution.
    """
    def __init__(self):
        self._graph = nx.DiGraph()

    @property
    def graph(self):
        return self._graph

    @property
    def is_dag(self):
        """
        Convenience method for checking that the graph is a DAG.

        return: Bool
        """
        return nx.is_directed_acyclic_graph(self._graph)

    def add_node(self, node):
        """
        Adds `node` to the graph.

        Relies upon the unique nid for identifying the node.

        :param node: Node object
        """
        dbg.dassert_isinstance(node, Node,
                               "Only graphs of class `Node` are supported!")
        self._graph.add_node(node.nid, stage=node)

    def get_node(self, nid):
        """
        Convenience node accessor.

        :param nid: unique string node id
        :return: Node object
        """
        return self._graph.nodes[nid]['stage']

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
        self._graph.add_edge(parent[0], child[0], **kwargs)

    def _run_node(self, method, nid):
        """
        Helper method for running individual nodes.
        """
        _LOG.debug("Node nid=`%s` executing method `%s`...", nid, method)
        kwargs = {}
        for pre in self._graph.predecessors(nid):
            kvs = self._graph.edges[[pre, nid]]
            pre_node = self.get_node(pre)
            for k, v in kvs.items():
                # Retrieve output from store.
                kwargs[k] = pre_node.get_output(method, v)
        _LOG.debug("kwargs are %s", kwargs)
        node = self.get_node(nid)
        output = getattr(node, method)(**kwargs)
        for out in node.output_names:
            node.store_output(method, out, output[out])

    def run(self, method):
        """
        Executes entire pipeline.

        :param method: Method of class `Node` (or subclass) to be executed for
            the entire DAG.
        """
        dbg.dassert(self.is_dag, "Graph execution requires a DAG!")
        for nid in nx.topological_sort(self._graph):
            self._run_node(method=method, nid=nid)

    def run_node(self, method, nid, eval_mode="default"):
        """
        Executes pipeline only up to (and including) `node` and returns output.

        :param method: Same as in `run`.
        :param node: terminal evaluation node
        """
        if eval_mode == "default":
            dbg.dassert(self.is_dag, "Graph execution requires a DAG!")
            ancestors = filter(lambda x: x in nx.ancestors(self._graph, nid),
                               nx.topological_sort(self._graph))
            nids = itertools.chain(ancestors, [nid])
        elif eval_mode == "cache":
            nids = [nid]
        else:
            raise ValueError("Supported eval_modes are `default` and `cache`.")
        for nid in nids:
            self._run_node(method=method, nid=nid)
        return self.get_node(nid).get_outputs(method)


class ReadData(Node):
    def __init__(self, nid):
        super().__init__(nid, outputs=["out"])
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
        return {"out": train_df}

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
        return {"out": test_df}

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
