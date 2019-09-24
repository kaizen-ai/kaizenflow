import itertools
import logging

import networkx as nx

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class Node:
    """
    Node class for creating DAG pipelines.
    """
    def __init__(self, nid, inputs=None, outputs=None):
        dbg.dassert_isinstance(nid, str)
        self._nid = nid
        self._inputs = {}
        if inputs is not None:
            for input in inputs:
                dbg.dassert_isinstance(input, str)
                self._inputs[input] = None
        self._outputs = {}
        if outputs is not None:
            for output in outputs:
                dbg.dassert_isinstance(output, str)
                self._outputs[output] = None

    @property
    def nid(self):
        return self._nid

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def _info(self, **kwargs):
        _LOG.info("inputs: %s", self._inputs.keys())
        _LOG.info("outputs: %s", self._outputs.keys())
        _LOG.info("nid: %s", self._nid)


class Pipeline:
    """
    Class for building pipeline graphs using Nodes.
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

        TODO(Paul): Support connecting multiple inputs/outputs.

        :param parent: tuple of the form (nid, output)
        :param child: tuple of the form (nid, input)
        """
        dbg.dassert_in(parent[1], self.get_node(parent[0]).outputs.keys())
        dbg.dassert_in(child[1], self.get_node(child[0]).inputs.keys())
        kwargs = {child[1]: parent[1]}
        self._graph.add_edge(parent[0], child[0], **kwargs)

    def _run_node(self, method, nid):
        """
        Helper method for running nodes
        """
        _LOG.info("Node nid=`%s` executing method `%s`...", nid, method)
        kwargs = {}
        for pre in self._graph.predecessors(nid):
            kvs = self._graph.edges[[pre, nid]]
            for k, v in kvs.items():
                kwargs[k] = self.get_node(pre).outputs[v]
        _LOG.info("kwargs are %s", kwargs)
        getattr(self.get_node(nid), method)(**kwargs)

    def run(self, method):
        """
        Executes pipeline.

        :param method: Method of class `Node` (or subclass) to be executed for
            the entire DAG.
        """
        dbg.dassert(self.is_dag, "Graph execution requires a DAG!")
        for nid in nx.topological_sort(self._graph):
            self._run_node(method, nid)

    def run_node(self, method, nid, eval_mode='full'):
        """
        Executes pipeline only up to (and including) `node`.

        :param method: Same as in `run`.
        :param node: terminal evaluation node
        :param eval_mode: options for rerunning ancestors / caching, etc.
        """
        if eval_mode == 'full':
            dbg.dassert(self.is_dag, "Graph execution requires a DAG!")
            ancestors = filter(lambda x: x in nx.ancestors(self._graph, nid),
                               nx.topological_sort(self._graph))
            nids = itertools.chain(ancestors, [nid])
        elif eval_mode == 'cached':
            nids = [nid]
        else:
            raise ValueError("Supported eval_modes are `full` and `cached`.")
        for nid in nids:
            self._run_node(method, nid)