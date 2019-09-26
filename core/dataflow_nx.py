import itertools
import logging

import networkx as nx

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class Node:
    """
    Node class for creating DAG pipelines of functions.

    Common use case: Nodes wrap functions with a common method (e.g., `fit`).

    The Node class provides some convenient introspection (input/output names)
    accessors and, importantly, a unique identifier (`nid`) for building
    graphs of nodes. The `nid` is also useful for config purposes.

    For nodes requiring fit/transform, we can subclass / provide a mixin with
    the desired methods.

    - As currently written, nodes are assumed to manage output state, e.g., if
      a node wraps a function, then when the node (and hence function) is
      executed, the output of that function is stored in the node.
    - This makes it easy to manage node execution in a graph, especially when
      - We execute nodes sequentially in a topological sorted DAG, but outputs
        from multiple previous nodes are needed downstream (downstream node has
        multiple parents).
      - The output of a node is needed for more multiple downstream nodes
        (node has multiple child nodes).
      - We want to work interactively in a notebook
      - We want to debug node state
    - An alternative would be to not explicitly store such state, but use
      caching.
       - This could work well for interactive and debug use.
       - This is less straightforward for the multiple parent / children case,
         e.g., we can execute the nodes in a topological sequentially, but each
         node execution would require a parent node "re-run" using the cached
         value.
       - Caching may make it more difficult to set different state policies
         (e.g., suppose we want to retain the X most recent values).
       - What if we use stateful nodes?
    """
    def __init__(self, nid, inputs=None, outputs=None):
        """
        :param nid: node identifier. Should be unique in a graph.
        :param inputs: list-like string names of input_names.
            # TODO(Paul): Consider splitting into required/optional.
        :param outputs: list-like string names of output_names. The node is assumed
            to store the last output.
            # TODO(Paul): Consider other policies.
        """
        dbg.dassert_isinstance(nid, str)
        if not nid:
            _LOG.warning("Empty string chosen for unique nid!")
        self._nid = nid
        self._inputs = []
        if inputs is not None:
            for input in inputs:
                dbg.dassert_isinstance(input, str)
                self._inputs.append(input)
        self._outputs = {}
        if outputs is not None:
            for output in outputs:
                dbg.dassert_isinstance(output, str)
                self._outputs[output] = None

    @property
    def nid(self):
        return self._nid

    @property
    def input_names(self):
        return self._inputs

    @property
    def output_names(self):
        return list(self._outputs.keys())

    def output(self, name):
        dbg.dassert_in(name, self._outputs.keys(),
                       "%s is not an output of node %s!", name, self.nid)
        return self._outputs[name]

    def _info(self, **kwargs):
        _LOG.info("input_names: %s", self.input_names)
        _LOG.info("output_names: %s", self.output_names)
        _LOG.info("nid: %s", self._nid)


class Graph:
    """
    Class for building pipeline graphs using Nodes.

    The Graph is directed and should be a DAG (TODO(Paul): enforce this when
    trying to added edges).

    The Graph manages node execution. As currently written, it does not manage
    Node output state.
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
        Helper method for running nodes
        """
        _LOG.info("Node nid=`%s` executing method `%s`...", nid, method)
        kwargs = {}
        for pre in self._graph.predecessors(nid):
            kvs = self._graph.edges[[pre, nid]]
            for k, v in kvs.items():
                kwargs[k] = self.get_node(pre).output(v)
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