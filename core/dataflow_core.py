import itertools
import logging

import networkx as nx

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Core node classes
# #############################################################################


# TODO(Paul): Make this abstract with ABC
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
        dbg.dassert_in(
            name,
            self.output_names,
            "%s is not an output of node %s!",
            name,
            self.nid,
        )
        if method not in self._output_vals:
            self._output_vals[method] = {}
        self._output_vals[method][name] = value

    def get_output(self, method, name):
        dbg.dassert_in(
            name,
            self.output_names,
            "%s is not an output of node %s!",
            name,
            self.nid,
        )
        dbg.dassert_in(
            method,
            self._output_vals.keys(),
            "%s of node %s has no output!",
            method,
            self.nid,
        )
        return self._output_vals[method][name]

    def get_outputs(self, method):
        dbg.dassert_in(method, self._output_vals.keys())
        return self._output_vals[method]


# #############################################################################
# Graph class for creating and executing a DAG of nodes.
# #############################################################################


class DAG:
    """
    Class for building pipeline graphs using Nodes.

    The DAG manages node execution and storage of outputs (within executed
    nodes).

    TODO(Paul): Think about how subgraphs should fit into this framework.
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
        dbg.dassert_isinstance(
            node, Node, "Only graphs of class `Node` are supported!"
        )
        self._dag.add_node(node.nid, stage=node)

    def get_node(self, nid):
        """
        Convenience node accessor.

        :param nid: unique string node id
        :return: Node object
        """
        return self._dag.nodes[nid]["stage"]

    # TODO(Paul): Automatically infer edge labels when possible (e.g., SISO).
    def connect(self, parent, child):
        """
        Adds a directed edge from parent node output to child node input.

        Raises if the requested edge is invalid or forms a cycle.

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
            _LOG.warning(
                "Creating edge %s -> %s failed because it creates a cycle!",
                parent[0],
                child[0],
            )
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
        # Convenient for experiments/debugging, but not needed for internal use.
        # Perhaps we should expose a public `run_node` that just invokes
        # `_run_node` and then returns the output as below.
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
            # Collect all sinks so that we can easily output their data after
            # all nodes have been run.
            if any(True for _ in self._dag.predecessors(nid)):
                sinks.append(nid)
            self._run_node(method=method, nid=nid)
        return [self.get_node(sink).get_outputs(method) for sink in sinks]

    def run_leq_node(self, nid, method):
        """
        Executes pipeline up to (and including) `node` and returns output.

        "leq" refers to the partial ordering on the vertices. This method
        runs a node if and only if there is a directed path from the node to
        `nid`. Nodes are run according to a topological sort.
        """
        ancestors = filter(
            lambda x: x in nx.ancestors(self._dag, nid),
            nx.topological_sort(self._dag),
        )
        nids = itertools.chain(ancestors, [nid])
        for n in nids:
            self._run_node(n, method)
        return self.get_node(nid).get_outputs(method)
