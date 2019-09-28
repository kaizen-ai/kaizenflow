import abc
import itertools
import logging

import networkx as nx

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Core node classes
# #############################################################################


class AbstractNode(abc.ABC):
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
        :param outputs: list-like string names out output_names.
        """
        dbg.dassert_isinstance(nid, str)
        dbg.dassert(nid, "Empty string chosen for unique nid!")
        self._nid = nid
        self._inputs = self._init_validation_helper(inputs)
        self._outputs = self._init_validation_helper(outputs)

    @property
    def nid(self):
        return self._nid

    @property
    def input_names(self):
        return self._inputs

    @property
    def output_names(self):
        return self._outputs

    def _init_validation_helper(self, l):
        if l is None:
            return []
        for item in l:
            dbg.dassert_isinstance(item, str)
        return l

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and isinstance(self, other.__class__)
            and self.nid == other.nid
            and self.input_names == other.input_names
            and self.output_names == other.output_names
        )


class Node(AbstractNode):
    """
    Concrete node that also stores its output when run.
    """

    def __init__(self, nid, inputs=None, outputs=None):
        """
        :param nid: node identifier. Should be unique in a graph.
        :param inputs: list-like string names of input_names.
        :param outputs: list-like string names of output_names. The node is
            assumed to store the last output.
        """
        super().__init__(nid=nid, inputs=inputs, outputs=outputs)
        self._output_vals = {}

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

    def _store_output(self, method, name, value):
        """
        Store the output for a specific method.
        """
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


def assert_single_element_and_return(l):
    """
    Asserts that list `l` has a single element and returns it.

    :param l: list
    :return: returns the unique element of the list
    """
    dbg.dassert_isinstance(l, list)
    dbg.dassert_eq(len(l), 1, "List has %d elements!", len(l))
    return l[0]


# #############################################################################
# Graph class for creating and executing a DAG of nodes.
# #############################################################################


class DAG:
    """
    Class for building DAGs using Nodes.

    The DAG manages node execution and storage of outputs (within executed
    nodes).
    """

    def __init__(self, name=None):
        self._dag = nx.DiGraph()
        self._name = name

    @property
    def dag(self):
        return self._dag

    @property
    def name(self):
        return self._name

    def add_node(self, node):
        """
        Adds `node` to the graph.

        Relies upon the unique nid for identifying the node.

        :param node: Node object
        """
        # In principle, AbstractNode could be supported; however, to do so,
        # the `run` methods below would need to be suitably modified.
        dbg.dassert_isinstance(
            node, Node, "Only DAGs of class `Node` are supported!"
        )
        # NetworkX requires that nodes be hashable and uses hashes for
        # identifying nodes. Because our Nodes are objects whose hashes can
        # change as operations are performed, we use the Node.nid as the
        # NetworkX node and the Node instance as a `node attribute`, which we
        # identifying internally with the keyword `stage`.
        #
        # Note that this usage requires that nid's be unique within a given
        # DAG.
        if not self.dag.has_node(node.nid):
            self._dag.add_node(node.nid, stage=node)
            return
        # Allow `add_node` to be run again on equivalent nodes.
        # This is useful for research / notebook flows.
        if node == self.get_node(node.nid):
            _LOG.warning(
                "Node `%s` is already in DAG. Removing existing node (clears"
                "edges) and adding new equivalent node.", node.nid
            )
            self._dag.remove_node(node.nid)
            self._dag.add_node(node.nid, stage=node)
        else:
            dbg.dfatal("A node with nid={} is already in DAG!".format(node.nid))

    def get_node(self, nid):
        """
        Convenience node accessor.

        :param nid: unique string node id
        :return: Node object
        """
        dbg.dassert(
            self.dag.has_node(nid), "Node `%s` is not in DAG!", nid
        )
        return self.dag.nodes[nid]["stage"]

    def remove_node(self, nid):
        """
        Removes node from graph (and clears any edges).
        """
        self.dag.remove_node(nid)

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
        # Automatically infer output name when the parent has only one output.
        if isinstance(parent, tuple):
            parent_nid, parent_out = parent
            dbg.dassert_in(parent_out, self.get_node(parent_nid).output_names)
        else:
            parent_nid = parent
            parent_out = assert_single_element_and_return(
                self.get_node(parent_nid).output_names
            )
        # Automatically infer input name when the child has only one input.
        if isinstance(child, tuple):
            child_nid, child_in = child
            dbg.dassert_in(child_in, self.get_node(child_nid).input_names)
        else:
            child_nid = child
            child_in = assert_single_element_and_return(
                self.get_node(child_nid).input_names
            )
        # Ensure that `child_in` is not already hooked up to another output.
        for nid in self._dag.predecessors(child_nid):
            if nid == parent_nid:
                # This branch enables idempotency.
                edge_data = self.dag.get_edge_data(parent_nid, child_nid)
                if child_in in edge_data:
                    dbg.dassert_eq(edge_data[child_in], parent_out)
                    _LOG.warning(
                        "Edge %s:%s -> %s:%s already in DAG.",
                            parent_nid, parent_out, child_nid, child_in
                    )
            else:
                dbg.dassert_not_in(
                    child_in,
                    self.dag.get_edge_data(nid, child_nid),
                    "`%s` already receiving input from node %s",
                        child_in, nid
                )
        # Add the edge along with an `edge attribute` indicating the parent
        # output to connect to the child input.
        kwargs = {child_in: parent_out}
        self._dag.add_edge(parent_nid, child_nid, **kwargs)
        # If adding the edge causes the DAG property to be violated, remove the
        # edge and raise an error.
        if not nx.is_directed_acyclic_graph(self.dag):
            self._dag.remove_edge(parent_nid, child_nid)
            dbg.dfatal(
                "Creating edge {} -> {} introduces a cycle!".format(
                    parent_nid, child_nid
                )
            )

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
            node._store_output(method, out, output[out])
        # Convenient for experiments/debugging, but not needed for internal use.
        # Perhaps we should expose a public `run_node` that just invokes
        # `_run_node` and then returns the output as below.
        return self.get_node(nid).get_outputs(method)

    def run_dag(self, method):
        """
        Executes entire DAG.

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
            self._run_node(nid, method)
        return {sink: self.get_node(sink).get_outputs(method) for sink in sinks}

    def run_leq_node(self, nid, method):
        """
        Executes DAG up to (and including) Node `nid` and returns output.

        "leq" refers to the partial ordering on the vertices. This method
        runs a node if and only if there is a directed path from the node to
        `nid`. Nodes are run according to a topological sort.
        """
        ancestors = filter(
            lambda x: x in nx.ancestors(self._dag, nid),
            nx.topological_sort(self._dag),
        )
        # The `ancestors` filter only returns nodes strictly less than `nid`,
        # and so we need to add `nid` back.
        nids = itertools.chain(ancestors, [nid])
        for n in nids:
            self._run_node(n, method)
        return self.get_node(nid).get_outputs(method)
