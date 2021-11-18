import json
import logging
import os
from typing import Any, Dict

import networkx as nx

import core.dataflow as dtf
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class _Dataflow_helper(hunitest.TestCase):
    @staticmethod
    def _remove_stage_names(node_link_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Remove stages names from `node_link_data` dictionary.

        The stage names refer to Node objects, which are not json
        serializable.
        """
        _LOG.debug("nld=\n%s", hprint.to_pretty_str(node_link_data))
        # `nld` looks like:
        #   {'directed': True,
        #    'graph': {},
        #    'links': [],
        #    'multigraph': False,
        #    'nodes': [{'id': 'n1',
        #               'stage': <core.dataflow.core.Node object at 0x...>}]}
        nld = node_link_data.copy()
        for data in nld["nodes"]:
            data["stage"] = data["stage"].__class__.__name__
        return nld

    def _check(self, dag: dtf.DAG) -> None:
        """
        Compute and freeze with `check_string` the signature of a graph.
        """
        graph: nx.classes.digraph.DiGraph = dag.dag
        nld = nx.readwrite.json_graph.node_link_data(graph)
        nld = self._remove_stage_names(nld)
        _LOG.debug("stripped node_link_data=\n%s", hprint.to_pretty_str(nld))
        json_nld = json.dumps(nld, indent=4, sort_keys=True)
        _LOG.debug("output=\n%s", json_nld)
        # Visualize if needed.
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "graph.png")
        dtf.draw_to_file(dag, file_name)
        _LOG.debug("Saved plot to %s", file_name)
        # Check output.
        self.check_string(json_nld)


class Test_dataflow_core_DAG1(_Dataflow_helper):
    def test_add_nodes1(self) -> None:
        """
        Create a node and add it to a DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1")
        dag.add_node(n1)
        self._check(dag)

    def test_add_nodes2(self) -> None:
        """
        Demonstrate "strict" and "loose" behavior on repeated add_node().
        """
        dag_strict = dtf.DAG(mode="strict")
        m1 = dtf.Node("m1")
        dag_strict.add_node(m1)
        # Assert when adding a node that already exists.
        with self.assertRaises(AssertionError):
            dag_strict.add_node(m1)
        #
        dag_loose = dtf.DAG(mode="loose")
        n1 = dtf.Node("n1")
        # Add the same node twice.
        dag_loose.add_node(n1)
        dag_loose.add_node(n1)
        self._check(dag_loose)

    def test_add_nodes3(self) -> None:
        """
        Demonstrate "strict" and "loose" behavior on repeated add_node().

        Same as `test_add_nodes2()` but creating another node.
        """
        dag_strict = dtf.DAG(mode="strict")
        m1 = dtf.Node("m1")
        dag_strict.add_node(m1)
        m1_prime = dtf.Node("m1")
        with self.assertRaises(AssertionError):
            dag_strict.add_node(m1_prime)
        #
        dag_loose = dtf.DAG(mode="loose")
        n1 = dtf.Node("n1")
        dag_loose.add_node(n1)
        n1_prime = dtf.Node("n1")
        dag_loose.add_node(n1_prime)
        self._check(dag_loose)

    def test_add_nodes4(self) -> None:
        """
        Add multiple nodes to a DAG.
        """
        dag = dtf.DAG()
        for name in ["n1", "n2", "n3", "n4"]:
            dag.add_node(dtf.Node(name, inputs=["in1"], outputs=["out1"]))
        self._check(dag)

    def test_add_nodes5(self) -> None:
        """
        Re-adding a node clears node, successors, and edges in `loose` mode.
        """
        dag = dtf.DAG(mode="loose")
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        n3 = dtf.Node("n3", inputs=["in1"])
        dag.add_node(n3)
        dag.connect("n2", "n3")
        dag.add_node(n1)
        self._check(dag)


class Test_dataflow_core_DAG2(_Dataflow_helper):
    def test_connect_nodes1(self) -> None:
        """
        Simplest case of connecting two nodes.
        """
        dag = self._get_two_nodes()
        dag.connect(("n1", "out1"), ("n2", "in1"))
        self._check(dag)

    def test_connect_nodes2(self) -> None:
        """
        Simplest case, but inferred input/output names.
        """
        dag = self._get_two_nodes()
        dag.connect("n1", "n2")
        self._check(dag)

    def test_connect_nodes3(self) -> None:
        """
        Ensure input/output names are valid.
        """
        dag = self._get_two_nodes()
        with self.assertRaises(AssertionError) as cm:
            dag.connect(("n2", "out1"), ("n1", "in1"))
        act = str(cm.exception)
        exp = """
        * Failed assertion *
        'out1' in '[]'
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_connect_nodes4(self) -> None:
        """
        Forbid creating cycles in DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", inputs=["in1"], outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        # n1 -> n2
        dag.connect(("n1", "out1"), ("n2", "in1"))
        # n2 -> n1 creates a cycle.
        with self.assertRaises(AssertionError) as cm:
            dag.connect(("n2", "out1"), ("n1", "in1"))
        act = str(cm.exception)
        exp = """
        Creating edge n2 -> n1 introduces a cycle!
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_connect_nodes5(self) -> None:
        """
        Forbid creating cycles in DAG (inferred input/output names).
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", inputs=["in1"], outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        with self.assertRaises(AssertionError) as cm:
            dag.connect("n2", "n1")
        act = str(cm.exception)
        exp = r"""
        Creating edge n2 -> n1 introduces a cycle!
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_connect_nodes6(self) -> None:
        """
        A nontrivial, multi-input/output example.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"], outputs=["out1", "out2"])
        dag.add_node(n2)
        n3 = dtf.Node("n3", inputs=["in1"], outputs=["out1"])
        dag.add_node(n3)
        n4 = dtf.Node("n4", inputs=["in1"], outputs=["out1"])
        dag.add_node(n4)
        n5 = dtf.Node("n5", inputs=["in1", "in2"], outputs=["out1"])
        dag.add_node(n5)
        dag.connect("n1", ("n2", "in1"))
        dag.connect(("n2", "out1"), "n3")
        dag.connect(("n2", "out2"), "n4")
        dag.connect("n3", ("n5", "in1"))
        dag.connect("n4", ("n5", "in2"))
        self._check(dag)

    def test_connect_nodes7(self) -> None:
        """
        Forbid connecting a node that doesn't belong to the DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        with self.assertRaises(AssertionError) as cm:
            dag.connect("n2", "n1")
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        cond=False
        Node `n2` is not in DAG!
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_connect_nodes8(self) -> None:
        """
        Ensure at most one output connects to any input.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1", "out2"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), "n2")
        with self.assertRaises(AssertionError) as cm:
            dag.connect(("n1", "out2"), "n2")
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        'in1' not in '{'in1': 'out1'}'
        `in1` already receiving input from node n1
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    def test_connect_nodes9(self) -> None:
        """
        Allow multi-attribute edges if each input has at most one source.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1", "in2"])
        dag.add_node(n2)
        dag.connect("n1", ("n2", "in1"))
        dag.connect("n1", ("n2", "in2"))
        self._check(dag)

    def test_connect_nodes10(self) -> None:
        """
        Demonstrate adding edges is not idempotent.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        with self.assertRaises(AssertionError) as cm:
            dag.connect("n1", "n2")
        act = str(cm.exception)
        exp = r"""
        * Failed assertion *
        'in1' not in '{'in1': 'out1'}'
        `in1` already receiving input from node n1
        """
        self.assert_equal(act, exp, fuzzy_match=True)

    @staticmethod
    def _get_two_nodes() -> dtf.DAG:
        """
        Return a DAG with two unconnected nodes.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        return dag


class Test_dataflow_core_DAG3(_Dataflow_helper):
    def test_sources_sinks1(self) -> None:
        """
        Check sources and sinks of a single node linear DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1")
        dag.add_node(n1)
        #
        self.assertEqual(dag.get_sources(), ["n1"])
        self.assertEqual(dag.get_sinks(), ["n1"])

    def test_sources_sinks2(self) -> None:
        """
        Check sources and sinks of a two node linear DAG.
        """
        # Build a DAG n1 -> n2
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        # Check.
        self.assertEqual(dag.get_sources(), ["n1"])
        self.assertEqual(dag.get_sinks(), ["n2"])

    def test_sources_sinks3(self) -> None:
        dag = dtf.DAG()
        src1 = dtf.Node("src1", outputs=["out1"])
        dag.add_node(src1)
        src2 = dtf.Node("src2", outputs=["out1"])
        dag.add_node(src2)
        m1 = dtf.Node("m1", inputs=["in1", "in2"], outputs=["out1"])
        dag.add_node(m1)
        dag.connect("src1", ("m1", "in1"))
        dag.connect("src2", ("m1", "in2"))
        snk1 = dtf.Node("snk1", inputs=["in1"])
        dag.add_node(snk1)
        dag.connect("m1", "snk1")
        snk2 = dtf.Node("snk2", inputs=["in1"])
        dag.add_node(snk2)
        dag.connect("m1", "snk2")
        #
        sources = dag.get_sources()
        sources.sort()
        self.assertListEqual(sources, ["src1", "src2"])
        #
        sinks = dag.get_sinks()
        sinks.sort()
        self.assertListEqual(sinks, ["snk1", "snk2"])
