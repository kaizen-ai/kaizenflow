import io
import json
import logging
from typing import Any, Callable, Dict, Tuple

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import pytest
import scipy

import core.config as cfg
import core.dataflow as dtf
import core.explore as exp
import core.pandas_helpers as pde
import core.residualizer as res
import core.statistics as stats
import helpers.dbg as dbg
import helpers.printing as pri
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################
# config.py
# #############################################################################


class Test_config1(ut.TestCase):
    def test_config1(self) -> None:
        """
        Test print flatten config.
        """
        config = cfg.Config()
        config["hello"] = "world"
        self.check_string(str(config))

    def _check_python(self, config: cfg.Config) -> str:
        code = config.to_python()
        _LOG.debug("code=%s", code)
        config2 = cfg.Config.from_python(code)
        #
        act = []
        act.append("config=%s" % str(config))
        act.append("code=%s" % str(code))
        act.append("config2=%s" % str(config2))
        act = "\n".join(act)
        self.assertEqual(str(config), str(config2))
        return act

    @staticmethod
    def _get_flat_config1() -> cfg.Config:
        config = cfg.Config()
        config["hello"] = "world"
        config["foo"] = [1, 2, 3]
        return config

    def test_config2(self) -> None:
        """
        Test serialization / deserialization for flat config.
        """
        config = self._get_flat_config1()
        #
        act = self._check_python(config)
        self.check_string(act)

    def test_config3(self) -> None:
        """
        Test Config.get()
        """
        config = cfg.Config()
        config["nrows"] = 10000
        #
        self.assertEqual(config["nrows"], 10000)
        self.assertEqual(config.get("nrows", None), 10000)
        #
        self.assertEqual(config.get("nrows_tmp", None), None)

    @staticmethod
    def _get_nested_config1() -> cfg.Config:
        config = cfg.Config()
        config["nrows"] = 10000
        #
        config.add_subconfig("read_data")
        config["read_data"]["file_name"] = "foo_bar.txt"
        config["read_data"]["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config.add_subconfig("zscore")
        config["zscore"]["style"] = "gaz"
        config["zscore"]["com"] = 28
        return config

    def test_get2(self) -> None:
        """
        Test Config.get() with missing key.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]
        with self.assertRaises(AssertionError):
            _ = config["read_data"]["file_name2"]
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]["file_name2"]
        elem = config["read_data"]["file_name"]
        self.assertEqual(elem, "foo_bar.txt")

    def test_config4(self) -> None:
        """
        Test print nested config.
        """
        config = self._get_nested_config1()
        act = str(config)
        self.check_string(act)

    def test_config5(self) -> None:
        """
        Test to_python() nested config.
        """
        config = self._get_nested_config1()
        act = config.to_python()
        self.check_string(act)

    def test_config6(self) -> None:
        """
        Test serialization / deserialization for nested config.
        """
        config = self._get_nested_config1()
        #
        act = self._check_python(config)
        self.check_string(act)

    @staticmethod
    def _get_nested_config2() -> cfg.Config:
        config = cfg.Config()
        config["nrows"] = 10000
        #
        config_tmp = config.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config["single_val"] = "hello"
        #
        config_tmp = config.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        return config

    def test_config7(self) -> None:
        """
        Compare two different styles of building a nested config.
        """
        config1 = self._get_nested_config1()
        config2 = self._get_nested_config2()
        #
        self.assertEqual(str(config1), str(config2))

    def test_hierarchical_getitem1(self) -> None:
        """
        Test accessing the config with hierarchical access.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        elem1 = config[("read_data", "file_name")]
        elem2 = config["read_data"]["file_name"]
        self.assertEqual(str(elem1), str(elem2))

    def test_hierarchical_getitem2(self) -> None:
        """
        Test accessing the config with hierarchical access with correct and
        incorrect paths.
        """
        config = self._get_nested_config1()
        _LOG.debug("config=%s", config)
        with self.assertRaises(AssertionError):
            _ = config["read_data2"]
        with self.assertRaises(AssertionError):
            _ = config[("read_data2", "file_name")]
        with self.assertRaises(AssertionError):
            _ = config[("read_data2")]
        with self.assertRaises(AssertionError):
            _ = config[["read_data2"]]
        #
        elem = config[("read_data", "file_name")]
        self.assertEqual(elem, "foo_bar.txt")

    def test_hierarchical_get1(self) -> None:
        """
        Show that hierarchical access is equivalent to chained access.
        """
        config = self._get_nested_config1()
        elem1 = config.get(("read_data", "file_name"), None)
        elem2 = config["read_data"]["file_name"]
        self.assertEqual(str(elem1), str(elem2))

    def test_hierarchical_get2(self) -> None:
        """
        Test `get()` with hierarchical access.
        """
        config = self._get_nested_config1()
        elem = config.get(("read_data2", "file_name"), "hello_world1")
        self.assertEqual(elem, "hello_world1")
        elem = config.get(("read_data2", "file_name2"), "hello_world2")
        self.assertEqual(elem, "hello_world2")
        elem = config.get(("read_data", "file_name2"), "hello_world3")
        self.assertEqual(elem, "hello_world3")

    def test_hierarchical_update1(self) -> None:
        config1 = cfg.Config()
        #
        config_tmp = config1.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config1["single_val"] = "hello"
        #
        config_tmp = config1.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config2 = cfg.Config()
        #
        config_tmp = config2.add_subconfig("write_data")
        config_tmp["file_name"] = "baz.txt"
        config_tmp["nrows"] = 999
        #
        config2["single_val2"] = "goodbye"
        #
        config_tmp = config2.add_subconfig("zscore2")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config1.update(config2)
        self.check_string(str(config1))

    def test_hierarchical_update2(self) -> None:
        config1 = cfg.Config()
        #
        config_tmp = config1.add_subconfig("read_data")
        config_tmp["file_name"] = "foo_bar.txt"
        config_tmp["nrows"] = 999
        #
        config1["single_val"] = "hello"
        #
        config_tmp = config1.add_subconfig("zscore")
        config_tmp["style"] = "gaz"
        config_tmp["com"] = 28
        #
        config2 = cfg.Config()
        #
        config_tmp = config2.add_subconfig("read_data")
        config_tmp["file_name"] = "baz.txt"
        config_tmp["nrows"] = 999
        #
        config2["single_val"] = "goodbye"
        #
        config_tmp = config2.add_subconfig("zscore")
        config_tmp["style"] = "super"
        #
        config_tmp = config2.add_subconfig("extra_zscore")
        config_tmp["style"] = "universal"
        config_tmp["tau"] = 32
        #
        config1.update(config2)
        self.check_string(str(config1))

    @pytest.mark.skip
    def test_hierarchical_update_empty_nested_config1(self) -> None:
        subconfig = cfg.Config()
        subconfig.add_subconfig("key0")
        #
        config = cfg.Config()
        config_tmp = config.add_subconfig("key1")
        config_tmp.update(subconfig)
        #
        expected_result = cfg.Config()
        config_tmp = expected_result.add_subconfig("key1")
        config_tmp.add_subconfig("key0")
        self.assertEqual(str(config), str(expected_result))

    def test_config_with_function(self) -> None:
        config = cfg.Config()
        config[
            "filters"
        ] = "[(<function _filter_relevance at 0x7fe4e35b1a70>, {'thr': 90})]"
        expected_result = "filters: [(<function _filter_relevance>, {'thr': 90})]"
        actual_result = str(config)
        self.assertEqual(actual_result, expected_result)


# #############################################################################
# dataflow_core.py
# #############################################################################


class _Dataflow_helper(ut.TestCase):
    @staticmethod
    def _remove_stage_names(node_link_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove stages names from node_link_data dictionary.

        The stage names refer to Node objects, which are not json serializable.
        """
        nld = node_link_data.copy()
        for data in nld["nodes"]:
            data["stage"] = data["stage"].__class__.__name__
        return nld

    def _check(self, dag: nx.classes.digraph.DiGraph) -> None:
        nld = nx.readwrite.json_graph.node_link_data(dag)
        nld = self._remove_stage_names(nld)
        _LOG.debug("stripped node_link_data=%s", nld)
        json_nld = json.dumps(nld, indent=4, sort_keys=True)
        self.check_string(json_nld)


class Test_dataflow_core_DAG1(_Dataflow_helper):
    def test_add_nodes1(self) -> None:
        """
        Creates a node and adds it to a DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1")
        dag.add_node(n1)
        self._check(dag.dag)

    def test_add_nodes2(self) -> None:
        """
        Demonstrates "strict" and "loose" behavior on repeated add_node().
        """
        dag_strict = dtf.DAG(mode="strict")
        m1 = dtf.Node("m1")
        dag_strict.add_node(m1)
        with self.assertRaises(AssertionError):
            dag_strict.add_node(m1)
        #
        dag_loose = dtf.DAG(mode="loose")
        n1 = dtf.Node("n1")
        dag_loose.add_node(n1)
        dag_loose.add_node(n1)
        self._check(dag_loose.dag)

    def test_add_nodes3(self) -> None:
        """
        Demonstrates "strict" and "loose" behavior on repeated add_node().
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
        self._check(dag_loose.dag)

    def test_add_nodes4(self) -> None:
        """
        Adds multiple nodes to a DAG.
        """
        dag = dtf.DAG()
        for name in ["n1", "n2", "n3", "n4"]:
            dag.add_node(dtf.Node(name, inputs=["in1"], outputs=["out1"]))
        self._check(dag.dag)

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
        self._check(dag.dag)


class Test_dataflow_core_DAG2(_Dataflow_helper):
    def test_connect_nodes1(self) -> None:
        """
        Simplest case of connecting two nodes.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), ("n2", "in1"))
        self._check(dag.dag)

    def test_connect_nodes2(self) -> None:
        """
        Simplest case, but inferred input/output names.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        self._check(dag.dag)

    def test_connect_nodes3(self) -> None:
        """
        Ensures input/output names are valid.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        with self.assertRaises(AssertionError):
            dag.connect(("n2", "out1"), ("n1", "in1"))

    def test_connect_nodes4(self) -> None:
        """
        Forbids creating cycles in DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", inputs=["in1"], outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), ("n2", "in1"))
        with self.assertRaises(AssertionError):
            dag.connect(("n2", "out1"), ("n1", "in1"))

    def test_connect_nodes5(self) -> None:
        """
        Forbids creating cycles in DAG (inferred input/output names).
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", inputs=["in1"], outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        with self.assertRaises(AssertionError):
            dag.connect("n2", "n1")

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
        self._check(dag.dag)

    def test_connect_nodes7(self) -> None:
        """
        Forbids connecting a node that doesn't belong to the DAG.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        with self.assertRaises(AssertionError):
            dag.connect("n2", "n1")

    def test_connect_nodes8(self) -> None:
        """
        Ensures at most one output connects to any input.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1", "out2"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), "n2")
        with self.assertRaises(AssertionError):
            dag.connect(("n1", "out2"), "n2")

    def test_connect_nodes9(self) -> None:
        """
        Allows multi-attribute edges if each input has at most one source.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1", "in2"])
        dag.add_node(n2)
        dag.connect("n1", ("n2", "in1"))
        dag.connect("n1", ("n2", "in2"))
        self._check(dag.dag)

    def test_connect_nodes10(self) -> None:
        """
        Demonstrates adding edges is not idempotent.
        """
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        with self.assertRaises(AssertionError):
            dag.connect("n1", "n2")


class Test_dataflow_core_DAG3(_Dataflow_helper):
    def test_sources_sinks1(self) -> None:
        dag = dtf.DAG()
        n1 = dtf.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtf.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        self.assertEqual(dag.get_sources(), ["n1"])
        self.assertEqual(dag.get_sinks(), ["n2"])

    def test_sources_sinks2(self) -> None:
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
        sources = dag.get_sources()
        sources.sort()
        self.assertListEqual(sources, ["src1", "src2"])
        sinks = dag.get_sinks()
        sinks.sort()
        self.assertListEqual(sinks, ["snk1", "snk2"])

    def test_sources_sinks3(self) -> None:
        dag = dtf.DAG()
        n1 = dtf.Node("n1")
        dag.add_node(n1)
        self.assertEqual(dag.get_sources(), ["n1"])
        self.assertEqual(dag.get_sinks(), ["n1"])


# #############################################################################
# explore.py
# #############################################################################


class Test_explore1(ut.TestCase):
    def test_ols_regress_series(self) -> None:
        x = 5 * np.random.randn(100)
        y = x + np.random.randn(*x.shape)
        df = pd.DataFrame()
        df["x"] = x
        df["y"] = y
        exp.ols_regress_series(
            df["x"], df["y"], intercept=True, print_model_stats=False
        )

    def test_rolling_pca_over_time1(self) -> None:
        np.random.seed(42)
        df = pd.DataFrame(np.random.randn(10, 5))
        df.index = pd.date_range("2017-01-01", periods=10)
        corr_df, eigval_df, eigvec_df = exp.rolling_pca_over_time(
            df, 0.5, "fill_with_zero"
        )
        txt = (
            "corr_df=\n%s\n" % corr_df.to_string()
            + "eigval_df=\n%s\n" % eigval_df.to_string()
            + "eigvec_df=\n%s\n" % eigvec_df.to_string()
        )
        self.check_string(txt)


# #############################################################################
# pandas_helpers.py
# #############################################################################


# TODO(gp): -> Test_pandas_helper1
class TestResampleIndex1(ut.TestCase):
    def test1(self) -> None:
        index = pd.date_range(start="01-04-2018", periods=200, freq="30T")
        df = pd.DataFrame(np.random.rand(len(index), 3), index=index)
        txt = []
        txt.extend(["df.head()=", df.head()])
        txt.extend(["df.tail()=", df.tail()])
        resampled_index = pde.resample_index(df.index, time=(10, 30), freq="D")
        # Normalize since the format seems to be changing on different machines.
        txt_tmp = str(resampled_index).replace("\n", "").replace(" ", "")
        txt.extend(["resampled_index=", txt_tmp])
        result = df.loc[resampled_index]
        txt.extend(["result=", str(result)])
        txt = "\n".join(map(str, txt))
        self.check_string(txt)


# #############################################################################


# TODO(gp): -> Test_pandas_helper2
class TestDfRollingApply(ut.TestCase):
    def test1(self) -> None:
        """
        Test with function returning a pd.Series.
        """
        df_str = pri.dedent(
            """
         ,A,B
        2018-01-01,0.47,0.01
        2018-01-02,0.83,0.43
        2018-01-04,0.81,0.79
        2018-01-05,0.83,0.93
        2018-01-06,0.66,0.71
        2018-01-08,0.41,0.6
        2018-01-09,0.83,0.82
        2019-01-10,0.69,0.82
        """
        )
        df_str = io.StringIO(df_str)
        df = pd.read_csv(df_str, index_col=0)
        #
        window = 5
        func = np.mean
        df_act = pde.df_rolling_apply(df, window, func)
        #
        df_exp = df.rolling(window).apply(func, raw=True)
        # Check.
        exp_val = [0.720, 0.574]
        np.testing.assert_array_almost_equal(
            df.loc["2018-01-01":"2018-01-06"].mean().tolist(), exp_val
        )
        np.testing.assert_array_almost_equal(
            df_act.loc["2018-01-06"].tolist(), exp_val
        )
        self.assert_equal(df_act.to_string(), df_exp.to_string())
        self.check_string(df_act.to_string())

    def test2(self) -> None:
        """
        Test with function returning a pd.Series.
        """
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=["A", "B"])
        #
        window = 5
        func = np.mean
        df_act = pde.df_rolling_apply(df, window, func)
        #
        df_exp = df.rolling(window).apply(func, raw=True)
        # Check.
        self.assert_equal(df_act.to_string(), df_exp.to_string())
        self.check_string(df_act.to_string())

    def test3(self) -> None:
        """
        Test with function returning a pd.DataFrame.
        """
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=["A", "B"])
        #
        window = 5
        func = lambda x: pd.DataFrame(np.mean(x))
        df_act = pde.df_rolling_apply(df, window, func)
        #
        func = np.mean
        df_exp = df.rolling(window).apply(func, raw=True)
        # Convert to an equivalent format.
        df_exp = pd.DataFrame(df_exp.stack(dropna=False))
        # Check.
        self.assert_equal(df_act.to_string(), df_exp.to_string())
        self.check_string(df_act.to_string())

    def test4(self) -> None:
        """
        Test with function returning a pd.DataFrame with multiple lines.
        """
        df = pd.DataFrame(np.random.rand(100, 2).round(2), columns=["A", "B"])
        #
        window = 5
        func = lambda x: pd.DataFrame([np.mean(x), np.sum(x)])
        df_act = pde.df_rolling_apply(df, window, func)
        # Check.
        self.check_string(df_act.to_string())

    def test5(self) -> None:
        """
        Like test1 but with a down-sampled version of the data.
        """
        dts = pd.date_range(start="2009-01-04", end="2009-01-10", freq="1H")
        df = pd.DataFrame(
            np.random.rand(len(dts), 2).round(2), columns=["A", "B"], index=dts
        )
        #
        resampled_index = pde.resample_index(df.index, time=(9, 0), freq="1D")
        self.assertEqual(len(resampled_index), 6)
        #
        window = 5
        func = np.mean
        df_act = pde.df_rolling_apply(
            df, window, func, timestamps=resampled_index
        )
        # Check.
        df_tmp = df.loc["2009-01-04 05:00:00":"2009-01-04 09:00:00"]
        exp_val = [0.592, 0.746]
        np.testing.assert_array_almost_equal(df_tmp.mean().tolist(), exp_val)
        np.testing.assert_array_almost_equal(
            df_act.loc["2009-01-04 09:00:00"].tolist(), exp_val
        )
        #
        df_tmp = df.loc["2009-01-09 05:00:00":"2009-01-09 09:00:00"]
        exp_val = [0.608, 0.620]
        np.testing.assert_array_almost_equal(df_tmp.mean().tolist(), exp_val)
        np.testing.assert_array_almost_equal(
            df_act.loc["2009-01-09 09:00:00"].tolist(), exp_val
        )
        #
        self.check_string(df_act.to_string())


# #############################################################################
# residualizer.py
# #############################################################################


# TODO(gp): -> Test_residualizer1
class TestPcaFactorComputer1(ut.TestCase):
    @staticmethod
    def get_ex1() -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
    ]:
        df_str = pri.dedent(
            """
        ,0,1,2
        0,0.68637724274453,0.34344509725064354,0.6410395820984168
        1,-0.7208890365507423,0.205021903910637,0.6620309780499695
        2,-0.09594413803541411,0.916521404055221,-0.3883081743735094"""
        )
        df_str = io.StringIO(df_str)
        prev_eigvec_df = pd.read_csv(df_str, index_col=0)
        prev_eigvec_df.index = prev_eigvec_df.index.map(int)
        prev_eigvec_df.columns = prev_eigvec_df.columns.map(int)
        #
        prev_eigval_df = pd.DataFrame([[1.0, 0.5, 0.3]], columns=[0, 1, 2])
        # Shuffle eigenvalues / eigenvectors.
        eigvec_df = prev_eigvec_df.copy()
        shuffle = [1, 2, 0]
        eigvec_df = eigvec_df.reindex(columns=shuffle)
        eigvec_df.columns = list(range(eigvec_df.shape[1]))
        eigvec_df.iloc[:, 1] *= -1
        #
        eigval_df = prev_eigval_df.reindex(columns=shuffle)
        eigval_df.columns = list(range(eigval_df.shape[1]))
        for obj in (prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df):
            dbg.dassert_monotonic_index(obj)
        return prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df

    def _test_stabilize_eigenvec_helper(
        self, data_func: Callable, eval_func: Callable
    ) -> None:
        # Get data.
        prev_eigval_df, eigval_df, prev_eigvec_df, eigvec_df = data_func()
        # Check if they are stable.
        num_fails = res.PcaFactorComputer.are_eigenvectors_stable(
            prev_eigvec_df, eigvec_df
        )
        self.assertEqual(num_fails, 3)
        # Transform.
        col_map, _ = eval_func(prev_eigvec_df, eigvec_df)
        #
        obj = res.PcaFactorComputer.shuffle_eigval_eigvec(
            eigval_df, eigvec_df, col_map
        )
        shuffled_eigval_df, shuffled_eigvec_df = obj
        # Check.
        txt = (
            "prev_eigval_df=\n%s\n" % prev_eigval_df
            + "prev_eigvec_df=\n%s\n" % prev_eigvec_df
            + "eigval_df=\n%s\n" % eigval_df
            + "eigvec_df=\n%s\n" % eigvec_df
            + "shuffled_eigval_df=\n%s\n" % shuffled_eigval_df
            + "shuffled_eigvec_df=\n%s\n" % shuffled_eigvec_df
        )
        self.check_string(txt)
        # Check stability.
        num_fails = res.PcaFactorComputer.are_eigenvectors_stable(
            prev_eigvec_df, shuffled_eigvec_df
        )
        self.assertEqual(num_fails, 0)
        self.assertTrue(
            res.PcaFactorComputer.are_eigenvalues_stable(
                prev_eigval_df, shuffled_eigval_df
            )
        )

    def test_stabilize_eigenvec1(self) -> None:
        data_func = self.get_ex1
        eval_func = res.PcaFactorComputer._build_stable_eig_map
        self._test_stabilize_eigenvec_helper(data_func, eval_func)

    def test_stabilize_eigenvec2(self) -> None:
        data_func = self.get_ex1
        eval_func = res.PcaFactorComputer._build_stable_eig_map2
        self._test_stabilize_eigenvec_helper(data_func, eval_func)

    # #########################################################################

    def test_linearize_eigval_eigvec(self) -> None:
        # Get data.
        eigval_df, _, eigvec_df, _ = self.get_ex1()
        # Evaluate.
        out = res.PcaFactorComputer.linearize_eigval_eigvec(eigval_df, eigvec_df)
        _LOG.debug("out=\n%s", out)
        # Check.
        txt = (
            "eigval_df=\n%s\n" % eigval_df
            + "eigvec_df=\n%s\n" % eigvec_df
            + "out=\n%s" % out
        )
        self.check_string(txt)

    # #########################################################################

    def _test_sort_eigval_helper(
        self, eigval: np.ndarray, eigvec: np.ndarray, are_eigval_sorted_exp: bool
    ) -> None:
        # pylint: disable=possibly-unused-variable
        obj = res.PcaFactorComputer.sort_eigval(eigval, eigvec)
        are_eigval_sorted, eigval_tmp, eigvec_tmp = obj
        self.assertEqual(are_eigval_sorted, are_eigval_sorted_exp)
        self.assertSequenceEqual(
            eigval_tmp.tolist(), sorted(eigval_tmp, reverse=True)
        )
        vars_as_str = [
            "eigval",
            "eigvec",
            "are_eigval_sorted",
            "eigval_tmp",
            "eigvec_tmp",
        ]
        txt = pri.vars_to_debug_string(vars_as_str, locals())
        self.check_string(txt)

    def test_sort_eigval1(self) -> None:
        eigval = np.array([1.30610138, 0.99251131, 0.70138731])
        eigvec = np.array(
            [
                [-0.55546523, 0.62034663, 0.55374041],
                [0.70270302, -0.00586218, 0.71145914],
                [-0.4445974, -0.78430587, 0.43266321],
            ]
        )
        are_eigval_sorted_exp = True
        self._test_sort_eigval_helper(eigval, eigvec, are_eigval_sorted_exp)

    def test_sort_eigval2(self) -> None:
        eigval = np.array([0.99251131, 0.70138731, 1.30610138])
        eigvec = np.array(
            [
                [-0.55546523, 0.62034663, 0.55374041],
                [0.70270302, -0.00586218, 0.71145914],
                [-0.4445974, -0.78430587, 0.43266321],
            ]
        )
        are_eigval_sorted_exp = False
        self._test_sort_eigval_helper(eigval, eigvec, are_eigval_sorted_exp)


# #############################################################################


class TestPcaFactorComputer2(ut.TestCase):
    @staticmethod
    def _get_data(num_samples: int, report_stats: bool) -> Dict[str, Any]:
        # The desired covariance matrix.
        # r = np.array([
        #         [  3.40, -2.75, -2.00],
        #         [ -2.75,  5.50,  1.50],
        #         [ -2.00,  1.50,  1.25]
        #     ])
        cov = np.array([[1.0, 0.5, 0], [0.5, 1, 0], [0, 0, 1]])
        if report_stats:
            _LOG.info("cov=\n%s", cov)
            exp.plot_heatmap(cov, mode="heatmap", title="cov")
            plt.show()
        # Generate samples from three independent normally distributed random
        # variables with mean 0 and std dev 1.
        x = scipy.stats.norm.rvs(size=(3, num_samples))
        if report_stats:
            _LOG.info("x=\n%s", x[:2, :])
        # We need a matrix `c` for which `c*c^T = r`.
        # We can use # the Cholesky decomposition, or the we can construct `c`
        # from the eigenvectors and eigenvalues.
        # Compute the eigenvalues and eigenvectors.
        # evals, evecs = np.linalg.eig(r)
        evals, evecs = np.linalg.eigh(cov)
        if report_stats:
            _LOG.info("evals=\n%s", evals)
            _LOG.info("evecs=\n%s", evecs)
            exp.plot_heatmap(evecs, mode="heatmap", title="evecs")
            plt.show()
        # Construct c, so c*c^T = r.
        transform = np.dot(evecs, np.diag(np.sqrt(evals)))
        if report_stats:
            _LOG.info("transform=\n%s", transform)
        # print(c.T * c)
        # print(c * c.T)
        # Convert the data to correlated random variables.
        y = np.dot(transform, x)
        y_cov = np.corrcoef(y)
        if report_stats:
            _LOG.info("cov(y)=\n%s", y_cov)
            exp.plot_heatmap(y_cov, mode="heatmap", title="y_cov")
            plt.show()
        #
        y = pd.DataFrame(y).T
        _LOG.debug("y=\n%s", y.head(5))
        result = {
            "y": y,
            "cov": cov,
            "evals": evals,
            "evecs": evecs,
            "transform": transform,
        }
        return result

    def _helper(
        self,
        num_samples: int,
        report_stats: bool,
        stabilize_eig: bool,
        window: int,
    ) -> Tuple[res.PcaFactorComputer, pd.DataFrame]:
        result = self._get_data(num_samples, report_stats)
        _LOG.debug("result=%s", result.keys())
        #
        nan_mode_in_data = "drop"
        nan_mode_in_corr = "fill_with_zero"
        sort_eigvals = True
        comp = res.PcaFactorComputer(
            nan_mode_in_data, nan_mode_in_corr, sort_eigvals, stabilize_eig
        )
        df_res = pde.df_rolling_apply(
            result["y"], window, comp, progress_bar=True
        )
        if report_stats:
            comp.plot_over_time(df_res, num_pcs_to_plot=-1)
        return comp, df_res

    def _check(self, comp: res.PcaFactorComputer, df_res: pd.DataFrame) -> None:
        txt = []
        txt.append("comp.get_eigval_names()=\n%s" % comp.get_eigval_names())
        txt.append("df_res.mean()=\n%s" % df_res.mean())
        txt.append("df_res.std()=\n%s" % df_res.std())
        txt = "\n".join(txt)
        self.check_string(txt)

    def test1(self) -> None:
        num_samples = 100
        report_stats = False
        stabilize_eig = False
        window = 50
        comp, df_res = self._helper(
            num_samples, report_stats, stabilize_eig, window
        )
        self._check(comp, df_res)

    def test2(self) -> None:
        num_samples = 100
        report_stats = False
        stabilize_eig = True
        window = 50
        comp, df_res = self._helper(
            num_samples, report_stats, stabilize_eig, window
        )
        self._check(comp, df_res)


# #############################################################################
# statistics.py
# #############################################################################


class TestComputeFracZero1(ut.TestCase):
    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        nrows = 15
        ncols = 5
        num_nans = 15
        num_infs = 5
        num_zeros = 20
        #
        np.random.seed(seed=seed)
        mat = np.random.randn(nrows, ncols)
        mat.ravel()[np.random.choice(mat.size, num_nans, replace=False)] = np.nan
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = np.inf
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = -np.inf
        mat.ravel()[np.random.choice(mat.size, num_zeros, replace=False)] = 0
        #
        index = pd.date_range(start="01-04-2018", periods=nrows, freq="30T")
        df = pd.DataFrame(data=mat, index=index)
        return df

    def test1(self) -> None:
        data = [0.466667, 0.2, 0.13333, 0.2, 0.33333]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_zero(self._get_df(1))
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test2(self) -> None:
        data = [
            0.4,
            0.0,
            0.2,
            0.4,
            0.4,
            0.2,
            0.4,
            0.0,
            0.6,
            0.4,
            0.6,
            0.2,
            0.0,
            0.0,
            0.2,
        ]
        index = pd.date_range(start="1-04-2018", periods=15, freq="30T")
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_zero(self._get_df(1), axis=1)
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test3(self) -> None:
        # Equals 20 / 75 = num_zeros / num_points.
        expected = 0.266666
        actual = stats.compute_frac_zero(self._get_df(1), axis=None)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test4(self) -> None:
        series = self._get_df(1)[0]
        expected = 0.466667
        actual = stats.compute_frac_zero(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test5(self) -> None:
        series = self._get_df(1)[0]
        expected = 0.466667
        actual = stats.compute_frac_zero(series, axis=0)
        np.testing.assert_almost_equal(actual, expected, decimal=3)


class TestComputeFracNan1(ut.TestCase):
    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        nrows = 15
        ncols = 5
        num_nans = 15
        num_infs = 5
        num_zeros = 20
        #
        np.random.seed(seed=seed)
        mat = np.random.randn(nrows, ncols)
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = np.inf
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = -np.inf
        mat.ravel()[np.random.choice(mat.size, num_zeros, replace=False)] = 0
        mat.ravel()[np.random.choice(mat.size, num_nans, replace=False)] = np.nan
        #
        index = pd.date_range(start="01-04-2018", periods=nrows, freq="30T")
        df = pd.DataFrame(data=mat, index=index)
        return df

    def test1(self) -> None:
        data = [0.4, 0.133333, 0.133333, 0.133333, 0.2]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_nan(self._get_df(1))
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test2(self) -> None:
        data = [
            0.4,
            0.0,
            0.2,
            0.4,
            0.2,
            0.2,
            0.2,
            0.0,
            0.4,
            0.2,
            0.6,
            0.0,
            0.0,
            0.0,
            0.2,
        ]
        index = pd.date_range(start="1-04-2018", periods=15, freq="30T")
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_nan(self._get_df(1), axis=1)
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test3(self) -> None:
        # Equals 15 / 75 = num_nans / num_points.
        expected = 0.2
        actual = stats.compute_frac_nan(self._get_df(1), axis=None)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test4(self) -> None:
        series = self._get_df(1)[0]
        expected = 0.4
        actual = stats.compute_frac_nan(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)

    def test5(self) -> None:
        series = self._get_df(1)[0]
        expected = 0.4
        actual = stats.compute_frac_nan(series, axis=0)
        np.testing.assert_almost_equal(actual, expected, decimal=3)


class TestComputeFracConstant1(ut.TestCase):
    @staticmethod
    def _get_df(seed: int) -> pd.DataFrame:
        nrows = 15
        ncols = 5
        num_nans = 4
        num_infs = 2
        #
        np.random.seed(seed=1)
        mat = np.random.randint(-1, 1, (nrows, ncols)).astype("float")
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = np.inf
        mat.ravel()[np.random.choice(mat.size, num_infs, replace=False)] = -np.inf
        mat.ravel()[np.random.choice(mat.size, num_nans, replace=False)] = np.nan
        #
        index = pd.date_range(start="01-04-2018", periods=nrows, freq="30T")
        df = pd.DataFrame(data=mat, index=index)
        return df

    def test1(self) -> None:
        data = [0.357143, 0.5, 0.285714, 0.285714, 0.071429]
        index = [0, 1, 2, 3, 4]
        expected = pd.Series(data=data, index=index)
        actual = stats.compute_frac_constant(self._get_df(1))
        pd.testing.assert_series_equal(actual, expected, check_less_precise=3)

    def test2(self) -> None:
        series = self._get_df(1)[0]
        expected = 0.357143
        actual = stats.compute_frac_constant(series)
        np.testing.assert_almost_equal(actual, expected, decimal=3)
