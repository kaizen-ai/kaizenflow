import io
import logging

import json
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
from scipy.stats import norm

import core.config as cfg
import core.dataflow_core as dtfc
import core.dataflow_old as dtf_old
import core.explore as exp
import core.pandas_helpers as pde
import core.residualizer as res
import core.signal_processing as sigp
import helpers.dbg as dbg
import helpers.printing as pri
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


# #############################################################################
# config.py
# #############################################################################


class Test_config1(ut.TestCase):
    def test_config1(self):
        """
        Test print flatten config.
        """
        config = cfg.Config()
        config["hello"] = "world"
        self.check_string(str(config))

    def _check_python(self, config):
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
    def _get_flat_config1():
        config = cfg.Config()
        config["hello"] = "world"
        config["foo"] = [1, 2, 3]
        return config

    def test_config2(self):
        """
        Test serialization / deserialization for flat config.
        """
        config = self._get_flat_config1()
        #
        act = self._check_python(config)
        self.check_string(act)

    def test_config3(self):
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
    def _get_nested_config1():
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

    def test_config4(self):
        """
        Test print nested config.
        """
        config = self._get_nested_config1()
        act = str(config)
        self.check_string(act)

    def test_config5(self):
        """
        Test to_python() nested config.
        """
        config = self._get_nested_config1()
        act = config.to_python()
        self.check_string(act)

    def test_config6(self):
        """
        Test serialization / deserialization for nested config.
        """
        config = self._get_nested_config1()
        #
        act = self._check_python(config)
        self.check_string(act)

    @staticmethod
    def _get_nested_config2():
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

    def test_config7(self):
        """
        Compare two different styles of building a nested config.
        """
        config1 = self._get_nested_config1()
        config2 = self._get_nested_config2()
        #
        self.assertEqual(str(config1), str(config2))


# #############################################################################
# dataflow.py
# #############################################################################


class _Dataflow_helper(ut.TestCase):
    def _remove_stage_names(self, node_link_data):
        """
        Remove stages names from node_link_data dictionary.

        The stage names refer to Node objects, which are not json serializable.
        """
        nld = node_link_data.copy()
        for data in nld['nodes']:
            data['stage'] = None
        return nld

    def _check(self, dag):
        nld = nx.readwrite.json_graph.node_link_data(dag)
        nld = self._remove_stage_names(nld)
        _LOG.debug("stripped node_link_data=%s", nld)
        json_nld = json.dumps(nld, indent=4, sort_keys=True)
        self.check_string(json_nld)


class Test_dataflow_core_DAG1(_Dataflow_helper):
    def test_add_nodes1(self):
        """
        Creates a node and adds it to a DAG.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1")
        dag.add_node(n1)
        self._check(dag.dag)

    def test_add_nodes2(self):
        """
        Demonstrates that add_node is idempotent with equivalent nodes.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1")
        dag.add_node(n1)
        n1_prime = dtfc.Node("n1")
        dag.add_node(n1_prime)
        self._check(dag.dag)

    def test_add_nodes3(self):
        """
        Requires nid uniqueness in a DAG, up to equivalence of Nodes.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1")
        dag.add_node(n1)
        n1_prime = dtfc.Node("n1", inputs=["in1"])
        with self.assertRaises(AssertionError):
            dag.add_node(n1_prime)

    def test_add_nodes4(self):
        """
        Adds multiple nodes to a DAG.
        """
        dag = dtfc.DAG()
        for name in ["n1", "n2", "n3", "n4"]:
            dag.add_node(dtfc.Node(name, inputs=["in1"], outputs=["out1"]))
        self._check(dag.dag)

    def test_add_nodes5(self):
        """
        Demonstrates that re-adding a node clears edges.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        dag.add_node(n1)
        self._check(dag.dag)


class Test_dataflow_core_DAG2(_Dataflow_helper):
    def test_connect_nodes1(self):
        """
        Simplest case of connecting two nodes.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), ("n2", "in1"))
        self._check(dag.dag)

    def test_connect_nodes2(self):
        """
        Simplest case, but inferred input/output names.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        self._check(dag.dag)

    def test_connect_nodes3(self):
        """
        Ensures input/output names are valid.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        with self.assertRaises(AssertionError):
           dag.connect(("n2", "out1"), ("n1", "in1"))

    def test_connect_nodes4(self):
        """
        Forbids creating cycles in DAG.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", inputs=["in1"], outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), ("n2", "in1"))
        with self.assertRaises(AssertionError):
            dag.connect(("n2", "out1"), ("n1", "in1"))

    def test_connect_nodes5(self):
        """
        Forbids creating cycles in DAG (inferred input/output names).
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", inputs=["in1"], outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"], outputs=["out1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        with self.assertRaises(AssertionError):
            dag.connect("n2", "n1")

    def test_connect_nodes6(self):
        """
        A nontrivial, multi-input/output example.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"], outputs=["out1", "out2"])
        dag.add_node(n2)
        n3 = dtfc.Node("n3", inputs=["in1"], outputs=["out1"])
        dag.add_node(n3)
        n4 = dtfc.Node("n4", inputs=["in1"], outputs=["out1"])
        dag.add_node(n4)
        n5 = dtfc.Node("n5", inputs=["in1", "in2"], outputs=["out1"])
        dag.add_node(n5)
        dag.connect("n1", ("n2", "in1"))
        dag.connect(("n2", "out1"), "n3")
        dag.connect(("n2", "out2"), "n4")
        dag.connect("n3", ("n5", "in1"))
        dag.connect("n4", ("n5", "in2"))
        self._check(dag.dag)

    def test_connect_nodes7(self):
        """
        Forbids connecting a node that doesn't belong to the DAG.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        with self.assertRaises(AssertionError):
            dag.connect("n2", "n1")

    def test_connect_nodes8(self):
        """
        Ensures at most one output connects to any input.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1", "out2"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect(("n1", "out1"), "n2")
        with self.assertRaises(AssertionError):
            dag.connect(("n1", "out2"), "n2")

    def test_connect_nodes9(self):
        """
        Allows multi-attribute edges if each input has at most one source.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1", "in2"])
        dag.add_node(n2)
        dag.connect("n1", ("n2", "in1"))
        dag.connect("n1", ("n2", "in2"))
        self._check(dag.dag)

    def test_connect_nodes10(self):
        """
        Demonstrates idempotency.
        """
        dag = dtfc.DAG()
        n1 = dtfc.Node("n1", outputs=["out1"])
        dag.add_node(n1)
        n2 = dtfc.Node("n2", inputs=["in1"])
        dag.add_node(n2)
        dag.connect("n1", "n2")
        dag.connect("n1", "n2")
        self._check(dag.dag)


# #############################################################################
# dataflow_old.py
# #############################################################################


class Test_dataflow_old_Node1(ut.TestCase):
    def _check(self, n):
        act = n.dag_to_string()
        _LOG.debug("act=%s", act)
        self.check_string(act)

    def test_connect1(self):
        n1 = dtf_old.Node("n1", num_inputs=0)
        n1.connect()
        #
        self._check(n1)

    def test_connect2(self):
        n1 = dtf_old.Node("n1", num_inputs=0)
        #
        self._check(n1)

    def test_connect3(self):
        n1 = dtf_old.Node("n1", num_inputs=0)
        n1.connect()
        n2 = dtf_old.Node("n2", num_inputs=1)
        n2.connect(n1)
        #
        self._check(n2)

    def test_connect4(self):
        n1 = dtf_old.Node("n1", num_inputs=0)
        n1.connect()
        n2 = dtf_old.Node("n2", num_inputs=0)
        n2.connect()
        n3 = dtf_old.Node("n3", num_inputs=2)
        n3.connect(n2, n1)
        #
        self._check(n3)


class Test_dataflow_old_ReadData1(ut.TestCase):
    def _check(self, n):
        act = n.dag_to_string()
        _LOG.debug("act=%s", act)
        self.check_string(act)

    def test_read_data1(self):
        # Create a file.
        df = pd.DataFrame(np.random.rand(10, 3), columns="a b c".split())
        # Build the data flow graph.
        read_data = dtf_old.ReadDataFromDf("read_data", df)
        read_data.connect()
        #
        idxs = list(range(df.shape[0]))
        read_data.set_train_idxs(idxs)
        #
        zscore = dtf_old.Zscore("zscore", style="rolling_std", com=28)
        zscore.connect(read_data)
        #zscore.fit()
        #
        self._check(zscore)


# #############################################################################
# explore.py
# #############################################################################


class Test_explore1(ut.TestCase):
    def test_ols_regress_series(self):
        x = 5 * np.random.randn(100)
        y = x + np.random.randn(*x.shape)
        df = pd.DataFrame()
        df["x"] = x
        df["y"] = y
        exp.ols_regress_series(
            df["x"], df["y"], intercept=True, print_model_stats=False
        )


# #############################################################################
# pandas_helpers.py
# #############################################################################


# TODO(gp): -> Test_pandas_helper1
class TestResampleIndex1(ut.TestCase):
    def test1(self):
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
    def test1(self):
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

    def test2(self):
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

    def test3(self):
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

    def test4(self):
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

    def test5(self):
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
    def get_ex1():
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

    def _test_stabilize_eigenvec_helper(self, data_func, eval_func):
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

    def test_stabilize_eigenvec1(self):
        data_func = self.get_ex1
        eval_func = res.PcaFactorComputer._build_stable_eig_map
        self._test_stabilize_eigenvec_helper(data_func, eval_func)

    def test_stabilize_eigenvec2(self):
        data_func = self.get_ex1
        eval_func = res.PcaFactorComputer._build_stable_eig_map2
        self._test_stabilize_eigenvec_helper(data_func, eval_func)

    # ##########################################################################

    def test_linearize_eigval_eigvec(self):
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

    # ##########################################################################

    def _test_sort_eigval_helper(self, eigval, eigvec, are_eigval_sorted_exp):
        # pylint: disable=W0641
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

    def test_sort_eigval1(self):
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

    def test_sort_eigval2(self):
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
    def _get_data(num_samples, report_stats):
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
        x = norm.rvs(size=(3, num_samples))
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

    def _helper(self, num_samples, report_stats, stabilize_eig, window):
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

    def _check(self, comp, df_res):
        txt = []
        txt.append("comp.get_eigval_names()=\n%s" % comp.get_eigval_names())
        txt.append("df_res.mean()=\n%s" % df_res.mean())
        txt.append("df_res.std()=\n%s" % df_res.std())
        txt = "\n".join(txt)
        self.check_string(txt)

    def test1(self):
        num_samples = 100
        report_stats = False
        stabilize_eig = False
        window = 50
        comp, df_res = self._helper(
            num_samples, report_stats, stabilize_eig, window
        )
        self._check(comp, df_res)

    def test2(self):
        num_samples = 100
        report_stats = False
        stabilize_eig = True
        window = 50
        comp, df_res = self._helper(
            num_samples, report_stats, stabilize_eig, window
        )
        self._check(comp, df_res)


# #############################################################################
# signal_processing.py
# #############################################################################


class TestSignalProcessingRollingZScore1(ut.TestCase):
    def test_default_values1(self):
        heaviside = sigp.get_heaviside(-10, 252, 1, 1)
        zscored = sigp.rolling_zscore(heaviside, tau=40)
        self.check_string(zscored.to_string())

    def test_default_values2(self):
        heaviside = sigp.get_heaviside(-10, 252, 1, 1)
        zscored = sigp.rolling_zscore(heaviside, tau=20)
        self.check_string(zscored.to_string())
