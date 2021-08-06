import logging

import numpy as np

# TODO(gp): We should import only the strict dependencies.
import core.dataflow as dtf
import core.dataflow.runners as cdtfr
import core.dataflow.test.test_real_time as cdtfttrt
import core.dataflow.nodes.test.test_dag as cdtfnttd
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# #############################################################################


class TestRollingFitPredictDagRunner1(hut.TestCase):
    def test1(self) -> None:
        """
        Test the DagRunner using `ArmaReturnsBuilder`
        """
        dag_builder = dtf.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_builder.get_dag(config)
        #
        dag_runner = dtf.RollingFitPredictDagRunner(
            config=config,
            dag_builder=dag_builder,
            start="2010-01-04 09:30",
            end="2010-01-04 15:30",
            retraining_freq="H",
            retraining_lookback=4,
        )
        result_bundles = list(dag_runner.fit_predict())
        np.testing.assert_equal(len(result_bundles), 2)


# #############################################################################


class TestIncrementalDagRunner1(hut.TestCase):
    def test1(self) -> None:
        """
        Test the DagRunner using `ArmaReturnsBuilder`.
        """
        dag_builder = dtf.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        # Create DAG and generate fit state.
        dag = dag_builder.get_dag(config)
        nid = dag.get_unique_sink()
        dag.run_leq_node(nid, "fit")
        fit_state = dtf.get_fit_state(dag)
        #
        dag_runner = dtf.IncrementalDagRunner(
            config=config,
            dag_builder=dag_builder,
            start="2010-01-04 15:30",
            end="2010-01-04 15:45",
            freq="5T",
            fit_state=fit_state,
        )
        result_bundles = list(dag_runner.predict())
        self.assertEqual(len(result_bundles), 4)
        # Check that dataframe results of `col` do not retroactively change
        # over successive prediction steps (which would suggest future
        # peeking).
        col = "vwap_ret_0_vol_2_hat"
        for rb_i, rb_i_next in zip(result_bundles[:-1], result_bundles[1:]):
            srs_i = rb_i.result_df[col]
            srs_i_next = rb_i_next.result_df[col]
            self.assertTrue(srs_i.compare(srs_i_next[:-1]).empty)


# #############################################################################


class TestRealTimeDagRunner1(hut.TestCase):
    def test1(self) -> None:
        """
        Test the RealTimeDagRunner using a simple DAG triggering every 2 seconds.
        """
        # Get a naive pipeline as DAG.
        dag_builder = cdtfnttd._NaivePipeline()
        config = dag_builder.get_config_template()
        # Set up the event loop.
        execute_rt_loop_kwargs = cdtfttrt.get_test_execute_rt_loop_kwargs()
        kwargs = {
            "config": config,
            "dag_builder": dag_builder,
            "fit_state": None,
            #
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            #
            "dst_dir": None,
        }
        # Run.
        dtf.align_on_even_second()
        dag_runner = cdtfr.RealTimeDagRunner(**kwargs)
        result_bundles = dag_runner.predict()
        # Check the events.
        actual = "\n".join([event.to_str(include_tenths_of_secs=False)
                           for event in dag_runner.events])
        expected = r"""
        num_it=1 current_time=20100104_093000 need_execute=True
        num_it=2 current_time=20100104_093001 need_execute=False
        num_it=3 current_time=20100104_093002 need_execute=True"""
        expected = hprint.dedent(expected)
        self.assert_equal(actual, expected)
        # Check the result bundles.
        actual = []
        events_as_str = str(dag_runner.events)
        actual.append("events=\n%s" % events_as_str)
        result_bundles_as_str = "\n".join(map(str, result_bundles))
        actual.append("result_bundles=\n%s" % result_bundles_as_str)
        actual = "\n".join(map(str, actual))
        self.check_string(actual)

