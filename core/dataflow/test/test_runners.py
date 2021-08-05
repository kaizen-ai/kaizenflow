import logging

import numpy as np

import core.dataflow as dtf
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
        dag.run_leq_node("rets/clip", "fit")
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


# # TODO(gp): Test with a very simple DAG since the focus is on the DagRunner.
# class TestRealTimeDagRunner1(hut.TestCase):
#    def test1(self) -> None:
#        """
#        Test the RealTimeDagRunner using synthetic data.
#        """
#        dtf.align_on_even_second()
#        #
#        execute_rt_loop_kwargs = cdtfttrt.get_test_execute_rt_loop_kwargs()
#        kwargs = {
#            "config": config,
#            "dag_builder": dag_builder,
#            "fit_state": None,
#            #
#            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
#            #
#            "dst_dir": None,
#        }
#        dag_runner = cdtf.RealTimeDagRunner(**kwargs)
#        dag_runner.predict()
