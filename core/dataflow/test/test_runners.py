import logging

import core.dataflow as dtf
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestRollingFitPredictDagRunner(hut.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtf.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_builder.get_dag(config)
        #
        dag_runner = dtf.RollingFitPredictDagRunner(
            config=config,
            dag_builder=dag_builder,
            start="2010-01-04 09:30",
            end="2010-01-04 15:30",
            retraining_frequency="H",
            training_period_lookback=4,
        )
        result_bundle_pairs = list(dag_runner.fit_predict())
        # TODO(Paul): Use a proper testing function.
        self.assertTrue(len(result_bundle_pairs) == 2)


class TestIncrementalDagRunner(hut.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
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
