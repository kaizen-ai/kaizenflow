import asyncio
import logging
from typing import List, Optional, Tuple

import numpy as np

# TODO(gp): We should import only the strict dependencies.
import core.dataflow as dtf
import core.dataflow.real_time as cdtfrt
import core.dataflow.result_bundle as cdtfrb
import core.dataflow.runners as cdtfr
import core.dataflow.test.test_builders as cdtfnttd
import core.dataflow.test.test_real_time as cdtfttrt
import helpers.hasyncio as hasyncio
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
    def test_simulated_replayed_time1(self) -> None:
        """
        Use simulated replayed time.
        """
        with hasyncio.solipsism_context() as event_loop:
            events, result_bundles = self._helper(event_loop)
        self._check(events, result_bundles)

    def test_replayed_time1(self) -> None:
        """
        Use replayed real-time.
        """
        dtf.align_on_even_second()
        event_loop = None
        events, result_bundles = self._helper(event_loop)
        # It's difficult to check the output of any real-time test.
        _ = events, result_bundles

    @staticmethod
    def _helper(
        event_loop: Optional[asyncio.AbstractEventLoop],
    ) -> Tuple[cdtfrt.Events, List[cdtfrb.ResultBundle]]:
        """
        Test `RealTimeDagRunner` using a simple DAG triggering every 2 seconds.
        """
        # Get a naive pipeline as DAG.
        dag_builder = cdtfnttd._NaivePipeline()
        config = dag_builder.get_config_template()
        # Set up the event loop.
        execute_rt_loop_kwargs = (
            cdtfttrt.get_replayed_time_execute_rt_loop_kwargs(event_loop)
        )
        kwargs = {
            "config": config,
            "dag_builder": dag_builder,
            "fit_state": None,
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            "dst_dir": None,
        }
        # Run.
        dag_runner = cdtfr.RealTimeDagRunner(**kwargs)
        result_bundles = hasyncio.run(dag_runner.predict(), event_loop=event_loop)
        events = dag_runner.events
        #
        _LOG.debug("events=\n%s", events)
        _LOG.debug("result_bundles=\n%s", result_bundles)
        return events, result_bundles

    def _check(
        self, events: cdtfrt.Events, result_bundles: List[cdtfrb.ResultBundle]
    ) -> None:
        # Check the events.
        actual = "\n".join(
            [
                event.to_str(
                    include_tenths_of_secs=False, include_wall_clock_time=False
                )
                for event in events
            ]
        )
        expected = r"""
        num_it=1 current_time='2010-01-04 09:30:00'
        num_it=2 current_time='2010-01-04 09:30:01'
        num_it=3 current_time='2010-01-04 09:30:02'"""
        self.assert_equal(actual, expected, dedent=True)
        # Check the result bundles.
        actual = []
        result_bundles_as_str = "\n".join(map(str, result_bundles))
        actual.append("result_bundles=\n%s" % result_bundles_as_str)
        actual = "\n".join(map(str, actual))
        self.check_string(actual)
