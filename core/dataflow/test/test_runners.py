import asyncio
import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pytest

# TODO(gp): We should import only the strict dependencies.
import core.config as cconfig
import core.dataflow as dtf
import core.dataflow.real_time as cdtfretim
import core.dataflow.result_bundle as cdtfrebun
import core.dataflow.runners as cdtfrun
import core.dataflow.test.test_builders as cdtfnttd
import core.dataflow.test.test_db_interface as dartttdi
import core.dataflow.test.test_real_time as cdtfttrt
import dataflow_amp.real_time.pipeline as dtfamretipip
import helpers.hasyncio as hhasynci
import helpers.unit_test as huntes

_LOG = logging.getLogger(__name__)


# #############################################################################


class TestRollingFitPredictDagRunner1(huntes.TestCase):
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


class TestIncrementalDagRunner1(huntes.TestCase):
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


class TestRealTimeDagRunner1(huntes.TestCase):
    """
    - Create a naive DAG pipeline with a node generating random data and
      processing the data through a pass-through node
    - Create an event loop replaying time
    - Run the DAG with a `RealTimeDagRunner`
    - Check that the output is what is expected

    We simulate this in real and simulated time.
    """

    def test_simulated_replayed_time1(self) -> None:
        """
        Use simulated replayed time.
        """
        with hhasynci.solipsism_context() as event_loop:
            events, result_bundles = self._helper(event_loop)
        self._check(events, result_bundles)

    # TODO(gp): Enable this but make it trigger more often.
    @pytest.mark.skip(reason="Too slow for real time")
    def test_replayed_time1(self) -> None:
        """
        Use replayed time.
        """
        dtf.align_on_even_second()
        event_loop = None
        events, result_bundles = self._helper(event_loop)
        # It's difficult to check the output of any real-time test, so we don't
        # verify the output.
        _ = events, result_bundles

    @staticmethod
    def _helper(
        event_loop: Optional[asyncio.AbstractEventLoop],
    ) -> Tuple[cdtfretim.Events, List[cdtfrebun.ResultBundle]]:
        """
        Test `RealTimeDagRunner` using a simple DAG triggering every 2 seconds.
        """
        # Get a naive pipeline as DAG.
        dag_builder = cdtfnttd._NaivePipeline()
        config = dag_builder.get_config_template()
        # Set up the event loop.
        sleep_interval_in_secs = 1.0
        execute_rt_loop_kwargs = (
            cdtfttrt.get_replayed_time_execute_rt_loop_kwargs(
                sleep_interval_in_secs, event_loop=event_loop
            )
        )
        dag_runner_kwargs = {
            "config": config,
            "dag_builder": dag_builder,
            "fit_state": None,
            "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
            "dst_dir": None,
        }
        # Run.
        dag_runner = cdtfrun.RealTimeDagRunner(**dag_runner_kwargs)
        result_bundles = hhasynci.run(dag_runner.predict(), event_loop=event_loop)
        events = dag_runner.events
        #
        _LOG.debug("events=\n%s", events)
        _LOG.debug("result_bundles=\n%s", result_bundles)
        return events, result_bundles

    # TODO(gp): Centralize this.
    def _check(
        self,
        events: cdtfretim.Events,
        result_bundles: List[cdtfrebun.ResultBundle],
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
