import asyncio
import logging
from typing import List, Optional, Tuple

import pytest

import core.config as cconfig
import core.real_time as creatime
import core.real_time_example as cretiexa
import dataflow.core as dtfcore
import dataflow.system.real_time_dag_runner as dtfsrtdaru
import helpers.hdatetime as hdateti
import helpers.hasyncio as hasynci
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


class TestRealTimeDagRunner1(hunitest.TestCase):
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
        with hasynci.solipsism_context() as event_loop:
            events, result_bundles = self._helper(event_loop)
        self._check(events, result_bundles)

    # TODO(gp): Enable this but make it trigger more often.
    @pytest.mark.skip(reason="Too slow for real time")
    def test_replayed_time1(self) -> None:
        """
        Use replayed time.
        """
        event_loop = None
        events, result_bundles = self._helper(event_loop)
        # It's difficult to check the output of any real-time test, so we don't
        # verify the output.
        _ = events, result_bundles

    @staticmethod
    def _helper(
        event_loop: Optional[asyncio.AbstractEventLoop],
    ) -> Tuple[creatime.Events, List[dtfcore.ResultBundle]]:
        """
        Test `RealTimeDagRunner` using a simple DAG triggering every 2 seconds.
        """
        # Get a naive pipeline as DAG.
        dag_builder = dtfcore.MvnReturnsBuilder()
        #
        overriding_config = cconfig.Config()
        overriding_config["load_prices"] = {
            "frequency": "T",
            "start_date": "2010-01-04 09:30:00",
            "end_date": "2010-01-14 16:05:00",
            "dim": 4,
            "target_volatility": 0.25,
            "seed": 247,
        }
        node = dtfcore.MultivariateNormalGenerator
        nodes_to_insert = [("load_prices", node)]
        dag_builder = dtfcore.DagAdapter(
            dag_builder,
            overriding_config,
            nodes_to_insert,
            [],
        )
        #
        config = dag_builder.get_config_template()
        # Set up the event loop.
        sleep_interval_in_secs = 1.0
        execute_rt_loop_kwargs = (
            cretiexa.get_replayed_time_execute_rt_loop_kwargs(
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
        # Align on a second boundary.
        get_wall_clock_time = lambda: hdateti.get_current_time(
            tz="ET", event_loop=event_loop
        )
        grid_time_in_secs = 1
        creatime.align_on_time_grid(
            get_wall_clock_time, grid_time_in_secs, event_loop=event_loop
        )
        # Run.
        dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
        result_bundles = hasynci.run(dag_runner.predict(), event_loop=event_loop)
        events = dag_runner.events
        #
        _LOG.debug("events=\n%s", events)
        _LOG.debug("result_bundles=\n%s", result_bundles)
        return events, result_bundles

    # TODO(gp): Centralize this.
    def _check(
        self,
        events: creatime.Events,
        result_bundles: List[dtfcore.ResultBundle],
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
