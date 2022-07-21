"""
Import as:

import dataflow.system.real_time_dag_runner as dtfsrtdaru
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import core.config as cconfig
import core.real_time as creatime
import dataflow.core as dtfcore
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import helpers.hasyncio as hasynci
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# TODO(Paul): Consider renaming `EventLoopDagRunner`.
class RealTimeDagRunner(dtfcore.DagRunner):
    """
    Run a DAG in true or simulated real-time.

    A DAG can be executed in real-time if one of the source nodes has a notion of
    time and emits different data for the same invocation because time has elapsed.

    See:
    - `real_time.py` for definitions of different types of real-time executions
    - `_AbstractRealTimeDataSource` and descendants for nodes with real-time semantic
    """

    def __init__(
        self,
        dag: dtfcore.DAG,
        fit_state: cconfig.Config,
        execute_rt_loop_kwargs: Dict[str, Any],
        dst_dir: str,
        #
        *,
        get_wall_clock_time: Optional[hdateti.GetWallClockTime] = None,
        wake_up_timestamp: Optional[pd.Timestamp] = None,
        grid_time_in_secs: Optional[int] = None,
    ) -> None:
        """

        :param get_wall_clock_time: wall clock to use to
        :param wake_up_timestamp: timestamp to wait to start the execution
        :param grid_time_in_secs:
        """
        super().__init__(dag)
        # Save input parameters.
        # TODO(gp): Use this for stateful DAGs.
        _ = fit_state
        self._get_wall_clock_time = get_wall_clock_time
        self._wake_up_timestamp = wake_up_timestamp
        self._grid_time_in_secs = grid_time_in_secs
        self._execute_rt_loop_kwargs = execute_rt_loop_kwargs
        self._dst_dir = dst_dir
        # Store information about the real-time execution.
        self._events: creatime.Events = []

    async def wait_for_start_trading(self) -> None:
        """
        Wait until `wake_up_timestamp` in config.

        E.g., 9:45am.
        """
        get_wall_clock_time = self._get_wall_clock_time
        # The system should come up sometime before the first bar (e.g., around
        # 9:37am ET) and then we align to the next trading bar.
        curr_timestamp = get_wall_clock_time()
        _LOG.info("Current time=%s", curr_timestamp)
        wake_up_timestamp = self._wake_up_timestamp
        _LOG.info("Waiting until session start at %s ...", wake_up_timestamp)
        await hasynci.async_wait_until(wake_up_timestamp, get_wall_clock_time)
        # If the system comes up in the middle of the day then we need to wait to
        # align to a bar.
        # Align on the trading grid (e.g., 1, 5, 15 minutes).
        grid_time_in_secs = self._grid_time_in_secs
        hdbg.dassert_lte(1, grid_time_in_secs)
        _LOG.info("Aligning on a bar lasting %s secs ...", grid_time_in_secs)
        # Add one second to make sure we are after the start trading time.
        add_buffer_in_secs = 1
        target_time, secs_to_wait = hasynci.get_seconds_to_align_to_grid(
            grid_time_in_secs,
            get_wall_clock_time,
            add_buffer_in_secs=add_buffer_in_secs,
        )
        await hasynci.async_wait_until(target_time, get_wall_clock_time)
        _LOG.debug("Aligning ... done")

    async def predict(self) -> List[dtfcore.ResultBundle]:
        """
        Execute the DAG for all the events.

        This adapts the asynchronous generator to a synchronous
        semantic.
        """
        if self.dag._nx_dag.has_node("predict"):
            method = "fit"
            await self._run_dag(method)
            _LOG.info("dag after fit=\n%s", str(self.dag))
            self.dag.get_node("predict").get_fit_state()
        # Align on the bar.
        if self._wake_up_timestamp is not None:
            # TODO(gp): Add a check to make sure that all the params
            # are set up consistently.
            await self.wait_for_start_trading()
        # Start loop.
        result_bundles = [
            result_bundle async for result_bundle in self.predict_at_datetime()
        ]
        return result_bundles

    async def predict_at_datetime(self) -> dtfcore.ResultBundle:
        """
        Predict every time there is a real-time event.

        :return: list of `ResultBundle`, one per event
        """
        method = "predict"
        # Adapt `_dag_workload()` to the expected call back signature.
        workload = lambda current_time: self._run_dag(method)
        # Call the event loop.
        async for event, result_bundle in creatime.execute_with_real_time_loop(
            **self._execute_rt_loop_kwargs, workload=workload
        ):
            self._events.append(event)
            yield result_bundle

    @property
    def events(self) -> Optional[creatime.Events]:
        return self._events

    def compute_run_signature(
        self, result_bundles: List[dtfcore.ResultBundle]
    ) -> str:
        """
        Compute a signature of an execution in terms of `ResultBundles` and
        `events`.
        """
        ret = []
        events = self.events
        hdbg.dassert_eq(len(events), len(result_bundles))
        for event, result_bundle in zip(events, result_bundles):
            event_as_str = event.to_str(
                include_tenths_of_secs=False, include_wall_clock_time=False
            )
            ret.append(hprint.frame(event_as_str))
            ret.append("result_bundle=\n%s" % str(result_bundle))
        ret = "\n".join(ret)
        return ret

    async def _run_dag(self, method: dtfcore.Method) -> dtfcore.ResultBundle:
        # Wait until all the real-time source nodes are ready to compute.
        _LOG.debug("Waiting for real-time nodes to be ready ...")
        sources = self.dag.get_sources()
        for nid in sources:
            node = self.dag.get_node(nid)
            _LOG.debug("nid=%s node=%s type=%s", nid, str(node), str(type(node)))
            if isinstance(node, dtfsysonod.HistoricalDataSource):
                raise ValueError(
                    f"HistoricalDataSource node {node} not allowed "
                    "in RealTimeDagRunner"
                )
            if isinstance(node, dtfsysonod.RealTimeDataSource):
                _LOG.debug("Waiting on node '%s' ...", str(nid))
                await node.wait_for_latest_data()
                _LOG.debug("Waiting on node '%s': done", str(nid))
        _LOG.debug("Waiting for real-time nodes to be ready: done")
        # Execute the DAG.
        df_out, info = self._run_dag_helper(method)
        # Wait for the sinks to complete.
        # TODO(gp): Find ProcessForecast. We can also create an abstract class
        #  AwaitableNode with a `wait()` method and then wait on all the
        #  sources and sinks that are awaitable.
        nid = self.dag.get_unique_sink()
        node = self.dag.get_node(nid)
        if isinstance(node, dtfsysinod.ProcessForecasts):
            _LOG.debug("Waiting on node '%s' ...", str(nid))
            await node.process_forecasts()
            _LOG.debug("Waiting on node '%s': done", str(nid))
        #
        return self._to_result_bundle(method, df_out, info)
