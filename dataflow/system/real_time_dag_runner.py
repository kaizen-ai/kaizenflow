"""
Import as:

import dataflow.system.real_time_dag_runner as dtfsrtdaru
"""

import logging
from typing import Any, Dict, List, Optional

import core.config as cconfig
import core.real_time as creatime
import dataflow.core as dtfcore
import dataflow.system.sink_nodes as dtfsysinod
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


class RealTimeDagRunner(dtfcore.AbstractDagRunner):
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
        config: cconfig.Config,
        dag_builder: dtfcore.DagBuilder,
        fit_state: cconfig.Config,
        execute_rt_loop_kwargs: Dict[str, Any],
        dst_dir: str,
    ) -> None:
        super().__init__(config, dag_builder)
        # Save input parameters.
        # TODO(gp): Use this for stateful DAGs.
        _ = fit_state
        self._execute_rt_loop_kwargs = execute_rt_loop_kwargs
        self._dst_dir = dst_dir
        # Store information about the real-time execution.
        self._events: creatime.Events = []

    async def predict(self) -> List[dtfcore.ResultBundle]:
        """
        Execute the DAG for all the events.

        This adapts the asynchronous generator to a synchronous
        semantic.
        """
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