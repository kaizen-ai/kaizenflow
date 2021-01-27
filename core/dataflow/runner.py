import logging
from typing import Any, List, Optional, Tuple

import core.config as cconfi
import helpers.dbg as dbg
from core.dataflow.builder import DagBuilder
from core.dataflow.nodes import extract_info
from core.dataflow.result_bundle import PredictionResultBundle, ResultBundle

_LOG = logging.getLogger(__name__)


class FitPredictDagRunner:
    """
    Class for running DAGs.
    """

    def __init__(self, config: cconfi.Config, dag_builder: DagBuilder) -> None:
        """
        Initialize DAG.

        :param config: config for DAG
        :param dag_builder: `DagBuilder` instance
        """
        self.config = config
        self._dag_builder = dag_builder
        # Create DAG using DAG builder.
        self.dag = self._dag_builder.get_dag(self.config)
        self._methods = self._dag_builder.methods
        self._column_to_tags_mapping = (
            self._dag_builder.get_column_to_tags_mapping(self.config)
        )
        # Confirm that "fit" and "predict" are registered DAG methods.
        dbg.dassert_in("fit", self._methods)
        dbg.dassert_in("predict", self._methods)
        result_nids = self.dag.get_sinks()
        dbg.dassert_eq(len(result_nids), 1)
        self._result_nid = result_nids[0]

    def set_fit_intervals(
        self, intervals: Optional[List[Tuple[Any, Any]]]
    ) -> None:
        """
        Set fit intervals for input nodes.

        :param intervals: as in `DataSource` node, but allowing `None`
        """
        if intervals is None:
            return
        for input_nid in self.dag.get_sources():
            self.dag.get_node(input_nid).set_fit_intervals(intervals)

    def set_predict_intervals(
        self, intervals: Optional[List[Tuple[Any, Any]]]
    ) -> None:
        """
        Set predict intervals for input nodes.

        :param intervals: as in `DataSource` node, but allowing `None`
        """
        if intervals is None:
            return
        for input_nid in self.dag.get_sources():
            self.dag.get_node(input_nid).set_predict_intervals(intervals)

    def fit(self) -> ResultBundle:
        return self._run_dag(self._result_nid, "fit")

    def predict(self) -> ResultBundle:
        return self._run_dag(self._result_nid, "predict")

    def _run_dag(self, nid: str, method: str) -> ResultBundle:
        """
        Run DAG and return a ResultBundle.

        :param nid: identifier of terminal node for execution
        :param method: `Node` subclass method to be executed
        :return: `ResultBundle` class containing `config`, `nid`, `method`,
            result dataframe and DAG info
        """
        dbg.dassert_in(method, self._methods)
        df_out = self.dag.run_leq_node(nid, method)["df_out"]
        info = extract_info(self.dag, [method])
        return ResultBundle(
            config=self.config,
            result_nid=nid,
            method=method,
            result_df=df_out,
            column_to_tags=self._column_to_tags_mapping,
            info=info,
        )


class PredictionDagRunner(FitPredictDagRunner):
    """
    Class for running prediction DAGs.

    Identical to `FitPredictDagRunner`, but returns a
    `PredictionResultBundle`.
    """

    def _run_dag(self, nid: str, method: str) -> PredictionResultBundle:
        """
        Run DAG and return a `PredictionResultBundle`.

        :param nid: identifier of terminal node for execution
        :param method: `Node` subclass method to be executed
        :return: `PredictionResultBundle` class containing `config`, `nid`,
            `method`, result dataframe and DAG info
        """
        dbg.dassert_in(method, self._methods)
        df_out = self.dag.run_leq_node(nid, method)["df_out"]
        info = extract_info(self.dag, [method])
        return PredictionResultBundle(
            config=self.config,
            result_nid=nid,
            method=method,
            result_df=df_out,
            column_to_tags=self._column_to_tags_mapping,
            info=info,
        )
