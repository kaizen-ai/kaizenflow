import abc
import logging

import core.config as cfg
import helpers.dbg as dbg
import helpers.introspection as intr
from core.dataflow.builder import DagBuilder
from core.dataflow.nodes import extract_info
from core.dataflow.result_bundle import PredictionResultBundle, ResultBundle

_LOG = logging.getLogger(__name__)


class DagRunner(abc.ABC):
    """
    Class for running DAGs from a meta-config.

    TODO(*): Handle set fit/predict intervals.
    """

    def __init__(self, config: cfg.Config, dag_builder: DagBuilder) -> None:
        """
        Initialize DAG.

        :param config: meta config containing a "DAG" key
        :param dag_builder: `DagBuilder` instance. If it has `validate_config`
            method, validate config before building a DAG
        """
        self.config = config
        self._dag_builder = dag_builder
        # Use DAG builder to validate DAG config.
        if "validate_config" in intr.get_methods(self._dag_builder):
            self._dag_builder.validate_config(self.config["DAG"])
        # Create DAG using DAG builder.
        self.dag = self._dag_builder.get_dag(self.config["DAG"])
        self._input_nids = self._dag_builder.input_nids
        self._result_nids = self._dag_builder.result_nids
        self._methods = self._dag_builder.methods

    @abc.abstractmethod
    def predict(self) -> ResultBundle:
        pass

    def _run_dag(self, nid: str, method: str) -> ResultBundle:
        """
        Run DAG.

        :param nid: identifier of terminal node for execution
        :param method: `Node` subclass method to be executed
        :return: `ResultBundle` class containing `config`, `nid`, `method`,
            result dataframe and DAG info
        """
        df_out = self.dag.run_leq_node(nid, method)["df_out"]
        info = extract_info(self.dag, [method])
        return ResultBundle(
            config=self.config,
            result_nid=nid,
            method=method,
            result_df=df_out,
            column_to_tags=None,
            info=info,
        )

