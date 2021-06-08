import datetime
import logging
from typing import Any, Generator, List, Optional, Tuple, Union

import pandas as pd

import core.config as cconfig
import helpers.dbg as dbg
from core.dataflow.builders import DagBuilder
from core.dataflow.result_bundle import PredictionResultBundle, ResultBundle
from core.dataflow.visitors import extract_info, set_fit_state

_LOG = logging.getLogger(__name__)
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


class FitPredictDagRunner:
    """
    Class for running DAGs.
    """

    def __init__(self, config: cconfig.Config, dag_builder: DagBuilder) -> None:
        """

        :param config: config for DAG
        :param dag_builder: `DagBuilder` instance
        """
        # Save input parameters.
        self.config = config
        self._dag_builder = dag_builder
        # Create DAG using DAG builder.
        self.dag = self._dag_builder.get_dag(self.config)
        _LOG.info("dag=%s", self.dag)
        self._methods = self._dag_builder.methods
        _LOG.info("_methods=%s", self._methods)
        self._column_to_tags_mapping = (
            self._dag_builder.get_column_to_tags_mapping(self.config)
        )
        _LOG.info("_column_to_tags_mapping=%s", self._column_to_tags_mapping)
        # Confirm that "fit" and "predict" are registered DAG methods.
        # TODO(gp): Factor this out.
        dbg.dassert_in("fit", self._methods)
        dbg.dassert_in("predict", self._methods)
        # Save the sink node.
        # TODO(gp): Factor this out.
        result_nids = self.dag.get_sinks()
        dbg.dassert_eq(len(result_nids), 1)
        self._result_nid = result_nids[0]
        _LOG.info("_result_nid=%s", self._result_nid)

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
        # TODO(gp): Factor out this in _run_dag_helper().
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
        Same as super class but return a `PredictionResultBundle`.
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


class IncrementalDagRunner:
    """
    Class for running DAGs.
    """

    def __init__(
        self,
        config: cconfig.Config,
        dag_builder: DagBuilder,
        start: _PANDAS_DATE_TYPE,
        end: _PANDAS_DATE_TYPE,
        freq: str,
        # result_dir: str,
        fit_state: cconfig.Config,
    ) -> None:
        """
        Initialize DAG.

        :param config: config for DAG
        :param dag_builder: `DagBuilder` instance
        :param start: first prediction datetime (e.g., first time at which we
            generate a prediction in `predict` mode, using all available data
            up to and including `start`)
        :param end: last prediction datetime
        :param freq: prediction frequency (typically the same as the frequency
            of the underlying DAG)
        :param fit_state: Config containing any learned state required for
            initializing the DAG
        """
        self.config = config
        self._dag_builder = dag_builder
        self._start = start
        self._end = end
        self._freq = freq
        # self._result_dir = result_dir
        self._fit_state = fit_state
        # Create DAG using DAG builder.
        self.dag = self._dag_builder.get_dag(self.config)
        #
        set_fit_state(self.dag, self._fit_state)
        #
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
        # Create predict range
        self._date_range = pd.date_range(
            start=self._start, end=self._end, freq=self._freq
        )

    def predict(self) -> Generator:
        """
        Generate a filtration and predict at each index of the filtration.

        Here we use "filtration" as it is used in the context of stochastic
        processes. To ensure that predictions are non-anticipating, we restrict
        the model inputs to times up to and including the prediction time.

        :return: a generator of result bundles (one result bundle for each
            prediction)
        """
        for end_dt in self._date_range:
            result_bundle = self.predict_at_datetime(end_dt)
            yield result_bundle

    def predict_at_datetime(self, dt) -> ResultBundle:
        """
        Generate a prediction as of `dt` (for a future point in time).

        :param dt: point in time at which to generate a prediction
        :return: populated `ResultBundle`
        """
        # Cut off data at `end_dt`. Do not restrict the start datetime so
        # so as not to adversely affect any required warm-up period.
        interval = [(None, dt)]
        # Set prediction intervals and predict.
        for input_nid in self.dag.get_sources():
            self.dag.get_node(input_nid).set_predict_intervals(interval)
        result_bundle = self._run_dag(self._result_nid, "predict")
        return result_bundle

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
