"""
Import as:

import core.dataflow.runners as cdtfr import core.dataflow as cdtf
"""
import datetime
import logging
from typing import Any, Dict, Generator, List, Optional, Tuple, Union, cast

import pandas as pd

import core.config as cconfig
import core.dataflow.real_time as cdrt
import helpers.dbg as dbg

# TODO(gp): Use the standard imports.
from core.dataflow.builders import DagBuilder
from core.dataflow.result_bundle import PredictionResultBundle, ResultBundle
from core.dataflow.visitors import extract_info, set_fit_state

_LOG = logging.getLogger(__name__)
_PANDAS_DATE_TYPE = Union[str, pd.Timestamp, datetime.datetime]


# TODO(gp): Why does DagRunner also builds a DAG through the DagBuilder?
#  This creates some necessary coupling. A DagRunner should accpets a DAG however
#  built and run it.


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


class RollingFitPredictDagRunner:
    """
    Class for periodic re-fitting of models.
    """

    def __init__(
        self,
        config: cconfig.Config,
        dag_builder: DagBuilder,
        start: _PANDAS_DATE_TYPE,
        end: _PANDAS_DATE_TYPE,
        retraining_freq: str,
        retraining_lookback: int,
    ) -> None:
        # Save input parameters.
        self.config = config
        self._dag_builder = dag_builder
        self._start = start
        self._end = end
        self._retraining_freq = retraining_freq
        self._retraining_lookback = retraining_lookback
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
        dbg.dassert_in("fit", self._methods)
        dbg.dassert_in("predict", self._methods)
        # Save the sink node.
        result_nids = self.dag.get_sinks()
        dbg.dassert_eq(len(result_nids), 1)
        self._result_nid = result_nids[0]
        _LOG.info("_result_nid=%s", self._result_nid)
        # Generate retraining dates.
        self._retraining_datetimes = self._generate_retraining_datetimes(
            start=self._start,
            end=self._end,
            retraining_freq=self._retraining_freq,
            retraining_lookback=self._retraining_lookback,
        )
        _LOG.info("_retraining_datetimes=%s", self._retraining_datetimes)

    def fit_predict(self) -> Generator:
        """
        Fit at each retraining date and predict until next retraining date.
        """
        for training_datetime in self._retraining_datetimes:
            (
                fit_result_bundle,
                predict_result_bundle,
            ) = self.fit_predict_at_datetime(training_datetime)
            training_datetime_str = training_datetime.strftime("%Y%m%d_%H%M%S")
            yield training_datetime_str, fit_result_bundle, predict_result_bundle

    def fit_predict_at_datetime(
        self,
        datetime_: _PANDAS_DATE_TYPE,
    ) -> Tuple[ResultBundle, ResultBundle]:
        """
        Fit at `datetime` and then predict.

        :param datetime_: point in time at which to train (historically) and then
            predict (one step ahead)
        :return: populated fit and predict `ResultBundle`s
        """
        # Determine fit interval.
        idx = pd.date_range(
            end=datetime_,
            freq=self._retraining_freq,
            periods=self._retraining_lookback,
        )
        start_datetime = idx[0]
        fit_interval = (start_datetime, datetime_)
        fit_result_bundle = self._run_fit(fit_interval)
        # Determine predict interval.
        idx = idx.shift(freq=self._retraining_freq)
        end_datetime = idx[-1]
        predict_interval = (start_datetime, end_datetime)
        predict_result_bundle = self._run_predict(predict_interval, datetime_)
        return fit_result_bundle, predict_result_bundle

    def _run_fit(
        self, interval: Tuple[_PANDAS_DATE_TYPE, _PANDAS_DATE_TYPE]
    ) -> ResultBundle:
        # Set fit interval on DAG.
        for input_nid in self.dag.get_sources():
            self.dag.get_node(input_nid).set_fit_intervals([interval])
        # Fit.
        method = "fit"
        df_out = self.dag.run_leq_node(self._result_nid, method)["df_out"]
        info = extract_info(self.dag, [method])
        return ResultBundle(
            config=self.config,
            result_nid=self._result_nid,
            method=method,
            result_df=df_out,
            column_to_tags=self._column_to_tags_mapping,
            info=info,
        )

    def _run_predict(
        self,
        interval: Tuple[_PANDAS_DATE_TYPE, _PANDAS_DATE_TYPE],
        oos_start: _PANDAS_DATE_TYPE,
    ) -> ResultBundle:
        # Set predict interval on DAG.
        for input_nid in self.dag.get_sources():
            self.dag.get_node(input_nid).set_predict_intervals([interval])
        # Predict.
        method = "predict"
        df_out = self.dag.run_leq_node(self._result_nid, method)["df_out"]
        # Restrict `df_out` to out-of-sample portion.
        df_out = df_out.loc[oos_start:]  # type: ignore[misc]
        info = extract_info(self.dag, [method])
        return ResultBundle(
            config=self.config,
            result_nid=self._result_nid,
            method=method,
            result_df=df_out,
            column_to_tags=self._column_to_tags_mapping,
            info=info,
        )

    @staticmethod
    def _generate_retraining_datetimes(
        start: _PANDAS_DATE_TYPE,
        end: _PANDAS_DATE_TYPE,
        retraining_freq: str,
        retraining_lookback: int,
    ) -> pd.DatetimeIndex:
        """
        Generate an index of retraining dates based on specs.

        :param start: start of available data for use in training
        :param end: end of available data for use in training
        :param retraining_freq: how often to retrain
        :param retraining_lookback: number of periods of past data to include
            in retraining, expressed in integral units of `retraining_freq`
        :return: (re)training dates
        """
        # Populate an initial index of candidate retraining dates.
        grid = pd.date_range(start=start, end=end, freq=retraining_freq)
        # The ability to compute a nonempty `idx` is the first sanity-check.
        dbg.dassert(
            not grid.empty, msg="Not enough data for requested training schedule!"
        )
        dbg.dassert_isinstance(retraining_lookback, int)
        # Ensure that `grid` has enough lookback points.
        dbg.dassert_lt(
            retraining_lookback,
            grid.size,
            msg="Input data does not have %i periods" % retraining_lookback,
        )
        # Shift the start of the index by the lookback amount. The new start
        # represents the first data point that will be used in (the first)
        # training.
        lookback = str(retraining_lookback) + retraining_freq
        idx = grid.shift(freq=lookback)
        # Trim end of date range back to `end`. This is needed because the
        # previous `shift()` also moves the end of the index.
        # TODO(Paul): Add back `end` for a fit-only step.
        idx = idx[idx < end]
        #
        dbg.dassert(not idx.empty)
        return idx

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
        :param start: first prediction datetime_ (e.g., first time at which we
            generate a prediction in `predict` mode, using all available data
            up to and including `start`)
        :param end: last prediction datetime_
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

    def predict_at_datetime(self, dt: _PANDAS_DATE_TYPE) -> ResultBundle:
        """
        Generate a prediction as of `dt` (for a future point in time).

        :param dt: point in time at which to generate a prediction
        :return: populated `ResultBundle`
        """
        # Cut off data at `end_dt`. Do not restrict the start datetime_ so
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


class RealTimeDagRunner:
    """
    Class for running a DAG in real-time.
    """

    def __init__(
        self,
        config: cconfig.Config,
        dag_builder: DagBuilder,
        fit_state: cconfig.Config,
        #
        execute_rt_loop_kwargs: Dict[str, Any],
        #
        dst_dir: str,
    ) -> None:
        # Save input parameters.
        self._config = config
        self._dag_builder = dag_builder
        # TODO(gp): Use this for stateful DAGs.
        _ = fit_state
        # Create DAG using DAG builder.
        self._dag = self._dag_builder.get_dag(self._config)
        #
        self._execute_rt_loop_kwargs = execute_rt_loop_kwargs
        #
        self._dst_dir = dst_dir
        #
        self._execution_trace: Optional[cdrt.ExecutionTrace] = None

    # TODO(gp): Should it return a List[ResultBundle]?
    def predict(self) -> List[Dict[str, Any]]:
        # TODO(gp): This is similar to an IncrementalDagRunner.
        def dag_workload(current_time: pd.Timestamp) -> Dict[str, Any]:
            """
            Workload for the real-time loop to execute a DAG.
            """
            _ = current_time
            sink = self._dag.get_unique_sink()
            dict_ = self._dag.run_leq_node(sink, "predict")
            dict_ = cast(Dict[str, Any], dict_)
            return dict_

        execution_trace, results = cdrt.execute_with_real_time_loop(
            **self._execute_rt_loop_kwargs, workload=dag_workload
        )
        self._execution_trace = execution_trace
        results = cast(List[Dict[str, Any]], results)
        return results

    @property
    def get_execution_trace(self) -> Optional[cdrt.ExecutionTrace]:
        return self._execution_trace
