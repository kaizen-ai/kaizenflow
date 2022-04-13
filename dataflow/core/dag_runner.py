"""
Import as:

import dataflow.core.dag_runner as dtfcodarun
"""

import abc
import logging
from typing import Generator, Optional, Tuple

import pandas as pd

import core.config as cconfig
import dataflow.core.dag as dtfcordag
import dataflow.core.dag_builder as dtfcodabui
import dataflow.core.node as dtfcornode
import dataflow.core.result_bundle as dtfcorebun
import dataflow.core.utils as dtfcorutil
import dataflow.core.visitors as dtfcorvisi
import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


# #############################################################################


# TODO(gp): -> DagRunner
class AbstractDagRunner(abc.ABC):
    """
    Abstract class with the common code to all `DagRunner`s.

    There is not a method common to all `DagRunner`s that is abstract,
    so we use `abc.ABC` to guarantee that this class is not instantiated
    directly.
    """

    def __init__(
        self, config: cconfig.Config, dag_builder: dtfcodabui.DagBuilder
    ) -> None:
        """
        Constructor.

        :param config: config for DAG
        :param dag_builder: `DagBuilder` instance to build a DAG from the config
        """
        # Save input parameters.
        hdbg.dassert_isinstance(config, cconfig.Config)
        self.config = config
        # Build DAG using DAG builder.
        hdbg.dassert_is_not(dag_builder, None)
        # TODO(gp): Now a DagRunner builds and runs a DAG. This creates some
        #  coupling. Consider having a DagRunner accept a DAG however built and run
        #  it.
        if isinstance(dag_builder, dtfcodabui.DagBuilder):
            self._dag_builder = dag_builder
            self.dag = self._dag_builder.get_dag(self.config)
            _LOG.debug("dag=%s", self.dag)
            # Check that the DAG has the required methods.
            methods = self._dag_builder.methods
            _LOG.debug("methods=%s", methods)
            hdbg.dassert_in("fit", methods)
            hdbg.dassert_in("predict", methods)
            # Get the mapping from columns to tags.
            self._column_to_tags_mapping = (
                self._dag_builder.get_column_to_tags_mapping(self.config)
            )
            _LOG.debug("_column_to_tags_mapping=%s", self._column_to_tags_mapping)
        elif isinstance(dag_builder, dtfcordag.DAG):
            self.dag = dag_builder
            # TODO(gp): Not sure what to do here.
            self._column_to_tags_mapping = []
        else:
            raise ValueError("Invalid dag_builder=%s" % dag_builder)
        # Extract the sink node.
        self._result_nid = self.dag.get_unique_sink()
        _LOG.debug("_result_nid=%s", self._result_nid)

    def _set_fit_predict_intervals(
        self, method: dtfcornode.Method, intervals: Optional[dtfcorutil.Intervals]
    ) -> None:
        """
        Set fit or predict intervals for all the source nodes.

        :param intervals: as in `DataSource` node, but allowing `None` to mean no
            interval
        """
        if intervals is None:
            return
        # Propagate the intervals to all source nodes.
        for input_nid in self.dag.get_sources():
            node = self.dag.get_node(input_nid)
            if method == "fit":
                node.set_fit_intervals(intervals)
            elif method == "predict":
                node.set_predict_intervals(intervals)
            else:
                raise ValueError("Invalid method='%s'" % method)

    def _run_dag_helper(
        self, method: dtfcornode.Method
    ) -> Tuple[pd.DataFrame, dtfcorvisi.NodeInfo]:
        """
        Run the DAG for the given method.

        :return: the dataframe for the only output and the associated Info
        """
        nid = self._result_nid
        # TODO(gp): Add a check for `df_out`.
        df_out = self.dag.run_leq_node(nid, method)["df_out"]
        info = dtfcorvisi.extract_info(self.dag, [method])
        return df_out, info

    # TODO(gp): This could be folded into `_run_dag_helper()` if we collapse
    #  `ResultBundle` and `PredictionResultBundle`.
    def _to_result_bundle(
        self,
        method: dtfcornode.Method,
        df_out: pd.DataFrame,
        info: dtfcorvisi.NodeInfo,
    ) -> dtfcorebun.ResultBundle:
        """
        Package the result of a DAG execution into a ResultBundle.
        """
        return dtfcorebun.ResultBundle(
            config=self.config,
            result_nid=self._result_nid,
            method=method,
            result_df=df_out,
            column_to_tags=self._column_to_tags_mapping,
            info=info,
        )


# #############################################################################


class FitPredictDagRunner(AbstractDagRunner):
    """
    Run DAGs that have fit / predict methods.
    """

    def set_fit_intervals(
        self, intervals: Optional[dtfcorutil.Intervals]
    ) -> None:
        """
        Set fit intervals for all the source nodes.

        :param intervals: as in `DataSource` node, but allowing `None`
        """
        method = "fit"
        self._set_fit_predict_intervals(method, intervals)

    def set_predict_intervals(
        self, intervals: Optional[dtfcorutil.Intervals]
    ) -> None:
        """
        Set predict intervals for all the source nodes.

        :param intervals: as in `DataSource` node, but allowing `None`
        """
        method = "predict"
        self._set_fit_predict_intervals(method, intervals)

    def fit(self) -> dtfcorebun.ResultBundle:
        """
        Fitting means running `fit()` method on the DAG up to the sink node.
        """
        method = "fit"
        return self._run_dag(method)

    def predict(self) -> dtfcorebun.ResultBundle:
        """
        Predicting means running `predict()` method on the DAG up to the sink
        node.
        """
        method = "predict"
        return self._run_dag(method)

    def _run_dag(self, method: dtfcornode.Method) -> dtfcorebun.ResultBundle:
        df_out, info = self._run_dag_helper(method)
        return self._to_result_bundle(method, df_out, info)


# #############################################################################


class PredictionDagRunner(FitPredictDagRunner):
    """
    Run prediction DAGs.

    Identical to `FitPredictDagRunner`, but returning a
    `PredictionResultBundle`.
    """

    def _run_dag(
        self, method: dtfcornode.Method
    ) -> dtfcorebun.PredictionResultBundle:
        """
        Same as super class but return a `PredictionResultBundle`.
        """
        df_out, info = self._run_dag_helper(method)
        return dtfcorebun.PredictionResultBundle(
            config=self.config,
            result_nid=self._result_nid,
            method=method,
            result_df=df_out,
            column_to_tags=self._column_to_tags_mapping,
            info=info,
        )


# #############################################################################


class RollingFitPredictDagRunner(AbstractDagRunner):
    """
    Run a DAG by periodic fitting on previous history and evaluating on new
    data.
    """

    def __init__(
        self,
        config: cconfig.Config,
        dag_builder: dtfcodabui.DagBuilder,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        retraining_freq: str,
        retraining_lookback: int,
    ) -> None:
        """
        Constructor.

        :param start_timestamp: start of available data for use in training
        :param end_timestamp: end of available data for use in training
        :param retraining_freq: how often to retrain using Pandas frequency
            convention (e.g., `2B`)
        :param retraining_lookback: number of periods of past data to include
            in retraining, expressed in integral units of `retraining_freq`
        """
        super().__init__(config, dag_builder)
        # Save input parameters.
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._retraining_freq = retraining_freq
        self._retraining_lookback = retraining_lookback
        # Generate retraining dates.
        self._retraining_datetimes = self._generate_retraining_datetimes(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            retraining_freq=self._retraining_freq,
            retraining_lookback=self._retraining_lookback,
        )
        _LOG.info("_retraining_datetimes=%s", self._retraining_datetimes)

    def fit_predict(self) -> Generator:
        """
        Fit at each retraining date and predict until next retraining date.

        :return: the training time, fit `ResultBundle`, predict `ResultBundle`
        """
        for training_datetime in self._retraining_datetimes:
            (
                fit_result_bundle,
                predict_result_bundle,
            ) = self.fit_predict_at_datetime(training_datetime)
            # TODO(gp): Better to return a pd.Timestamp rather than its representation.
            training_datetime_str = training_datetime.strftime("%Y%m%d_%H%M%S")
            yield training_datetime_str, fit_result_bundle, predict_result_bundle

    def fit_predict_at_datetime(
        self,
        datetime_: hdateti.Datetime,
    ) -> Tuple[dtfcorebun.ResultBundle, dtfcorebun.ResultBundle]:
        """
        Fit with all the history up and including `datetime` and then predict
        forward.

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
        # Fit in the interval [start_datetime, datetime_].
        fit_result_bundle = self._run_fit(fit_interval)
        # Determine predict interval.
        idx = idx.shift(freq=self._retraining_freq)
        end_datetime = idx[-1]
        predict_interval = (start_datetime, end_datetime)
        # Predict in the interval [datetime_ + \epsilon, end_datetime].
        predict_result_bundle = self._run_predict(predict_interval, datetime_)
        return fit_result_bundle, predict_result_bundle

    @staticmethod
    def _generate_retraining_datetimes(
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        retraining_freq: str,
        retraining_lookback: int,
    ) -> pd.DatetimeIndex:
        """
        Generate an index of retraining dates based on specs.

        The input parameters have the same meaning as in the constructor.

        :return: (re)training dates
        """
        # Populate an initial index of candidate retraining dates.
        grid = pd.date_range(
            start=start_timestamp, end=end_timestamp, freq=retraining_freq
        )
        # The ability to compute a nonempty `idx` is the first sanity-check.
        hdbg.dassert(
            not grid.empty, msg="Not enough data for requested training schedule!"
        )
        hdbg.dassert_isinstance(retraining_lookback, int)
        # Ensure that `grid` has enough lookback points.
        hdbg.dassert_lt(
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
        idx = idx[idx < end_timestamp]
        #
        hdbg.dassert(not idx.empty)
        return idx

    # TODO(gp): -> _fit for symmetry with the rest of the code.
    def _run_fit(
        self, interval: Tuple[hdateti.Datetime, hdateti.Datetime]
    ) -> dtfcorebun.ResultBundle:
        # Set fit interval on all source nodes of the DAG.
        for input_nid in self.dag.get_sources():
            node = self.dag.get_node(input_nid)
            node.set_fit_intervals([interval])
        # Fit.
        method = "fit"
        df_out, info = self._run_dag_helper(method)
        return self._to_result_bundle(method, df_out, info)

    def _run_predict(
        self,
        # TODO(gp): Use Interval
        interval: Tuple[hdateti.Datetime, hdateti.Datetime],
        oos_start: hdateti.Datetime,
    ) -> dtfcorebun.ResultBundle:
        # Set predict interval on all source nodes of the DAG.
        for input_nid in self.dag.get_sources():
            self.dag.get_node(input_nid).set_predict_intervals([interval])
        # Predict.
        method = "predict"
        df_out, info = self._run_dag_helper(method)
        # Restrict `df_out` to out-of-sample portion.
        df_out = df_out.loc[oos_start:]
        return self._to_result_bundle(method, df_out, info)

    def _run_dag(self, method: dtfcornode.Method) -> dtfcorebun.ResultBundle:
        """
        Run DAG and return a ResultBundle.
        """
        df_out, info = self._run_dag_helper(method)
        return self._to_result_bundle(method, df_out, info)


# #############################################################################


class IncrementalDagRunner(AbstractDagRunner):
    """
    Run DAGs in incremental fashion, i.e., running one step at a time.

    # TODO(gp): Improve description.
    """

    def __init__(
        self,
        config: cconfig.Config,
        dag_builder: dtfcodabui.DagBuilder,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
        freq: str,
        fit_state: cconfig.Config,
    ) -> None:
        """
        Constructor.

        :param config: config for DAG
        :param dag_builder: `DagBuilder` instance
        :param start_timestamp: first prediction datetime_ (e.g., first time at which we
            generate a prediction in `predict` mode, using all available data
            up to and including `start`)
        :param end_timestamp: last prediction datetime_
        :param freq: prediction frequency (typically the same as the frequency
            of the underlying DAG)
        :param fit_state: Config containing any learned state required for
            initializing the DAG
        """
        super().__init__(config, dag_builder)
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._freq = freq
        self._fit_state = fit_state
        dtfcorvisi.set_fit_state(self.dag, self._fit_state)
        # Create predict range.
        self._date_range = pd.date_range(
            start=self._start_timestamp, end=self._end_timestamp, freq=self._freq
        )

    def predict(self) -> Generator:
        """
        Generate a filtration and predict at each index of the filtration.

        Here we use "filtration" as it is used in the context of stochastic
        processes. To ensure that predictions are non-anticipating, we restrict
        the model inputs to times up to and including the prediction time.

        :return: a generator of `ResultBundle`s (one `ResultBundle` for each
            prediction)
        """
        for end_dt in self._date_range:
            result_bundle = self.predict_at_datetime(end_dt)
            yield result_bundle

    # TODO(gp): dt -> timestamp as used elsewhere.
    def predict_at_datetime(
        self, dt: hdateti.Datetime
    ) -> dtfcorebun.ResultBundle:
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
        result_bundle = self._run_dag("predict")
        return result_bundle

    def _run_dag(self, method: dtfcornode.Method) -> dtfcorebun.ResultBundle:
        """
        Run DAG and return a ResultBundle.
        """
        df_out, info = self._run_dag_helper(method)
        return self._to_result_bundle(method, df_out, info)
