import logging
from typing import Optional

import sklearn

import core.config as cfg
import core.event_study as esf
import core.signal_processing as sigp
from core.dataflow.builder import DagBuilder
from core.dataflow.core import DAG
from core.dataflow.nodes import (
    ColumnTransformer,
    DataframeMethodRunner,
    Resample,
    SkLearnModel,
    YConnector,
)

_LOG = logging.getLogger(__name__)


class EventStudyBuilder(DagBuilder):
    """
    Configurable pipeline for running event studies.
    """

    @staticmethod
    def get_config_template() -> cfg.Config:
        """
        Return a reference configuration for the event study pipeline.
        """
        config = cfg.Config()
        #
        config_tmp = config.add_subconfig("resample_events")
        config_tmp["rule"] = "T"
        config_tmp["agg_func"] = "mean"
        #
        config_tmp = config.add_subconfig("generate_event_signal")
        config_kwargs = config_tmp.add_subconfig("transformer_kwargs")
        config_kwargs["tau"] = 8
        config_kwargs["max_depth"] = 3
        #
        config_tmp = config.add_subconfig("shift")
        config_kwargs = config_tmp.add_subconfig("method_kwargs")
        config_kwargs["period"] = 1
        #
        config_tmp = config.add_subconfig("build_local_ts")
        config_kwargs = config_tmp.add_subconfig("connector_kwargs")
        config_kwargs["relative_grid_indices"] = range(-10, 50)
        #
        config_tmp = config.add_subconfig("model")
        config_tmp["x_vars"] = ["_DUMMY_"]
        config_tmp["y_vars"] = ["_DUMMY_"]
        config_kwargs = config_tmp.add_subconfig("model_kwargs")
        config_kwargs["alpha"] = 0.5
        return config

    def get_dag(self, config: cfg.Config, dag: Optional[DAG] = None) -> DAG:
        """
        Implement a pipeline for running event studies.

        WARNING: Modifies `dag` in-place.

        :param config: Must be compatible with pipeline construction
            implemented by this function.
        :param dag: May or may not already contain nodes. If `None`, then
            returns a new DAG.
        """
        nids = {}
        dag = dag or DAG()
        _LOG.debug("%s", config)
        # Dummy node for grid data input.
        # - The dataframe with timestamps along a frequency should connect to
        #   this node
        # - It is a no-op node but added so that the grid data connectivity can
        #   be encapsulated
        stage = "grid_data_input_socket"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = ColumnTransformer(
            nid, transformer_func=lambda x: x, col_mode="replace_all"
        )
        dag.add_node(node)
        # Dummy node for events data.
        # - The dataframe containing timestamp-indexed events and any feature
        #   columns should connect to this node
        # - It is a no-op node but added so that the event study connectivity
        #   can be encapsulated
        stage = "events_input_socket"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = ColumnTransformer(
            nid, transformer_func=lambda x: x, col_mode="replace_all"
        )
        dag.add_node(node)
        # Resample events data to uniform grid specified by config.
        # TODO(Paul): Add a check to ensure alignment with grid data.
        stage = "resample_events"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = Resample(nid, **config[stage].to_dict())
        dag.add_node(node)
        dag.connect(nids["events_input_socket"], nid)
        # Drop NaNs from resampled events.
        # - This node is used because resampling places events on a uniform
        #   time grid, and most of these times will not actually represent
        #   events
        # - The output of this node represents the `normalized` event times
        #   and features
        stage = "dropna_from_resampled_events"
        nid = self._get_nid(stage)
        nids[stage] = nid
        # TODO(Paul): Might want to expose "how".
        node = DataframeMethodRunner(
            nid, method="dropna", method_kwargs={"how": "all"}
        )
        dag.add_node(node)
        dag.connect(nids["resample_events"], nid)
        # Reindex events according to grid data.
        # - Effectively a restriction
        # - TODO(Paul): Decide whether we instead want "resample_events" to
        #   directly feed into this node (there may be some corner cases of
        #   interest)
        stage = "reindex_events"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = YConnector(nid, connector_func=esf.reindex_event_features)
        dag.add_node(node)
        dag.connect(
            (nids["dropna_from_resampled_events"], "df_out"), (nid, "df_in1")
        )
        dag.connect((nids["grid_data_input_socket"], "df_out"), (nid, "df_in2"))
        # Fill NaNs with zero (before signal processing).
        # - This node is used because we assume that event data is sparse
        #   compared to the grid data, and because of how NaNs are handled by
        #   `sigp` functions
        stage = "fillna_with_zero"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = ColumnTransformer(
            nid, transformer_func=lambda x: x.fillna(0), col_mode="replace_all"
        )
        dag.add_node(node)
        dag.connect(nids["reindex_events"], nid)
        # Generate event signal.
        # - There is opportunity here for problem-specific customization
        # - Here we use a smooth moving average (which can be made equivalent
        #   to an EMA or made more rectangular-like if desired)
        stage = "generate_event_signal"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = ColumnTransformer(
            nid,
            transformer_func=sigp.compute_smooth_moving_average,
            **config[stage].to_dict(),
            col_mode="replace_all",
        )
        dag.add_node(node)
        dag.connect(nids["fillna_with_zero"], nid)
        # Shift event signal (lag it).
        # - Use a positive integer to introduce a lag (e.g., to reflect
        #   ability to trade)
        stage = "shift"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = DataframeMethodRunner(
            nid, "shift", **config[stage].to_dict()
        )
        dag.add_node(node)
        dag.connect(nid["generate_event_signal"], nid)
        # Merge signal with grid data.
        # - The output of this node adds columns from processed event features
        #   to the columns in the grid data
        stage = "merge_event_signal_with_grid"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = YConnector(
            nid,
            connector_func=lambda x, y, **kwargs: x.merge(y, **kwargs),
            connector_kwargs={
                "how": "right",
                "left_index": True,
                "right_index": True,
            },
        )
        dag.add_node(node)
        dag.connect((nids["shift"], "df_out"), (nid, "df_in1"))
        dag.connect((nids["grid_data_input_socket"], "df_out"), (nid, "df_in2"))
        # Build local time series.
        # - The output of this node is of interest in exploratory work
        stage = "build_local_ts"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = YConnector(
            nid,
            connector_func=esf.build_local_timeseries,
            **config[stage].to_dict(),
        )
        dag.add_node(node)
        dag.connect(
            (nids["dropna_from_resampled_events"], "df_out"), (nid, "df_in1")
        )
        dag.connect(
            (nids["merge_event_signal_with_grid"], "df_out"), (nid, "df_in2")
        )
        # Model.
        # - One may want to use different models in different situations
        # - As a placeholder, we use a regularized linear model
        # - A linear model is useful for performing a Bayesian analysis of any
        #   supposed event effect
        stage = "model"
        nid = self._get_nid(stage)
        nids[stage] = nid
        # TODO(Paul): Alert that this model can be changed.
        node = SkLearnModel(
            nid, model_func=sklearn.linear_model.Ridge, **config[stage].to_dict(),
        )
        dag.add_node(node)
        dag.connect(nids["build_local_ts"], nid)
        # Merge predictions into grid.
        # - The output of this node is the grid data with all event-related
        #   data added as well, e.g., processed event features and model
        #   predictions
        stage = "merge_predictions"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = YConnector(
            nid,
            connector_func=lambda x, y, **kwargs: x.merge(y, **kwargs),
            connector_kwargs={"left_index": True, "right_index": True},
        )
        dag.add_node(node)
        dag.connect((nids["build_local_ts"], "df_out"), (nid, "df_in1"))
        dag.connect((nids["model"], "df_out"), (nid, "df_in2"))
        # TODO(Paul): Add a stage to unwrap causal part of signal only.
        # Unwrap augmented local time series.
        # - The main purpose of this node is to take model predictions
        #   generated from a model run on local time series and place them
        #   back in chronological order
        stage = "unwrap_local_ts"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = YConnector(nid, connector_func=esf.unwrap_local_timeseries)
        dag.add_node(node)
        dag.connect((nids["merge_predictions"], "df_out"), (nid, "df_in1"))
        dag.connect((nids["grid_data_input_socket"], "df_out"), (nid, "df_in2"))
        return dag
