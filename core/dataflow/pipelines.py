import logging

import core.config as cfg
import core.event_study as esf
import core.signal_processing as sigp
import helpers.dbg as dbg

from core.dataflow.builder import DagBuilder

_LOG = logging.getLogger(__name__)


class EventStudyBuilder(DagBuilder):
    """

    """

    @staticmethod
    def get_config_template() -> cfg.Config:
        """

        :return:
        """
        config = cfg.Config()
        return config

    def get_dag(self, config: cfg.Config, dag: Optional[dtf.DAG] = None) -> dtf.DAG:
        """

        :param config:
        :param dag:
        :return:
        """
        nids = {}
        dag = dag or dtf.DAG()
        _LOG.debug("%s", config)
        # Dummy node for grid data input.
        stage = "grid_data_input_socket"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.ColumnTransformer(nid
                                     transformer_func=lambda x: x,
                                     col_mode="replace_all")
        dag.add_node(node)
        # Dummy node for events data.
        stage = "events_input_socket"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.ColumnTransformer(nid
                             transformer_func=lambda x: x,
                             col_mode="replace_all")
        dag.add_node(node)
        # Resample events data to uniform grid.
        stage = "resample_events"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.Resample(nid,
                            rule="T", agg_func="mean")
        dag.add_node(node)
        dag.connect(nids["events_input_socket"], nid)
        # Drop NaNs from resampled events.
        stage = "dropna_from_resampled_events"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.DataframeMethodRunner("events/dropna",
                              method="dropna",
                              method_kwargs={"how": "all"})
        dag.add_node(node)
        dag.connect(nids["resample_events"], nid)
        # Reindex events according to grid data.
        stage = "reindex_events"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.YConnector(nid,
                      connector_func=esf.reindex_event_features)
        dag.add_node(node)
        dag.connect((nids["dropna_from_resampled_events"], "df_out"),
                    (nid, "df_in1"))
        dag.connect((nids["grid_data_input_socket"], "df_out"),
                    (nid, "df_in2"))
        # Fill NaNs with zero (before signal processing).
        stage = "fillna_with_zero"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.ColumnTransformer(nid,
                             transformer_func=lambda x: x.fillna(0),
                             col_mode="replace_all")
        dag.add_node(node)
        dag.connect(nids["reindex_events"], nid)
        # Generate event signal.
        stage = "generate_event_signal"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.ColumnTransformer(nid,
                             transformer_func=sigp.compute_smooth_moving_average,
                             transformer_kwargs={"tau": 8, "max_depth": 3},
                             col_mode="replace_all")
        dag.add_node(node)
        dag.connect(nids["fillna_with_zero"], nid)
        # Merge signal with grid data.
        stage = "merge_event_signal_with_grid"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.YConnector(nid,
                      connector_func=lambda x, y, **kwargs: x.merge(y, **kwargs),
                      connector_kwargs={"how": "right", "left_index": True, "right_index": True})
        dag.add_node(node)
        dag.connect((nids["generate_event_signal"], "df_out"),
                    (nid, "df_in1"))
        dag.connect((nids["grid_data_input_socket"], "df_out"),
                    (nid, "df_in2"))
        # Build local time series.
        stage = "build_local_ts"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.YConnector(nid
                      connector_func=esf.build_local_timeseries,
                      connector_kwargs={"relative_grid_indices": range(-10, 50)})
        dag.add_node(node)
        dag.connect((nids["dropna_from_resampled_events"], "df_out"),
                    (nid, "df_in1"))
        dag.connect((nids["merge_event_signal_with_grid"], "df_out"),
                    (nid, "df_in2"))
        # Model.
        stage = "model"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.SkLearnModel(nid,
            model_func=sklearn.linear_model.Ridge,
            model_kwargs={"alpha": 0.1},
            x_vars=["ind", "demand+", "demand-", "inventory+", "inventory-", "supply+"],
            y_vars=["zret_0"],
        )
        dag.add_node(node)
        dag.connect(nids["build_local_ts"], nid)
        # Merge predictions into grid.
        stage = "merge_predictions"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.YConnector("merge_prediction",
                      connector_func = lambda x, y, **kwargs: x.merge(y, **kwargs),
                      connector_kwargs = {"left_index": True, "right_index": True})
        dag.add_node(node)
        dag.connect((nids["build_local_ts"], "df_out"),
                    (nid, "df_in1"))
        dag.connect((nids["model"], "df_out"),
                    (nid, "df_in2"))
        # Unwrap augmented local time series.
        stage = "unwrap_local_ts"
        nid = self._get_nid(stage)
        nids[stage] = nid
        node = dtf.YConnector(nid,
                      connector_func = esf.unwrap_local_timeseries)
        dag.add_node(node)
        dag.connect((nids["merge_predictions"], "df_out"),
                    (nid, "df_in1"))
        dag.connect((nids["grid_data_input_socket"], "df_out"),
                    (nid, "df_in2"))
        return dag