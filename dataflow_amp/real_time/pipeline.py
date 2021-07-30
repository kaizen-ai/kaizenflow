"""
Import as:

import dataflow_amp.real_time.real_time_return_pipeline as dtfart
"""

import datetime
import logging
from typing import Optional

import pandas as pd

import core.config as cconfig
import core.dataflow as dtf
import core.dataflow_source_nodes as dsn
import core.finance as fin
import helpers.dbg as dbg
import dataflow_amp.returns.pipeline as darp

_LOG = logging.getLogger(__name__)


class RealTimeReturnPipeline(dtf.DagBuilder):
    """
    Real-time pipeline for computing returns from price data.
    """

    def __init__(self):
        self._dag_builder = darp.ReturnsPipeline()

    def get_config_template(self) -> cconfig.Config:
        """
        Return a template configuration for this pipeline.

        :return: reference config
        """
        # Get the DAG builder and the config template.
        config = self._dag_builder.get_config_template()
        #
        start_datetime = pd.Timestamp("2010-01-04 09:30:00")
        end_datetime = pd.Timestamp("2010-01-04 11:30:00")
        # Use a replayed real-time starting at the same time as the data.
        # TODO(gp): This should be moved to the `get_dag()` part. The config_template
        #  contains all the fields that need to be filled, then the rest builds it.
        #  There can be helpers that help fill configs with group of params that
        #  are related.
        rrt = dtf.ReplayRealTime(
            start_datetime,
            speed_up_factor=60.0,
        )
        # Data builder.
        data_builder = dtf.generate_synthetic_data
        data_builder_kwargs = {
            "columns": ["close", "vol"],
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
        }
        # Inject a real-time node.
        source_node_kwargs = {
            "delay_in_secs": 0.0,
            "external_clock": rrt.get_replayed_current_time,
            "data_builder": data_builder,
            "data_builder_kwargs": data_builder_kwargs
        }
        config["load_prices"] = cconfig.get_config_from_nested_dict(
            {
                "source_node_name": "RealTimeDataSource",
                "source_node_kwargs": source_node_kwargs,
            }
        )
        return config

    def get_dag(self, config: cconfig.Config, mode: str = "strict") -> dtf.DAG:
        """
        Generate pipeline DAG.
        """
        dag = self._dag_builder.get_dag(config, mode=mode)
        return dag

    @staticmethod
    def validate_config(config: cconfig.Config) -> None:
        """
        Sanity-check config.

        :param config: config object to validate
        """
        dbg.dassert(cconfig.check_no_dummy_values(config))