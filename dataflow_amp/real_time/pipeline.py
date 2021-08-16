"""
Import as:

import dataflow_amp.real_time.real_time_return_pipeline as dtfart
"""

import logging

import pandas as pd

import core.config as cconfig
import core.dataflow as dtf
import dataflow_amp.returns.pipeline as darp
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

# There are different problems that we are trying to address here:
# 1) Compositions of DAGs: we want to put together different DAGs in a single one
#    (e.g., `RealTimeReturnsPipeline` is `ReturnsPipeline` plus some other components)
# 2) Mixing Python objects / functions (e.g., the real time logic) and DAG nodes
# 3) The same parameter needs to be used by different objects / functions and DAG nodes
#    and kept in sync some how (e.g., the `start_datetime` for the node and for the
#    `ReplayedTime`
# 4) Use different Python objects / functions inside the DAG

# We could build the DAG using a builder function which returns a DAG without following
# the pattern
# - generate a template config
# - fill out the template config to make it into a complete config
# - instantiate the DAG from the complete config

# In practice a `DagBuilder` is just syntactic sugar to keep together a template config
# and a corresponding function building a DAG.

# The rule of thumb should be to build the Python objects in the `get_dag` part.

# A possible solution for 3) is to use a "late binding" approach
# - In the config there can be a ConfigParam specifying the path of the corresponding
#   value to use
# Another approach is to use a "meta_parameter" Config key with all the parameters
# used by multiple nodes


class RealTimeReturnPipeline(dtf.DagBuilder):
    """
    Real-time pipeline for computing returns from price data.

    It is built as a composition of:
    - the real-time machinery
    - the DAG `ReturnsPipeline`
    """

    def __init__(self) -> None:
        super().__init__()
        self._dag_builder = darp.ReturnsPipeline()

    def get_config_template(self) -> cconfig.Config:
        """
        Return a template configuration for this pipeline.

        :return: reference config
        """
        # Get the DAG builder and the config template.
        config = self._dag_builder.get_config_template()
        config["meta_parameters"] = cconfig.get_config_from_nested_dict(
            {
                "start_datetime": "RealTimeDataSource",
                "real_time": {
                    "type": cconfig.DUMMY,
                    "kwargs": cconfig.DUMMY,
                },
            }
        )
        #
        start_datetime = pd.Timestamp("2010-01-04 09:30:00")
        end_datetime = pd.Timestamp("2010-01-04 11:30:00")
        # Use a replayed real-time starting at the same time as the data.
        # TODO(gp): This should be moved to the `get_dag()` part. The config_template
        #  contains all the fields that need to be filled, then the rest builds it.
        #  There can be helpers that help fill configs with group of params that
        #  are related.
        # TODO(gp): Add this
        get_wall_clock_time = None
        rrt = dtf.ReplayedTime(
            start_datetime,
            get_wall_clock_time,
        )
        # Data builder.
        data_builder = dtf.generate_synthetic_data
        data_builder_kwargs = {
            "columns": ["close", "vol"],
            "start_datetime": cconfig.DUMMY,
            "end_datetime": cconfig.DUMMY,
        }
        # Inject a real-time node.
        source_node_kwargs = {
            "delay_in_secs": 0.0,
            "external_clock": rrt.get_wall_clock_time,
            "data_builder": data_builder,
            "data_builder_kwargs": data_builder_kwargs,
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
        # Wire some parameters that depend on other already set.

        # Use a replayed real-time starting at the same time as the data.
        # TODO(gp): This should be moved to the `get_dag()` part. The config_template
        #  contains all the fields that need to be filled, then the rest builds it.
        #  There can be helpers that help fill configs with group of params that
        #  are related.
        rrt = dtf.ReplayedTime(
            config["meta_parameters"]["start_datetime"],
            config["replayed_time"]["get_wall_clock_time"],
        )
        # Data builder.
        data_builder = dtf.generate_synthetic_data
        data_builder_kwargs = {
            "columns": ["close", "vol"],
            "start_datetime": cconfig.DUMMY,
            "end_datetime": cconfig.DUMMY,
        }
        # Inject a real-time node.
        source_node_kwargs = {
            "delay_in_secs": 0.0,
            "external_clock": rrt.get_wall_clock_time,
            "data_builder": data_builder,
            "data_builder_kwargs": data_builder_kwargs,
        }

        dag = self._dag_builder.get_dag(config, mode=mode)
        return dag

    @staticmethod
    def validate_config(config: cconfig.Config) -> None:
        """
        Sanity-check config.

        :param config: config object to validate
        """
        dbg.dassert(cconfig.check_no_dummy_values(config))
