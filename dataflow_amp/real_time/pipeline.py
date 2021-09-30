"""
Import as:

import dataflow_amp.real_time.pipeline as dtfartp
"""

import logging

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


# TODO(gp): With the last implementation of the async dataflow nodes there is no
#  need to create a different DAG.
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
