"""
Import as:

import dataflow.system.system_builders_utils as dtfssybuut
"""

import logging

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system.system as dtfsyssyst
import helpers.hdbg as hdbg
import market_data as mdata

_LOG = logging.getLogger(__name__)


# #############################################################################
# System config instances
# #############################################################################


def get_system_config_template_instance1(
    dag_builder: dtfcore.DagBuilder,
) -> cconfig.Config:
    """
    Build a system config from a DAG builder.
    """
    system_config = cconfig.Config()
    # Save the `DagBuilder` and the `DagConfig` in the config object.
    hdbg.dassert_isinstance(dag_builder, dtfcore.DagBuilder)
    dag_config = dag_builder.get_config_template()
    system_config["dag_config"] = dag_config
    system_config["dag_builder_object"] = dag_builder
    return system_config


# #############################################################################
# Market data instances
# #############################################################################


# TODO(gp): -> get_event_loop_market_data_from_df
def get_event_loop_market_data_instance1(
    system: dtfsyssyst.System,
) -> mdata.ReplayedMarketData:
    """
    Build an event loop MarketData with data from a dataframe.
    """
    event_loop = system.config["event_loop_object"]
    initial_replayed_delay = system.config[
        "market_data_config", "initial_replayed_delay"
    ]
    data = system.config["market_data_config", "data"]
    market_data, _ = mdata.get_ReplayedTimeMarketData_from_df(
        event_loop,
        initial_replayed_delay,
        data,
    )
    return market_data
