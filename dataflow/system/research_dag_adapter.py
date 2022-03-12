"""
Import as:

import dataflow.system.research_dag_adapter as dtfsredaad
"""

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod


class ResearchDagAdapter(dtfcore.DagAdapter):
    """
    Adapt a DAG builder for the research flow (batch execution, no OMS).
    """

    def __init__(
        self,
        dag_builder: dtfcore.DagBuilder,
        source_node_config: cconfig.Config,
    ):
        overriding_config = cconfig.Config()
        # Configure a DataSourceNode.
        overriding_config["load_prices"] = {**source_node_config.to_dict()}
        # Insert a node.
        nodes_to_insert = []
        stage = "load_prices"
        node_ctor = dtfsysonod.data_source_node_factory
        nodes_to_insert.append((stage, node_ctor))
        #
        super().__init__(dag_builder, overriding_config, nodes_to_insert, [])