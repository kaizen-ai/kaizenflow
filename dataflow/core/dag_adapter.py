"""
Import as:

import dataflow.core.dag_adapter as dtfcodaada
"""

import logging
from typing import Any, Dict, List

import core.config as cconfig
import dataflow.core.dag as dtfcordag
import dataflow.core.dag_builder as dtfcodabui
import dataflow.core.node as dtfcornode
import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


class DagAdapter(dtfcodabui.DagBuilder):
    """
    Adapt a DAG builder by overriding part of the config and appending nodes.
    """

    def __init__(
        self,
        dag_builder: dtfcodabui.DagBuilder,
        overriding_config: Dict[str, Any],
        nodes_to_insert: List[dtfcornode.Node],
        nodes_to_append: List[dtfcornode.Node],
        **kwargs: Any,
    ):
        """
        Constructor.

        :param dag_builder: a `DagBuilder` containing a single sink
        :param overriding_config: a template `Config` containing the fields to
            override. Note that this `Config` can still be a template, i.e.,
            containing dummies that are finally overwritten by callers.
        :param nodes_to_append: list of tuples `(node name, constructor)` storing
            the nodes to append to the DAG created from `dag_builder`.
            The node constructor function should accept only the `nid` and the
            configuration dict, while all the other inputs need to be already
            specified.
        """
        super().__init__()
        hdbg.dassert_isinstance(dag_builder, dtfcodabui.DagBuilder)
        self._dag_builder = dag_builder
        hdbg.dassert_isinstance(overriding_config, cconfig.Config)
        self._overriding_config = overriding_config
        hdbg.dassert_container_type(nodes_to_insert, list, tuple)
        self._nodes_to_insert = nodes_to_insert
        hdbg.dassert_container_type(nodes_to_append, list, tuple)
        self._nodes_to_append = nodes_to_append

    def __str__(self) -> str:
        txt = []
        #
        txt.append("dag_builder=")
        txt.append(hprint.indent(str(self._dag_builder), 2))
        #
        txt.append("overriding_config=")
        txt.append(hprint.indent(str(self._overriding_config), 2))
        #
        txt.append("nodes_to_insert=")
        txt.append(hprint.indent("\n".join(map(str, self._nodes_to_insert)), 2))
        #
        txt.append("nodes_to_append=")
        txt.append(hprint.indent("\n".join(map(str, self._nodes_to_append)), 2))
        #
        txt = "\n".join(txt)
        return txt

    def get_config_template(self) -> cconfig.Config:
        config = self._dag_builder.get_config_template()
        config.update(self._overriding_config)
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcordag.DAG:
        # Remove the nodes that are in config
        nested_config_template = self._dag_builder.get_config_template()
        config_diff = cconfig.Config()
        for key in config.keys():
            if key in nested_config_template:
                config_diff[key] = config[key]
        _LOG.debug("# config_diff=\n%s", str(config_diff))
        dag = self._dag_builder.get_dag(config_diff, mode=mode)
        _LOG.debug("# dag=\n%s", str(dag))
        #
        if self._nodes_to_insert:
            _LOG.debug("# Inserting nodes")
            # To insert a node we need to to assume that there is a single source node.
            source_nid = dag.get_unique_source()
            # TODO(gp): Allow to insert more than one node, if needed.
            hdbg.dassert_eq(len(self._nodes_to_insert), 1)
            stage, node_ctor = self._nodes_to_insert[0]
            _LOG.debug(hprint.to_str("stage node_ctor"))
            head_nid = (
                self._dag_builder._get_nid(  # pylint: disable=protected-access
                    stage
                )
            )
            node = node_ctor(
                head_nid,
                **config[head_nid].to_dict(),
            )
            dag.add_node(node)
            dag.connect(head_nid, source_nid)
        if self._nodes_to_append:
            _LOG.debug("# Appending nodes")
            # To append a node we need to to assume that there is a single sink node.
            sink_nid = dag.get_unique_sink()
            # TODO(gp): Allow to append more than one node, if needed.
            hdbg.dassert_eq(len(self._nodes_to_append), 1)
            stage, node_ctor = self._nodes_to_append[0]
            _LOG.debug(hprint.to_str("stage node_ctor"))
            tail_nid = (
                self._dag_builder._get_nid(  # pylint: disable=protected-access
                    stage
                )
            )
            node = node_ctor(
                tail_nid,
                **config[tail_nid].to_dict(),
            )
            dag.add_node(node)
            dag.connect(sink_nid, tail_nid)
        return dag
