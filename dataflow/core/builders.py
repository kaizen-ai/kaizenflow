"""
Import as:

import dataflow.core.builders as dtfcorbuil
"""
import abc
import logging
from typing import Any, Dict, List, Optional, cast

import core.config as cconfig
import dataflow.core.dag as dtfcordag
import dataflow.core.node as dtfcornode
import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


class DagBuilder(abc.ABC):
    """
    Abstract class for creating DAGs.

    Concrete classes must specify:
      1) `get_config_template()`
        - It returns a `Config` object that represents the parameters used to build
          the DAG
        - The config can depend upon variables used in class initialization
        - A config can be incomplete, e.g., `cconfig.DUMMY` is used for required
          fields that must be defined before the config can be used to initialize
          a DAG
      2) `get_dag()`
        - It builds a DAG
        - Defines the DAG nodes and how they are connected to each other. The
          passed-in config object tells this function how to
          configure/initialize the various nodes.
    """

    def __init__(self, nid_prefix: Optional[str] = None) -> None:
        """
        Constructor.

        :param nid_prefix: a namespace ending with "/" for graph node naming.
            This may be useful if the DAG built by the builder is either built
            upon an existing DAG or will be built upon subsequently.
        """
        # If no nid prefix is specified, make it an empty string to simplify
        # the implementation of helpers.
        self._nid_prefix = nid_prefix or ""
        # Make sure the nid_prefix ends with "/" (unless it is "").
        if self._nid_prefix and not self._nid_prefix.endswith("/"):
            _LOG.warning(
                "Appended '/' to nid_prefix '%s'. To avoid this warning, "
                "only pass nid prefixes ending in '/'.",
                self._nid_prefix,
            )
            self._nid_prefix += "/"

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    @abc.abstractmethod
    def get_config_template(self) -> cconfig.Config:
        """
        Return a config template compatible with `self.get_dag`.

        :return: a valid configuration for `self.get_dag`, possibly with some
            "dummy" required paths.
        """

    def get_dag(
        self, config: cconfig.Config, mode: str = "strict", validate: bool = True
    ) -> dtfcordag.DAG:
        """
        Build DAG given `config`.

        :param config: configures DAG. It is up to the client to guarantee
            compatibility. The result of `self.get_config_template` should
            always be compatible following template completion.
        :param mode: as in `DAG` constructor
        :return: `dag` with all builder operations applied
        """
        dag = self._get_dag(config, mode=mode)
        if validate:
            self.validate_config_and_dag(config, dag)
        return dag

    @staticmethod
    def validate_config_and_dag(
        config: cconfig.Config, dag: dtfcordag.DAG
    ) -> None:
        """
        Wraps `get_dag()` with additional sanity-checks.

        - Raises if `config` has a DUMMY value
        - Raises if `config` has an entry for a node that is not in the DAG
        """
        hdbg.dassert(cconfig.check_no_dummy_values(config))
        for key in config.to_dict().keys():
            # This raises if the node does not exist.
            dag.get_node(key)

    @property
    def methods(self) -> List[str]:
        """
        Methods supported by the DAG.
        """
        # TODO(*): Consider make this an abstractmethod.
        return ["fit", "predict"]

    # TODO(gp): -> tighten types along the lines of `Dict[Column, ...]`.
    def get_column_to_tags_mapping(  # pylint: disable=useless-return
        self, config: cconfig.Config
    ) -> Optional[Dict[Any, List[str]]]:
        """
        Get a dictionary of result nid column names to semantic tags.

        :return: dictionary keyed by column names and with values that are
            lists of str tag names
        """
        _ = self, config
        return None

    @abc.abstractmethod
    def _get_dag(self, config: cconfig.Config, mode: str = "strict"):
        """
        Implement the dag.
        """
        ...

    def _get_nid(self, stage_name: str) -> str:
        nid = self._nid_prefix + stage_name
        return nid

    @staticmethod
    def _append(
        dag: dtfcordag.DAG, tail_nid: Optional[str], node: dtfcornode.Node
    ) -> str:
        dag.add_node(node)
        if tail_nid is not None:
            dag.connect(tail_nid, node.nid)
        nid = node.nid
        nid = cast(str, nid)
        return nid
