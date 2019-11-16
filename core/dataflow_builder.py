import abc
import logging
from typing import Optional, Tuple

import core.config as cfg
import core.dataflow as dtf
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class DagBuilder(abc.ABC):
    """
    Abstract class for creating DAGs.

    Concrete classes must specify
      - a default configuration (which may depend upon variables used in class
        initialization)
      - the construction of a dag
    """

    def __init__(self,
                 nid_prefix: Optional[str] = None
                 ) -> None:
        """

        :param nid_prefix: a namespace ending with "/" for graph node naming.
            This may be useful if the DAG built by the builder is eiter built
            upon an existing DAG or will be built upon subsequently.
        """
        # If no nid prefix is specified, make it an empty string to simplify
        # the implementation of helpers.
        self._nid_prefix = nid_prefix or ""
        # Make sure the nid_prefix ends with "/" (unless it is "").
        if self._nid_prefix and not self._nid_prefix.endswith("/"):
            _LOG.warning("Appended '/' to nid_prefix. To avoid this warning, "
                         "only pass nid prefixes ending in '/'.")
            self._nid_prefix += "/"

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    def _get_nid(self, stage_name: str) -> str:
        nid = self._nid_prefix + stage_name
        return nid

    @abc.abstractmethod
    def get_config_template(self) -> cfg.Config:
        """
        Return a config template compatible with `self.get_dag`.

        :return: a valid configuration for `self.get_dag`, possibly with some
            "dummy" required paths.
        """
        pass

    @abc.abstractmethod
    def get_dag(self, config: cfg.Config, dag: Optional[dtf.DAG] = None) -> dtf.DAG:
        """
        Build DAG given `config`.

        WARNING: This function modifies `dag` in-place.
        TODO(Paul): Consider supporting deep copies for `dtf.DAG`.

        :param config: configures DAG. It is up to the client to guarantee
            compatibility. The result of `self.get_config` should always be
            compatible.
        :param dag: may or may not have nodes. If the DAG already has nodes,
            it is up to the client to ensure that there are no nid (node id)
            collisions, which can be ensured through the use of `nid_prefix`.
            If this parameter is `None`, then a new `dtf.DAG` object is
            created.
        :return: `dag` with all builder operations applied
        """
        pass
