import abc
import logging
from typing import Optional, Tuple

import core.config as cfg
import core.dataflow as dtf
import helpers.dbg as dbg

class DagBuilder(abc.ABC):
    """
    Abstract class for creating DAGs.

    Concrete classes must specify
      - a default configuration (which may depend upon variables used in class
        initialization)
      - the construction of a dag
    """

    def __init__(self,
                 config: Optional[cfg.Config] = None,
                 nid_prefix: Optional[str] = None
                 ) -> None:
        """

        :param nid_prefix:
        """
        self._nid_prefix = nid_prefix or ""
        # TODO(Paul): Defined in concrete class. Make static too?
        self._config = config or self.get_default_config()

    @property
    def config(self) -> cfg.Config:
        return self._config

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    # TODO(Paul): Add setters for `nid_prefix`, `config`.

    def _get_nid_and_config(self, stage_name: str) -> Tuple[str, cfg.Config]:
        nid = self._nid_prefix + stage_name
        config = self._config[stage_name]
        return nid, config.copy()

    @abc.abstractmethod
    def get_default_config(self) -> cfg.Config:
        pass

    @abc.abstractmethod
    def get_dag(self, dag: Optional[dtf.DAG] = None) -> dtf.DAG:
        pass
