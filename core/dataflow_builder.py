import abc
import logging
from typing import Optional, Tuple

import core.config as cfg
import core.dataflow as dtf
import helpers.dbg as dbg

class DagBuilder(abc.ABC):
    """
    Abstract class for creating DAGs.
    """

    def __init__(self,
                 config: Optional[cfg.Config] = None
                 nid_prefix: Optional[str] = None
                 ) -> None:
        """

        :param nid_prefix:
        """
        self._nid_prefix = nid_prefix or ""
        # TODO(Paul): Defined in concrete class. Make static too?
        self._config = config or self._get_default_config()

    @property
    def config(self) -> cfg.Config:
        return self._config

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    # TODO(Paul): Add setters for `nid_prefix`, `config`.

    def get_nid_and_config(stage_name: str) -> Tuple[str, cfg.Config]:
        nid = self._nid_prefix + stage_name
        config = self._config[stage_name]
        return nid, config

    @abc.abstractmethod
    def _get_default_config() -> cfg.Config:
        pass

    @abc.abstractmethod
    def get_dag(dtf.DAG) -> dtf.DAG:
        pass
