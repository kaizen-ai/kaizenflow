from __future__ import annotations

import abc
import collections
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

import core.config as cfg
import core.config_builders as cfgb
import helpers.git as git
from core.dataflow.core import DAG
from core.dataflow.nodes import extract_info

_LOG = logging.getLogger(__name__)


class ResultBundle(abc.ABC):
    """
    Abstract class for storing DAG results.
    """
    def __init__(
        self,
        config: Optional[cfg.Config] = None,
        result_nid: Optional[str] = None,
        method: Optional[str] = None,
        result_df: Optional[pd.DataFrame] = None,
        column_to_tags: Optional[Dict[Any, List[Any]]] = None,
        info: Optional[collections.OrderedDict] = None,
    ):
        """
        :param config: DAG config
        :param result_nid: identifier of terminal node for which DAG was
            executed
        :param method: method which was executed
        :param result_df: dataframe with results
        :param column_to_tags: mapping of column names to tags
        :param info: DAG execution info
        """
        self._config = config
        self._result_nid = result_nid
        self._method = method
        self._result_df = result_df
        self._column_to_tags = column_to_tags
        if info is not None:
            info = cfgb.get_config_from_nested_dict(info)
        self._info = info

    @property
    def config(self) -> Optional[cfg.Config]:
        if self._config is not None:
            return self._config.copy()

    @property
    def result_nid(self) -> Optional[str]:
        return self._result_nid

    @property
    def method(self) -> Optional[str]:
        return self._method

    @property
    def result_df(self) -> Optional[pd.DataFrame]:
        if self._result_df is not None:
            return self._result_df.copy()

    @property
    def column_to_tags(self) -> Optional[Dict[Any, List[Any]]]:
        if self._column_to_tags is not None:
            return self._column_to_tags.copy()

    @property
    def tag_to_columns(self) -> Optional[Dict[Any, List[Any]]]:
        if self._column_to_tags is not None:
            tag_to_columns: Dict[Any, List[Any]] = {}
            for column, tags in self._column_to_tags.items():
                for tag in tags:
                    tag_to_columns.setdefault(tag, []).append(column)
            return tag_to_columns

    @property
    def info(self) -> Optional[cfg.Config]:
        if self._info is not None:
            return self._info.copy()

    def to_config(self) -> cfg.Config:
        """
        Represent class state as config.
        """
        serialized_bundle = cfg.Config()
        serialized_bundle["config"] = self._config
        serialized_bundle["result_nid"] = self._result_nid
        serialized_bundle["method"] = self._method
        serialized_bundle["result_df"] = self._result_df
        serialized_bundle["column_to_tags"] = self._column_to_tags
        serialized_bundle["info"] = self._info
        serialized_bundle["class"] = self.__class__.__name__
        serialized_bundle["commit_hash"] = git.get_current_commit_hash()
        return serialized_bundle

    @classmethod
    def from_config(cls, serialized_bundle: cfg.Config) -> ResultBundle:
        """
        Initialize `ResultBundle` from config.
        """
        rb = cls(
            config=serialized_bundle["config"],
            result_nid=serialized_bundle["result_nid"],
            method=serialized_bundle["method"],
            result_df=serialized_bundle["result_df"],
            column_to_tags=serialized_bundle["column_to_tags"],
            info=serialized_bundle["info"],
        )
        return rb

    def get_tags_for_column(self, column: Any) -> Optional[List[Any]]:
        return self._search_mapping(column, self._column_to_tags)

    def get_columns_for_tag(self, tag: Any) -> Optional[List[Any]]:
        return self._search_mapping(tag, self.tag_to_columns)

    @staticmethod
    def _search_mapping(
        value: Any, mapping: Optional[Dict[Any, List[Any]]]
    ) -> Optional[List[Any]]:
        if mapping is None:
            _LOG.warning("No mapping provided.")
            return None
        if value not in mapping:
            _LOG.warning("'%s' not in `mapping`='%s'.", value, mapping)
            return None
        return mapping[value]


# TODO(Paul): Consider moving this to `core.py`.
class DagManager(abc.ABC):
    """
    Abstract class for creating DAGs.

    Concrete classes must specify:
      - a default configuration (which may depend upon variables used in class
        initialization)
      - the construction of a DAG
    """

    def __init__(self, nid_prefix: Optional[str] = None) -> None:
        """

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
                "Appended '/' to nid_prefix. To avoid this warning, "
                "only pass nid prefixes ending in '/'."
            )
            self._nid_prefix += "/"

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    @abc.abstractmethod
    def get_config_template(self) -> cfg.Config:
        """
        Return a config template compatible with `self.get_dag`.

        :return: a valid configuration for `self.get_dag`, possibly with some
            "dummy" required paths.
        """

    @abc.abstractmethod
    def get_dag(self, config: cfg.Config, dag: Optional[DAG] = None) -> DAG:
        """
        Build DAG given `config`.

        WARNING: This function modifies `dag` in-place.
        TODO(Paul): Consider supporting deep copies for `dtf.DAG`.

        :param config: configures DAG. It is up to the client to guarantee
            compatibility. The result of `self.get_config_template` should
            always be compatible following template completion.
        :param dag: may or may not have nodes. If the DAG already has nodes,
            it is up to the client to ensure that there are no `nid` (node id)
            collisions, which can be ensured through the use of `nid_prefix`.
            If this parameter is `None`, then a new `dtf.DAG` object is
            created.
        :return: `dag` with all builder operations applied
        """

    def run_dag(self, config: cfg.Config, nid: str, method: str) -> ResultBundle:
        """
        Build and run DAG.

        :param config: config for which to build DAG
        :param nid: identifier of terminal node for execution
        :param method: `Node` subclass method to be executed
        :return: `ResultBundle` class containing `config`, `nid`, `method`,
            result dataframe and DAG info
        """
        dag = self.get_dag(config)
        df_out = dag.run_leq_node(nid, method)["df_out"]
        info = extract_info(dag, [method])
        return ResultBundle(
            config=config,
            result_nid=nid,
            method=method,
            result_df=df_out,
            column_to_tags=None,
            info=info,
        )

    def _get_nid(self, stage_name: str) -> str:
        nid = self._nid_prefix + stage_name
        return nid


class DagRunner:
    """
    Class for running DAGs from a meta-config.

    TODO(*): Handle set fit/predict intervals.

    The result bundle self.result_bundle is organized as
      - config
      - predictor_cols (type: List[str])
      - target_cols
      - prediction_cols
      - info
        - fit (val type: OrderedDict)
        - predict (val type: OrderedDict)
      - run_results
        - fit (val type: pd.DataFrame)
        - predict (val type: pd.DataFrame)
    """

    def __init__(self, config: cfg.Config) -> None:
        """
        Initialize DAG and skeleton of result_bundle.

        :param config: A meta config.

        `config` should contain keys:
          - "meta"
            - "dag_builder" (val type: DagManager)
            - "result_node" (val type: str)
          - "DAG"

        The `dag_builder` should have methods
          - validate_config()
          - get_target_and_prediction_col_names()
          - get_predictor_col_names()

        The dataframe output of the node `result_node` of the `DAG` should contain
        the predictor, prediction, and target column names. This should be
        accessible from the node through "df_out".
        """
        self.config = config
        # Use DAG builder to validate DAG config.
        self._dag_builder = config["meta"]["dag_builder"]
        self._dag_builder.validate_config(self.config["DAG"])
        # Create DAG using DAG builder.
        self.dag = self._dag_builder.get_dag(self.config["DAG"])
        # Identify key columns.
        (
            self.target_cols,
            self.prediction_cols,
        ) = self._dag_builder.get_target_and_prediction_col_names(
            self.config["DAG"]
        )
        self.predictor_cols = self._dag_builder.get_predictor_col_names(
            self.config["DAG"]
        )
        self.result_node = self.config["meta", "result_node"]
        # Create result bundle.
        self.result_bundle = collections.OrderedDict()
        self.result_bundle["config"] = self.config
        self.result_bundle["predictor_cols"] = self.predictor_cols
        self.result_bundle["target_cols"] = self.target_cols
        self.result_bundle["prediction_cols"] = self.prediction_cols
        self.result_bundle["info"] = collections.OrderedDict()
        self.result_bundle["run_results"] = collections.OrderedDict()

    def fit(self) -> None:
        self._run_helper("fit")

    def predict(self) -> None:
        self._run_helper("predict")

    def _run_helper(self, mode: str) -> None:
        df = self.dag.run_leq_node(self.result_node, mode)["df_out"]
        self.result_bundle["run_results"][mode] = df
        info = extract_info(self.dag, [mode])
        self.result_bundle["info"][mode] = info
