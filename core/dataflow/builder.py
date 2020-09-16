import abc
import collections
import logging
from typing import Optional

import core.config as cfg
from core.dataflow.core import DAG
from core.dataflow.nodes import extract_info

_LOG = logging.getLogger(__name__)


# TODO(Paul): Consider moving this to `core.py`.
class DagBuilder(abc.ABC):
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
            - "dag_builder" (val type: DagBuilder)
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
