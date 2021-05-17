import abc
import datetime
import logging
from typing import Any, Dict, List, Optional

import core.config as cconfi
import core.config_builders as cfgb
import core.finance as fin
from core.dataflow.core import DAG, Node
from core.dataflow.nodes.volatility_models import VolatilityModel
from core.dataflow.nodes.sources import ArmaGenerator
from core.dataflow.nodes.transformers import (
    ColumnTransformer,
    TimeBarResampler,
    TwapVwapComputer,
)

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
                "Appended '/' to nid_prefix '%s'. To avoid this warning, "
                "only pass nid prefixes ending in '/'.",
                self._nid_prefix,
            )
            self._nid_prefix += "/"

    @property
    def nid_prefix(self) -> str:
        return self._nid_prefix

    @abc.abstractmethod
    def get_config_template(self) -> cconfi.Config:
        """
        Return a config template compatible with `self.get_dag`.

        :return: a valid configuration for `self.get_dag`, possibly with some
            "dummy" required paths.
        """

    @abc.abstractmethod
    # TODO(*): Consider getting rid of the `dag` argument here.
    def get_dag(self, config: cconfi.Config, dag: Optional[DAG] = None) -> DAG:
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

    @property
    def methods(self) -> List[str]:
        """
        Methods supported by the DAG.
        """
        # TODO(*): Consider make this an abstractmethod.
        return ["fit", "predict"]

    def get_column_to_tags_mapping(
        self, config: cconfi.Config
    ) -> Optional[Dict[Any, List[str]]]:
        """
        Get a dictionary of result nid column names to semantic tags.

        :return: dictionary keyed by column names and with values that are
            lists of str tag names
        """
        _ = self, config
        return None

    def _get_nid(self, stage_name: str) -> str:
        nid = self._nid_prefix + stage_name
        return nid


class ArmaReturnsBuilder(DagBuilder):
    """
    Pipeline for generating filtered returns from an ARMA process.
    """

    def get_config_template(self) -> cconfi.Config:
        """
        Return a reference configuration.

        :return: reference config
        """
        config = cfgb.get_config_from_nested_dict(
            {
                # Load prices.
                self._get_nid("rets/read_data"): {
                    "frequency": "T",
                    "start_date": "2010-01-04 09:00:00",
                    "end_date": "2010-01-04 16:30:00",
                    "ar_coeffs": [0],
                    "ma_coeffs": [0],
                    "scale": 0.1,
                    "burnin": 0,
                    "seed": 0,
                },
                # Filter ATH.
                self._get_nid("rets/filter_ath"): {
                    "col_mode": "replace_all",
                    "transformer_kwargs": {
                        "start_time": datetime.time(9, 30),
                        "end_time": datetime.time(16, 00),
                    },
                },
                # Resample returns.
                self._get_nid("rets/resample"): {
                    "rule": "1T",
                    "price_cols": ["close"],
                    "volume_cols": ["vol"],
                },
                # Compute TWAP and VWAP.
                self._get_nid("rets/compute_wap"): {
                    "rule": "5T",
                    "price_col": "close",
                    "volume_col": "vol",
                },
                # Calculate rets.
                self._get_nid("rets/compute_ret_0"): {
                    "cols": ["twap", "vwap"],
                    "col_mode": "merge_all",
                    "transformer_kwargs": {
                        "mode": "pct_change",
                    },
                },
                # Model volatility.
                self._get_nid("rets/model_volatility"): {
                    "cols": ["vwap_ret_0"],
                    "steps_ahead": 2,
                    "nan_mode": "leave_unchanged",
                },
                # Clip rets.
                self._get_nid("rets/clip"): {
                    "cols": ["vwap_ret_0_zscored"],
                    "col_mode": "replace_selected",
                },
            }
        )
        return config

    def get_dag(self, config: cconfi.Config, mode: str = "strict") -> DAG:
        """
        Generate pipeline DAG.

        :param config: config object used to configure DAG
        :param mode: "strict" (e.g., for production) or "loose" (e.g., for
            interactive jupyter notebooks)
        :return: initialized DAG
        """
        dag = DAG(mode=mode)
        _LOG.debug("%s", config)
        # Read data.
        stage = "rets/read_data"
        nid = self._get_nid(stage)
        node = ArmaGenerator(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, None, node)
        # Set weekends to Nan.
        stage = "rets/filter_weekends"
        nid = self._get_nid(stage)
        node = ColumnTransformer(
            nid,
            transformer_func=fin.set_weekends_to_nan,
            col_mode="replace_all",
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Set non-ATH to NaN.
        stage = "rets/filter_ath"
        nid = self._get_nid(stage)
        node = ColumnTransformer(
            nid,
            transformer_func=fin.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Resample.
        stage = "rets/resample"
        nid = self._get_nid(stage)
        node = TimeBarResampler(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Compute TWAP and VWAP.
        stage = "rets/compute_wap"
        nid = self._get_nid(stage)
        node = TwapVwapComputer(
            nid,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Compute returns.
        stage = "rets/compute_ret_0"
        nid = self._get_nid(stage)
        node = ColumnTransformer(
            nid,
            transformer_func=fin.compute_ret_0,
            col_rename_func=lambda x: x + "_ret_0",
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Model volatility.
        stage = "rets/model_volatility"
        nid = self._get_nid(stage)
        node = VolatilityModel(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Clip rets.
        stage = "rets/clip"
        nid = self._get_nid(stage)
        node = ColumnTransformer(
            nid,
            transformer_func=lambda x: x.clip(lower=-3, upper=3),
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        return dag

    @staticmethod
    def _append(dag: DAG, tail_nid: Optional[str], node: Node) -> str:
        dag.add_node(node)
        if tail_nid is not None:
            dag.connect(tail_nid, node.nid)
        return node.nid
