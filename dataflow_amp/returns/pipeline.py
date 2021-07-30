"""
Import as:

import dataflow_amp.returns.pipeline as darp
"""

import datetime
import logging
from typing import Optional, cast

import core.config as cconfig
import core.dataflow as dtf
import core.dataflow_source_nodes as dsn
import core.finance as fin
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# TODO(gp): -> ReturnPipeline everywhere?
class ReturnsPipeline(dtf.DagBuilder):
    """
    Pipeline for computing returns from price data.
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Return a template configuration for this pipeline.

        :return: reference config
        """
        dict_ = {
            # Load prices.
            # NOTE: The caller needs to inject config values to control the
            # `DataSourceNodeFactory` node in order to create the proper data
            # node.
            self._get_nid("load_prices"): {
                cconfig.DUMMY: None,
            },
            # Filter weekends.
            self._get_nid("filter_weekends"): {
                "col_mode": "replace_all",
            },
            # Filter ATH.
            self._get_nid("filter_ath"): {
                "col_mode": "replace_all",
                "transformer_kwargs": {
                    "start_time": datetime.time(9, 30),
                    "end_time": datetime.time(16, 00),
                },
            },
            # Resample prices to a 1 min grid.
            self._get_nid("resample_prices_to_1min"): {
                "func_kwargs": {
                    "rule": "1T",
                    "price_cols": ["close"],
                    # TODO(*): Rename "volume" to adhere with our naming
                    # conventions.
                    "volume_cols": ["vol"],
                },
            },
            # Compute VWAP.
            self._get_nid("compute_vwap"): {
                "func_kwargs": {
                    "rule": "5T",
                    "price_col": "close",
                    "volume_col": "vol",
                    "add_bar_start_timestamps": True,
                    "add_epoch": True,
                    "add_last_price": True,
                },
            },
            # Calculate returns.
            self._get_nid("compute_ret_0"): {
                "cols": ["twap", "vwap"],
                "col_mode": "merge_all",
                "transformer_kwargs": {
                    "mode": "pct_change",
                },
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def get_dag(self, config: cconfig.Config, mode: str = "strict") -> dtf.DAG:
        """
        Generate pipeline DAG.

        :param config: config object used to configure DAG
        :param mode: same meaning as in `dtf.DAG`
        :return: initialized DAG
        """
        dag = dtf.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        # Read data.
        stage = "load_prices"
        nid = self._get_nid(stage)
        node = dsn.DataSourceNodeFactory(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Set weekends to NaN.
        stage = "filter_weekends"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=fin.set_weekends_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Set non-ATH to NaN.
        stage = "filter_ath"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=fin.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Resample.
        stage = "resample_prices_to_1min"
        nid = self._get_nid(stage)
        node = dtf.FunctionWrapper(
            nid, func=fin.resample_time_bars, **config[nid].to_dict()
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Compute TWAP and VWAP.
        stage = "compute_vwap"
        nid = self._get_nid(stage)
        node = dtf.FunctionWrapper(
            nid,
            func=fin.compute_twap_vwap,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Compute returns.
        stage = "compute_ret_0"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=fin.compute_ret_0,
            col_rename_func=lambda x: x + "_ret_0",
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag

    @staticmethod
    def validate_config(config: cconfig.Config) -> None:
        """
        Sanity-check config.

        :param config: config object to validate
        """
        dbg.dassert(cconfig.check_no_dummy_values(config))

    @staticmethod
    def _append(dag: dtf.DAG, tail_nid: Optional[str], node: dtf.Node) -> str:
        dag.add_node(node)
        if tail_nid is not None:
            dag.connect(tail_nid, node.nid)
        return cast(str, node.nid)
