"""
Import as:

import dataflow.pipelines.examples.pipeline1 as dtfpiexpip
"""

import datetime
import logging

import numpy as np

import core.config as cconfig
import core.features as cofeatur
import core.finance as cofinanc
import core.signal_processing as csigproc
import dataflow.core as dtfcore

_LOG = logging.getLogger(__name__)

# TODO(gp): -> example_pipeline1.py?


class ExamplePipeline1_DagBuilder(dtfcore.DagBuilder):
    """
    A pipeline similar to real models.
    """

    def get_config_template(self) -> cconfig.Config:
        dict_ = {
            self._get_nid("filter_ath"): {
                "col_mode": "replace_all",
                "transformer_kwargs": {
                    "start_time": datetime.time(9, 30),
                    "end_time": datetime.time(16, 00),
                },
            },
            self._get_nid("resample"): {
                "in_col_groups": [
                    ("close",),
                    ("volume",),
                    ("feature1",),
                ],
                "out_col_group": (),
                "transformer_kwargs": {
                    "rule": "5T",
                    "resampling_groups": [
                        ({"close": "close"}, "last", {}),
                        (
                            {
                                "close": "twap",
                                "feature1": "feature1",
                            },
                            "mean",
                            {},
                        ),
                    ],
                    "vwap_groups": [
                        ("close", "volume", "vwap"),
                    ],
                },
                "reindex_like_input": False,
                "join_output_with_input": False,
            },
            self._get_nid("compute_ret_0"): {
                "in_col_groups": [
                    ("close",),
                    ("vwap",),
                    ("twap",),
                ],
                "out_col_group": (),
                "transformer_kwargs": {
                    "mode": "log_rets",
                },
                "col_mapping": {
                    "close": "close.ret_0",
                    "vwap": "vwap.ret_0",
                    "twap": "twap.ret_0",
                },
            },
            self._get_nid("compute_vol"): {
                "in_col_group": ("vwap.ret_0",),
                "out_col_group": ("vwap.ret_0.vol",),
                "drop_nans": True,
                "permitted_exceptions": (ValueError,),
            },
            self._get_nid("adjust_rets"): {
                "in_col_groups": [
                    ("vwap.ret_0",),
                    ("vwap.ret_0.vol",),
                ],
                "out_col_group": (),
                "transformer_kwargs": {
                    "term1_col": "vwap.ret_0",
                    "term2_col": "vwap.ret_0.vol",
                    "out_col": "vwap.ret_0.vol_adj",
                    "term2_delay": 2,
                    "operation": "div",
                },
                "drop_nans": True,
            },
            self._get_nid("compress_rets"): {
                "in_col_groups": [
                    ("vwap.ret_0.vol_adj",),
                ],
                "out_col_group": (),
                "col_mapping": {
                    "vwap.ret_0.vol_adj": "vwap.ret_0.vol_adj.c",
                },
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def _get_dag(
        self,
        config: cconfig.Config,
        mode: str = "strict",
    ) -> dtfcore.DAG:
        dag = dtfcore.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        #
        stage = "filter_ath"
        nid = self._get_nid(stage)
        node = dtfcore.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "resample"
        nid = self._get_nid(stage)
        node = dtfcore.GroupedColDfToDfTransformer(
            nid,
            transformer_func=cofinanc.resample_bars,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "compute_ret_0"
        nid = self._get_nid(stage)
        node = dtfcore.GroupedColDfToDfTransformer(
            nid,
            transformer_func=cofinanc.compute_ret_0,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "compute_vol"
        nid = self._get_nid(stage)
        node = dtfcore.SeriesToSeriesTransformer(
            nid,
            transformer_func=lambda x: np.sqrt(
                csigproc.compute_swt_var(x, depth=1)["swt_var"]
            ).rename(x.name),
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "adjust_rets"
        nid = self._get_nid(stage)
        node = dtfcore.GroupedColDfToDfTransformer(
            nid,
            transformer_func=cofeatur.combine_columns,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "compress_rets"
        nid = self._get_nid(stage)
        node = dtfcore.GroupedColDfToDfTransformer(
            nid,
            transformer_func=lambda x: csigproc.compress_tails(x, 4),
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        #
        return dag