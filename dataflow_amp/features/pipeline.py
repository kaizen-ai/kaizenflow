"""
Import as:

import dataflow_amp.features.pipeline as dtfamfepip
"""

import datetime
import logging

import core.config as cconfig
import core.dataflow as dtf
import core.dataflow_source_nodes as cdtfsonod
import core.features as cfea
import core.finance as cfin
import core.signal_processing as csipro
import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


class FeaturePipeline(dtf.DagBuilder):
    def get_config_template(self) -> cconfig.Config:
        dict_ = {
            self._get_nid("load_data"): {
                cconfig.DUMMY: None,
            },
            self._get_nid("filter_weekends"): {
                "col_mode": "replace_all",
            },
            self._get_nid("filter_ath"): {
                "col_mode": "replace_all",
                "transformer_kwargs": {
                    "start_time": datetime.time(9, 30),
                    "end_time": datetime.time(16, 00),
                },
            },
            self._get_nid("perform_col_arithmetic"): {
                "func_kwargs": {
                    cconfig.DUMMY: None,
                }
            },
            self._get_nid("add_diffs"): {
                "col_mode": "merge_all",
            },
            self._get_nid("zscore"): {
                "col_mode": "replace_all",
                "nan_mode": "drop",
            },
            self._get_nid("compress_tails"): {
                "col_mode": "replace_all",
            },
            self._get_nid("cross_feature_pairs"): {
                "func_kwargs": {
                    cconfig.DUMMY: None,
                },
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def get_dag(self, config: cconfig.Config, mode: str = "strict") -> dtf.DAG:
        dag = dtf.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        #
        stage = "load_data"
        nid = self._get_nid(stage)
        node = cdtfsonod.data_source_node_factory(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "filter_weekends"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=cfin.set_weekends_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "filter_ath"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=cfin.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "perform_col_arithmetic"
        nid = self._get_nid(stage)
        node = dtf.FunctionWrapper(
            nid,
            func=cfea.perform_col_arithmetic,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "add_diffs"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=lambda x: x.diff(),
            col_rename_func=lambda x: x + ".diff",
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "zscore"
        nid = self._get_nid(stage)
        node = dtf.SeriesTransformer(
            nid,
            transformer_func=csipro.compute_fir_zscore,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "compress_tails"
        nid = self._get_nid(stage)
        node = dtf.SeriesTransformer(
            nid,
            transformer_func=csipro.compress_tails,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        stage = "cross_feature_pairs"
        nid = self._get_nid(stage)
        node = dtf.FunctionWrapper(
            nid,
            func=cfea.cross_feature_pairs,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag

    @staticmethod
    def validate_config(config: cconfig.Config) -> None:
        hdbg.dassert(cconfig.check_no_dummy_values(config))
