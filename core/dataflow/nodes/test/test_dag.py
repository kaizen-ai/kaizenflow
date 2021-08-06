import logging
from typing import Any, Dict, List, Optional, cast

import pandas as pd

import core.config as cconfig
import core.dataflow.builders as cdtfb
import core.dataflow_source_nodes as dtfsn
import core.dataflow.core as cdtfc
import core.dataflow.nodes.transformers as cdtfnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# TODO(gp): -> test_dag_builer.py
class _NaivePipeline(cdtfb.DagBuilder):
    """
    Pipeline with:
    - a source node generating random data
    - a processing node pass-through
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Return a template configuration for this pipeline.

        :return: reference config
        """
        def _get_data() -> pd.DataFrame:
            """
            Generate random data.
            """
            num_cols = 2
            seed = 42
            date_range_kwargs = {
                "start": pd.Timestamp("2010-01-01"),
                "end": pd.Timestamp("2010-01-10"),
                "freq": "1B",
            }
            data = hut.get_random_df(num_cols, seed=seed, kwargs=date_range_kwargs)
            return data

        def _process_data(df_in: pd.DataFrame) -> pd.DataFrame:
            """
            Identity function.
            """
            return df_in

        dict_ = {
            # Get data.
            self._get_nid("get_data"): {
                "source_node_name": "DataLoader",
                "source_node_kwargs": {
                    "func": _get_data,
                }
            },
            # Process data.
            self._get_nid("process_data"): {
                "func": _process_data,
            }
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def get_dag(self, config: cconfig.Config, mode: str = "strict") -> cdtfc.DAG:
        """
        Generate pipeline DAG.

        :param config: config object used to configure DAG
        :param mode: same meaning as in `dtf.DAG`
        :return: initialized DAG
        """
        dag = cdtfc.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        # Get data.
        stage = "get_data"
        nid = self._get_nid(stage)
        node = dtfsn.data_source_node_factory(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Process data.
        stage = "process_data"
        nid = self._get_nid(stage)
        node = cdtfnt.FunctionWrapper(
            nid,
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
    def _append(dag: cdtfc.DAG, tail_nid: Optional[str], node: cdtfc.Node) -> str:
        dag.add_node(node)
        if tail_nid is not None:
            dag.connect(tail_nid, node.nid)
        return cast(str, node.nid)