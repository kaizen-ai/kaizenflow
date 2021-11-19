"""
Import as:

import dataflow_amp.price.pipeline as dtfamprpip
"""

import datetime
import logging

import core.config as cconfig
import core.dataflow as dtf
import core.dataflow_source_nodes as cdtfsonod
import core.finance as cofinanc
import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


class PricePipeline(dtf.DagBuilder):
    """
    Pipeline for processing prices.
    """

    def get_config_template(self) -> cconfig.Config:
        """
        Return a template configuration for this pipeline.

        :return: reference config
        """
        dict_ = {
            # Load prices.
            # NOTE: The caller needs to inject config values to control the
            # `data_source_node_factory` node in order to create the proper data
            # node.
            self._get_nid("load_prices"): {
                cconfig.DUMMY: None,
            },
            self._get_nid("process_bid_ask"): {
                "func_kwargs": {
                    "bid_col": cconfig.DUMMY,
                    "ask_col": cconfig.DUMMY,
                },
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
                    "price_cols": cconfig.DUMMY,
                    "volume_cols": cconfig.DUMMY,
                },
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    @staticmethod
    def validate_config(config: cconfig.Config) -> None:
        """
        Sanity-check config.

        :param config: config object to validate
        """
        hdbg.dassert(cconfig.check_no_dummy_values(config))

    def _get_dag(self, config: cconfig.Config, mode: str = "strict") -> dtf.DAG:
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
        node = cdtfsonod.data_source_node_factory(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Process bid/ask.
        stage = "process_bid_ask"
        nid = self._get_nid(stage)
        node = dtf.FunctionWrapper(
            nid, func=cofinanc.process_bid_ask, **config[nid].to_dict()
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Set weekends to NaN.
        stage = "filter_weekends"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_weekends_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Set non-ATH to NaN.
        stage = "filter_ath"
        nid = self._get_nid(stage)
        node = dtf.ColumnTransformer(
            nid,
            transformer_func=cofinanc.set_non_ath_to_nan,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Resample.
        stage = "resample_prices_to_1min"
        nid = self._get_nid(stage)
        node = dtf.FunctionWrapper(
            nid, func=cofinanc.resample_time_bars, **config[nid].to_dict()
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag
