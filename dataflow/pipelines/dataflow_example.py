"""
Import as:

import dataflow.pipelines.dataflow_example as dtfpidtfexa
"""

# TODO(gp): -> pipelines/examples/pipeline2.py

import logging

import pandas as pd

import core.config as cconfig
import dataflow.core as dtfcore
import dataflow.system as dtfsys
import helpers.hunit_test as hunitest

_LOG = logging.getLogger(__name__)


# TODO(gp): -> ExamplePipeline2_DagBuilder(dtfcore.DagBuilder):
class _NaivePipeline(dtfcore.DagBuilder):
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

        # TODO(gp): Move out of this function.
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
            data = hunitest.get_random_df(
                num_cols, seed=seed, date_range_kwargs=date_range_kwargs
            )
            # This needs to be multi-index.
            data = pd.concat([data, data], axis=1, keys=["stock1", "stock2"])
            data = data.swaplevel(i=0, j=1, axis=1)
            data.sort_index(axis=1, level=0, inplace=True)
            return data

        def _process_data(df_in: pd.DataFrame) -> pd.DataFrame:
            """
            Identity function.
            """
            return df_in

        dict_ = {
            # Get data.
            self._get_nid("get_data"): {
                "source_node_name": "FunctionDataSource",
                "source_node_kwargs": {
                    "func": _get_data,
                },
            },
            # Process data.
            self._get_nid("process_data"): {
                "func": _process_data,
            },
            # Place trades.
            self._get_nid("process_forecasts"): {
                "prediction_col": "price",
                "volatility_col": "price",
                "process_forecasts_config": {},
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> dtfcore.DAG:
        """
        Generate pipeline DAG.

        :param config: config object used to configure DAG
        :param mode: same meaning as in `dtfcore.DAG`
        :return: initialized DAG
        """
        dag = dtfcore.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        # Get data.
        stage = "get_data"
        nid = self._get_nid(stage)
        node = dtfsys.data_source_node_factory(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Process data.
        stage = "process_data"
        nid = self._get_nid(stage)
        node = dtfcore.FunctionWrapper(
            nid,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        # Process forecasts.
        stage = "process_forecasts"
        nid = self._get_nid(stage)
        node = dtfsys.ProcessForecasts(
            nid,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag
