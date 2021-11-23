import logging

import core.dataflow as dtf
import helpers.printing as hprint
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)

import logging

import pandas as pd

import core.config as cconfig
import core.dataflow.builders as cdtfbuil
import core.dataflow.core as cdtfcore
import core.dataflow.nodes.transformers as cdtfnotra
import core.dataflow_source_nodes as cdtfsonod
import helpers.unit_test as hunitest

_LOG = logging.getLogger(__name__)


class _NaivePipeline(cdtfbuil.DagBuilder):
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
            data = hunitest.get_random_df(
                num_cols, seed=seed, date_range_kwargs=date_range_kwargs
            )
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
                },
            },
            # Process data.
            self._get_nid("process_data"): {
                "func": _process_data,
            },
        }
        config = cconfig.get_config_from_nested_dict(dict_)
        return config

    def _get_dag(
        self, config: cconfig.Config, mode: str = "strict"
    ) -> cdtfcore.DAG:
        """
        Generate pipeline DAG.

        :param config: config object used to configure DAG
        :param mode: same meaning as in `dtf.DAG`
        :return: initialized DAG
        """
        dag = cdtfcore.DAG(mode=mode)
        _LOG.debug("%s", config)
        tail_nid = None
        # Get data.
        stage = "get_data"
        nid = self._get_nid(stage)
        node = cdtfsonod.data_source_node_factory(nid, **config[nid].to_dict())
        tail_nid = self._append(dag, tail_nid, node)
        # Process data.
        stage = "process_data"
        nid = self._get_nid(stage)
        node = cdtfnotra.FunctionWrapper(
            nid,
            **config[nid].to_dict(),
        )
        tail_nid = self._append(dag, tail_nid, node)
        #
        _ = tail_nid
        return dag


# #############################################################################


class TestArmaReturnsBuilder(hunitest.TestCase):
    """
    Test the ArmaReturnsBuilder pipeline.
    """

    def test1(self) -> None:
        dag_builder = dtf.ArmaReturnsBuilder()
        config = dag_builder.get_config_template()
        dag_runner = dtf.FitPredictDagRunner(config, dag_builder)
        result_bundle = dag_runner.fit()
        df_out = result_bundle.result_df
        str_output = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_out')}\n{hunitest.convert_df_to_string(df_out, index=True)}\n"
        )
        self.check_string(str_output)
