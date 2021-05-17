import logging
import pprint

import mxnet
import numpy as np
import pandas as pd

import core.artificial_signal_generators as casgen
import core.config as ccfg
import helpers.printing as hprint
import helpers.unit_test as hut
from core.dataflow.core import DAG
from core.dataflow.nodes.gluonts_models import (
    ContinuousDeepArModel,
    DeepARGlobalModel,
)
from core.dataflow.nodes.sources import ReadDataFromDf

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test gluon-ts - DeepAR
# #############################################################################


if True:

    class TestContinuousDeepArModel(hut.TestCase):
        def test_fit_dag1(self) -> None:
            dag = self._get_dag()
            #
            df_out = dag.run_leq_node("deepar", "fit")["df_out"]
            self.check_string(df_out.to_string())

        def test_predict_dag1(self) -> None:
            dag = self._get_dag()
            #
            dag.run_leq_node("deepar", "fit")
            df_out = dag.run_leq_node("deepar", "predict")["df_out"]
            self.check_string(df_out.to_string())

        def _get_dag(self) -> DAG:
            mxnet.random.seed(0)
            data, _ = casgen.get_gluon_dataset(
                dataset_name="m4_hourly",
                train_length=100,
                test_length=1,
            )
            fit_intervals = [("1750-01-01 00:00:00", "1750-01-03 21:00:00")]
            predict_intervals = [("1750-01-03 21:00:00", None)]
            data_source_node = ReadDataFromDf("data", data)
            data_source_node.set_fit_intervals(fit_intervals)
            data_source_node.set_predict_intervals(predict_intervals)
            # Create DAG and test data node.
            dag = DAG(mode="strict")
            dag.add_node(data_source_node)
            # Load deepar config and create modeling node.
            config = ccfg.Config()
            config["x_vars"] = None
            config["y_vars"] = ["y"]
            config["trainer_kwargs"] = {"epochs": 1}
            config["estimator_kwargs"] = {"prediction_length": 2}
            node = ContinuousDeepArModel(
                "deepar",
                **config.to_dict(),
            )
            dag.add_node(node)
            dag.connect("data", "deepar")
            return dag

    class TestDeepARGlobalModel(hut.TestCase):
        def test_fit1(self) -> None:
            mxnet.random.seed(0)
            local_ts = self._get_local_ts()
            num_entries = 100
            config = self._get_config()
            deepar = DeepARGlobalModel(**config.to_dict())
            output = deepar.fit(local_ts)
            info = deepar.get_info("fit")
            str_output = "\n".join(
                [
                    f"{key}:\n{val.head(num_entries).to_string()}"
                    for key, val in output.items()
                ]
            )
            output_shape = {
                str(key): str(val.shape) for key, val in output.items()
            }
            config_info_output = (
                f"{hprint.frame('config')}\n{config}\n"
                f"{hprint.frame('info')}\n{pprint.pformat(info)}\n"
                f"{hprint.frame('output')}\n{str_output}\n"
                f"{hprint.frame('output_shape')}\n{output_shape}\n"
            )
            self.check_string(config_info_output)

        def test_fit_dag1(self) -> None:
            mxnet.random.seed(0)
            dag = DAG(mode="strict")
            local_ts = self._get_local_ts()
            data_source_node = ReadDataFromDf("local_ts", local_ts)
            dag.add_node(data_source_node)
            config = self._get_config()
            deepar = DeepARGlobalModel(**config.to_dict())
            dag.add_node(deepar)
            dag.connect("local_ts", "deepar")
            df_out = dag.run_leq_node("deepar", "fit")["df_out"]
            expected_shape = (self._n_periods * (self._grid_len - 1), 1)
            self.assertEqual(df_out.shape, expected_shape)
            self.check_string(df_out.to_string())

        def _get_local_ts(self) -> pd.DataFrame:
            """
            Generate a dataframe of the following format:

            EVENT_SENTIMENT_SCORE           zret_0         0 2010-01-01
            00:00:00           0.496714 -0.138264 2010-01-01 00:01:00
            0.647689  1.523030 2010-01-01 00:02:00          -0.234153
            -0.234137 2010-01-01 00:03:00           1.579213  0.767435
            2010-01-01 00:04:00          -0.469474  0.542560
            """
            np.random.seed(42)
            self._n_periods = 10
            self._grid_len = 3
            grid_idx = range(self._grid_len)
            idx = pd.date_range("2010-01-01", periods=self._n_periods, freq="T")
            idx = pd.MultiIndex.from_product([grid_idx, idx])
            local_ts = pd.DataFrame(
                np.random.randn(self._n_periods * self._grid_len, 2), index=idx
            )
            self._x_vars = ["EVENT_SENTIMENT_SCORE"]
            self._y_vars = ["zret_0"]
            local_ts.columns = self._x_vars + self._y_vars
            return local_ts

        def _get_config(self) -> ccfg.Config:
            config = ccfg.Config()
            config["nid"] = "deepar"
            config["trainer_kwargs"] = {"epochs": 1}
            config["estimator_kwargs"] = {
                "freq": "T",
                "use_feat_dynamic_real": False,
            }
            config["x_vars"] = self._x_vars
            config["y_vars"] = self._y_vars
            return config
