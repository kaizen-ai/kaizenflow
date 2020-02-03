import logging
import pprint

import numpy as np
import pandas as pd

import core.config as cfg
import core.dataflow as dtf
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestDeepARGlobalModel(hut.TestCase):
    def test_fit1(self) -> None:
        local_ts = self._get_local_ts()
        num_entries = 100
        config = self._get_config()
        deepar = dtf.DeepARGlobalModel(**config.to_dict())
        output = deepar.fit(local_ts)
        info = deepar.get_info("fit")
        str_output = "\n".join(
            [
                f"{key}:\n{val.head(num_entries).to_string()}"
                for key, val in output.items()
            ]
        )
        output_shape = {str(key): str(val.shape) for key, val in output.items()}
        config_info_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('info')}\n{pprint.pformat(info)}\n"
            f"{prnt.frame('output')}\n{str_output}\n"
            f"{prnt.frame('output_shape')}\n{output_shape}\n"
        )
        self.check_string(config_info_output)

    def test_fit_dag1(self) -> None:
        dag = dtf.DAG(mode="strict")
        local_ts = self._get_local_ts()
        data_source_node = dtf.ReadDataFromDf("local_ts", local_ts)
        dag.add_node(data_source_node)
        config = self._get_config()
        deepar = dtf.DeepARGlobalModel(**config.to_dict())
        dag.add_node(deepar)
        dag.connect("local_ts", "deepar")
        output_df = dag.run_leq_node("deepar", "fit")["df_out"]
        expected_shape = (self._n_periods * (self._grid_len - 1), 1)
        self.assertEqual(output_df.shape, expected_shape)
        self.check_string(output_df.to_string())

    def _get_local_ts(self) -> pd.DataFrame:
        """
        Generate a dataframe of the following format:

                              EVENT_SENTIMENT_SCORE    zret_0
        0 2010-01-01 00:00:00               0.496714 -0.138264
          2010-01-01 00:01:00               0.647689  1.523030
          2010-01-01 00:02:00              -0.234153 -0.234137
          2010-01-01 00:03:00               1.579213  0.767435
          2010-01-01 00:04:00              -0.469474  0.542560
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

    def _get_config(self) -> cfg.Config:
        config = cfg.Config()
        config["nid"] = "deepar"
        config["trainer_kwargs"] = {"epochs": 1}
        config["estimator_kwargs"] = {"freq": "T", "use_feat_dynamic_real": False}
        config["x_vars"] = self._x_vars
        config["y_vars"] = self._y_vars
        return config
