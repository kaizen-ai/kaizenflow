import logging
import pprint

import mxnet
import numpy as np
import pandas as pd
import sklearn.linear_model as slm

import core.artificial_signal_generators as sig_gen
import core.config as cfg
import core.dataflow as dtf
import core.signal_processing as sigp
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestContinuousSkLearnModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        pred_lag = 1
        # Load test data.
        data = self._get_data(pred_lag)
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = dtf.ContinuousSkLearnModel(
            "sklearn", model_func=slm.Ridge, **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        output_df = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_fit_dag2(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = dtf.ContinuousSkLearnModel(
            "sklearn", model_func=slm.Ridge, **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        output_df = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag1(self) -> None:
        pred_lag = 1
        # Load test data.
        data = self._get_data(pred_lag)
        fit_idxs = data.iloc[:30].index
        predict_idxs = data.iloc[30:].index
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_idxs(fit_idxs)
        data_source_node.set_predict_idxs(predict_idxs)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = dtf.ContinuousSkLearnModel(
            "sklearn", model_func=slm.Ridge, **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        output_df = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag2(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        fit_idxs = data.iloc[:30].index
        predict_idxs = data.iloc[30:].index
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_idxs(fit_idxs)
        data_source_node.set_predict_idxs(predict_idxs)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = dtf.ContinuousSkLearnModel(
            "sklearn", model_func=slm.Ridge, **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        output_df = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(output_df.to_string())

    def _get_config(self, steps_ahead: int) -> cfg.Config:
        config = cfg.Config()
        config["x_vars"] = ["x"]
        config["y_vars"] = ["y"]
        config["steps_ahead"] = steps_ahead
        config_kwargs = config.add_subconfig("model_kwargs")
        config_kwargs["alpha"] = 0.5
        return config

    def _get_data(self, lag: int) -> None:
        """
        Generate "random returns". Use lag + noise as predictor.
        """
        num_periods = 50
        total_steps = num_periods + lag + 1
        rets = sigp.get_gaussian_walk(0, 0.2, total_steps, seed=10).diff()
        noise = sigp.get_gaussian_walk(0, 0.02, total_steps, seed=1).diff()
        pred = rets.shift(-lag).loc[1:num_periods] + noise.loc[1:num_periods]
        resp = rets.loc[1:num_periods]
        idx = pd.date_range("2010-01-01", periods=num_periods, freq="T")
        df = pd.DataFrame.from_dict({"x": pred, "y": resp}).set_index(idx)
        return df


class TestContinuousDeepArModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        dag = self._get_dag()
        #
        output_df = dag.run_leq_node("deepar", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag1(self) -> None:
        dag = self._get_dag()
        #
        dag.run_leq_node("deepar", "fit")
        output_df = dag.run_leq_node("deepar", "predict")["df_out"]
        self.check_string(output_df.to_string())

    def _get_dag(self) -> dtf.DAG:
        mxnet.random.seed(0)
        data, _ = sig_gen.get_gluon_dataset(
            dataset_name="m4_hourly", train_length=100, test_length=1,
        )
        fit_idxs = data.iloc[:70].index
        predict_idxs = data.iloc[70:].index
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_idxs(fit_idxs)
        data_source_node.set_predict_idxs(predict_idxs)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load deepar config and create modeling node.
        config = cfg.Config()
        config["x_vars"] = None
        config["y_vars"] = ["y"]
        config["trainer_kwargs"] = {"epochs": 1}
        config["estimator_kwargs"] = {"prediction_length": 2}
        node = dtf.ContinuousDeepArModel("deepar", **config.to_dict(),)
        dag.add_node(node)
        dag.connect("data", "deepar")
        return dag


class TestDeepARGlobalModel(hut.TestCase):
    def test_fit1(self) -> None:
        mxnet.random.seed(0)
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
        mxnet.random.seed(0)
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
