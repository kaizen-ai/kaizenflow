import logging

import pandas as pd
import sklearn.linear_model as slmode

import core.artificial_signal_generators as casgen
import core.config as ccfg
import helpers.unit_test as hut

from core.dataflow.core import DAG
from core.dataflow.nodes.sources import ReadDataFromDf
from core.dataflow.nodes.sklearn_models import ContinuousSkLearnModel

_LOG = logging.getLogger(__name__)


class TestContinuousSkLearnModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        pred_lag = 1
        # Load test data.
        data = self._get_data(pred_lag)
        data_source_node = ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        df_out = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(df_out.to_string())

    def test_fit_dag2(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        data_source_node = ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        df_out = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(df_out.to_string())

    def test_fit_dag3(self) -> None:
        """
        Test `slmode.Lasso` model.

        `Lasso` returns a one-dimensional array for a two-dimensional
        input.
        """
        pred_lag = 1
        # Load test data.
        data = self._get_data(pred_lag)
        data_source_node = ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Lasso,
            **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        df_out = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(df_out.to_string())

    def test_predict_dag1(self) -> None:
        pred_lag = 1
        # Load test data.
        data = self._get_data(pred_lag)
        fit_interval = ("1776-07-04 12:00:00", "2010-01-01 00:29:00")
        predict_interval = ("2010-01-01 00:30:00", "2100")
        data_source_node = ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        df_out = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(df_out.to_string())

    def test_predict_dag2(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        fit_interval = ("1776-07-04 12:00:00", "2010-01-01 00:29:00")
        predict_interval = ("2010-01-01 00:30:00", "2100")
        data_source_node = ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        df_out = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(df_out.to_string())

    def _get_config(self, steps_ahead: int) -> ccfg.Config:
        config = ccfg.Config()
        config["x_vars"] = ["x"]
        config["y_vars"] = ["y"]
        config["steps_ahead"] = steps_ahead
        config_kwargs = config.add_subconfig("model_kwargs")
        config_kwargs["alpha"] = 0.5
        return config

    def _get_data(self, lag: int) -> pd.DataFrame:
        """
        Generate "random returns".

        Use lag + noise as predictor.
        """
        num_periods = 50
        total_steps = num_periods + lag + 1
        rets = casgen.get_gaussian_walk(0, 0.2, total_steps, seed=10).diff()
        noise = casgen.get_gaussian_walk(0, 0.02, total_steps, seed=1).diff()
        pred = rets.shift(-lag).loc[1:num_periods] + noise.loc[1:num_periods]
        resp = rets.loc[1:num_periods]
        idx = pd.date_range("2010-01-01", periods=num_periods, freq="T")
        df = pd.DataFrame.from_dict({"x": pred, "y": resp}).set_index(idx)
        return df

