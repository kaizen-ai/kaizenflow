import logging

import pandas as pd
import sklearn.decomposition as sdecom

import core.artificial_signal_generators as casgen
import core.config_builders as ccbuild
import helpers.unit_test as hut
from core.dataflow.core import DAG
from core.dataflow.nodes.sources import ReadDataFromDf
from core.dataflow.nodes.unsupervised_sklearn_models import (
    Residualizer,
    UnsupervisedSkLearnModel,
)

_LOG = logging.getLogger(__name__)


class TestUnsupervisedSkLearnModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        # Create sklearn config and modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = UnsupervisedSkLearnModel("sklearn", **config.to_dict())
        # Fit model.
        df_out = node.fit(data)["df_out"]
        self.check_string(df_out.to_string())

    def test_predict_dag1(self) -> None:
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = UnsupervisedSkLearnModel("sklearn", **config.to_dict())
        node.fit(data.loc["2000-01-03": "2000-01-31"])
        # Predict.
        df_out = node.predict(data.loc["2000-02-01":"2000-02-25"])["df_out"]
        self.check_string(df_out.to_string())

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        return realization


class TestResidualizer(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = Residualizer("sklearn", **config.to_dict())
        #
        df_out = node.fit(data)["df_out"]
        self.check_string(df_out.to_string())

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = ReadDataFromDf("data", data)
        fit_interval = ("2000-01-03", "2000-01-31")
        predict_interval = ("2000-02-01", "2000-02-25")
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = Residualizer("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        df_out = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(df_out.to_string())

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        return realization
