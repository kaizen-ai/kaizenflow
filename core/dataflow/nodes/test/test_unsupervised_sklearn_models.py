import logging

import pandas as pd
import sklearn.decomposition as sdecom

import core.artificial_signal_generators as casgen
import core.config_builders as ccbuild
import core.dataflow.nodes.test.helpers as cdnth
import helpers.unit_test as hut
from core.dataflow.nodes.unsupervised_sklearn_models import (
    MultiindexUnsupervisedSkLearnModel,
    Residualizer,
    UnsupervisedSkLearnModel,
)

_LOG = logging.getLogger(__name__)


class TestUnsupervisedSkLearnModel(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
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
        df_str = hut.convert_df_to_string(df_out.round(3), index=True)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` after `fit()`.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = UnsupervisedSkLearnModel("sklearn", **config.to_dict())
        node.fit(data.loc["2000-01-03":"2000-01-31"])
        # Predict.
        df_out = node.predict(data.loc["2000-02-01":"2000-02-25"])["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True)
        self.check_string(df_str)

    def test3(self) -> None:
        """
        Test `get_fit_state()` and `set_fit_state()`
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        fit_df = data.loc["2000-01-03":"2000-01-31"]
        predict_df = data.loc["2000-02-01":"2000-02-25"]
        expected, actual = cdnth.test_get_set_state(
            fit_df, predict_df, config, UnsupervisedSkLearnModel
        )
        self.assert_equal(actual, expected)

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


class TestMultiindexUnsupervisedSkLearnModel(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("ret_0",),
                "out_col_group": ("pca",),
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = MultiindexUnsupervisedSkLearnModel("sklearn", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` after `fit()`.
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("ret_0",),
                "out_col_group": ("pca",),
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = MultiindexUnsupervisedSkLearnModel("sklearn", **config.to_dict())
        node.fit(data.loc["2000-01-03":"2000-01-31"])
        # Predict.
        df_out = node.predict(data.loc["2000-02-01":"2000-02-25"])["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True)
        self.check_string(df_str)

    def test3(self) -> None:
        """
        Test `get_fit_state()` and `set_fit_state()`
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("ret_0",),
                "out_col_group": ("pca",),
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        fit_df = data.loc["2000-01-03":"2000-01-31"]
        predict_df = data.loc["2000-02-01":"2000-02-25"]
        expected, actual = cdnth.test_get_set_state(
            fit_df, predict_df, config, MultiindexUnsupervisedSkLearnModel
        )
        self.assert_equal(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["ret_0", "volume"])
        return data


class TestResidualizer(hut.TestCase):
    def test1(self) -> None:
        """
        Test `fit()` call.
        """
        # Load test data.
        data = self._get_data()
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("ret_0",),
                "out_col_group": ("residual",),
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = Residualizer("sklearn", **config.to_dict())
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True)
        self.check_string(df_str)

    def test2(self) -> None:
        """
        Test `predict()` after `fit()`.
        """
        # Load test data.
        data = self._get_data()
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("ret_0",),
                "out_col_group": ("residual",),
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = Residualizer("sklearn", **config.to_dict())
        node.fit(data.loc["2000-01-03":"2000-01-31"])
        # Predict.
        df_out = node.predict(data.loc["2000-02-01":"2000-02-25"])["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True)
        self.check_string(df_str)

    def test3(self) -> None:
        """
        Test `get_fit_state()` and `set_fit_state()`
        """
        data = self._get_data()
        config = ccbuild.get_config_from_nested_dict(
            {
                "in_col_group": ("ret_0",),
                "out_col_group": ("residual",),
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        fit_df = data.loc["2000-01-03":"2000-01-31"]
        predict_df = data.loc["2000-02-01":"2000-02-25"]
        expected, actual = cdnth.test_get_set_state(
            fit_df, predict_df, config, Residualizer
        )
        self.assert_equal(actual, expected)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["ret_0", "volume"])
        return data

