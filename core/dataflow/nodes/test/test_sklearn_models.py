import logging

import pandas as pd
import sklearn.linear_model as slmode

import core.artificial_signal_generators as casgen
import core.config as cconfig
import core.dataflow.nodes.sklearn_models as cdnsm
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestContinuousSkLearnModel(hut.TestCase):
    def test1(self) -> None:
        # Load test data.
        data = self._get_data(1)
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
                "col_mode": "merge_all",
            }
        )
        # Load sklearn config and create modeling node.
        node = cdnsm.ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        data = self._get_data(2)
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 2,
                "model_kwargs": {
                    "alpha": 0.5,
                },
                "col_mode": "merge_all",
            }
        )
        node = cdnsm.ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test3(self) -> None:
        """
        Test `slmode.Lasso` model.

        `Lasso` returns a one-dimensional array for a two-dimensional
        input.
        """
        data = self._get_data(1)
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
            }
        )
        node = cdnsm.ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Lasso,
            **config.to_dict(),
        )
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test4(self) -> None:
        data = self._get_data(1)
        data_fit = data.loc[:"2010-01-01 00:29:00"]
        data_predict = data.loc["2010-01-01 00:30:00":]
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
                "col_mode": "merge_all",
            }
        )
        node = cdnsm.ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        node.fit(data_fit)
        df_out = node.predict(data_predict)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test5(self) -> None:
        data = self._get_data(2)
        data_fit = data.loc[:"2010-01-01 00:29:00"]
        data_predict = data.loc["2010-01-01 00:30:00":]
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 2,
                "model_kwargs": {
                    "alpha": 0.5,
                },
            }
        )
        node = cdnsm.ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        node.fit(data_fit)
        df_out = node.predict(data_predict)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test6(self) -> None:
        """
        Use `sample_weight_col` in `fit()`.
        """
        data = self._get_data(2)
        data["weights"] = range(0, data.shape[0])
        data_fit = data.loc[:"2010-01-01 00:29:00"]
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 2,
                "sample_weight_col": ["weights"],
            }
        )
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.LinearRegression,
            **config.to_dict(),
        )
        df_out = node.fit(data_fit)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test7(self) -> None:
        """
        Use `sample_weight_col` in `predict()`.
        """
        data = self._get_data(2)
        data["weights"] = range(0, data.shape[0])
        data_fit = data.loc[:"2010-01-01 00:29:00"]
        data_predict = data.loc["2010-01-01 00:30:00":]
        config = cconfig.get_config_from_nested_dict(
            {
                "x_vars": ["x"],
                "y_vars": ["y"],
                "steps_ahead": 2,
                "sample_weight_col": ["weights"],
            }
        )
        node = ContinuousSkLearnModel(
            "sklearn",
            model_func=slmode.LinearRegression,
            **config.to_dict(),
        )
        node.fit(data_fit)["df_out"]
        df_out = node.predict(data_predict)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

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


class TestMultiindexSkLearnModel(hut.TestCase):
    def test1(self) -> None:
        # Load test data.
        data = self._get_data()
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_groups": [
                    ("ret_0",),
                ],
                "out_col_group": (),
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
            }
        )
        # Load sklearn config and create modeling node.
        node = cdnsm.MultiindexSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        data = self._get_data()
        data_fit = data.loc[:"2000-01-31"]
        data_predict = data.loc["2000-01-31":]
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_groups": [
                    ("ret_0",),
                ],
                "out_col_group": (),
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
            }
        )
        node = cdnsm.MultiindexSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        node.fit(data_fit)
        df_out = node.predict(data_predict)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=2, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["ret_0", "volume"])
        return data


class TestMultiindexPooledSkLearnModel(hut.TestCase):
    def test1(self) -> None:
        # Load test data.
        data = self._get_data()
        # Generate node config.
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_groups": [
                    ("ret_0",),
                ],
                "out_col_group": (),
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
            }
        )
        # Load sklearn config and create modeling node.
        node = cdnsm.MultiindexPooledSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        #
        df_out = node.fit(data)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def test2(self) -> None:
        data = self._get_data()
        data_fit = data.loc[:"2000-01-31"]
        data_predict = data.loc["2000-01-31":]
        config = cconfig.get_config_from_nested_dict(
            {
                "in_col_groups": [
                    ("ret_0",),
                ],
                "out_col_group": (),
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": 1,
                "model_kwargs": {
                    "alpha": 0.5,
                },
            }
        )
        node = cdnsm.MultiindexPooledSkLearnModel(
            "sklearn",
            model_func=slmode.Ridge,
            **config.to_dict(),
        )
        node.fit(data_fit)
        df_out = node.predict(data_predict)["df_out"]
        df_str = hut.convert_df_to_string(df_out.round(3), index=True, decimals=3)
        self.check_string(df_str)

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = casgen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=2, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        realization = realization.rename(columns=lambda x: "MN" + str(x))
        volume = pd.DataFrame(
            index=realization.index, columns=realization.columns, data=100
        )
        data = pd.concat([realization, volume], axis=1, keys=["ret_0", "volume"])
        return data
