import logging
import pprint
from typing import List, Optional, Tuple

import mxnet
import numpy as np
import pandas as pd
import pytest
import sklearn.decomposition as sld
import sklearn.linear_model as slm

import core.artificial_signal_generators as sig_gen
import core.config as cfg
import core.config_builders as cfgb
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
            "sklearn",
            model_func=slm.Ridge,
            **config.to_dict(),
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
            "sklearn",
            model_func=slm.Ridge,
            **config.to_dict(),
        )
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        output_df = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_fit_dag3(self) -> None:
        """
        Test `slm.Lasso` model.

        `Lasso` returns a one-dimensional array for a two-dimensional input.
        """
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
            "sklearn",
            model_func=slm.Lasso,
            **config.to_dict(),
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
        fit_interval = ("1776-07-04 12:00:00", "2010-01-01 00:29:00")
        predict_interval = ("2010-01-01 00:30:00", "2100")
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = dtf.ContinuousSkLearnModel(
            "sklearn",
            model_func=slm.Ridge,
            **config.to_dict(),
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
        fit_interval = ("1776-07-04 12:00:00", "2010-01-01 00:29:00")
        predict_interval = ("2010-01-01 00:30:00", "2100")
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = dtf.ContinuousSkLearnModel(
            "sklearn",
            model_func=slm.Ridge,
            **config.to_dict(),
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

    def _get_data(self, lag: int) -> pd.DataFrame:
        """
        Generate "random returns". Use lag + noise as predictor.
        """
        num_periods = 50
        total_steps = num_periods + lag + 1
        rets = sig_gen.get_gaussian_walk(0, 0.2, total_steps, seed=10).diff()
        noise = sig_gen.get_gaussian_walk(0, 0.02, total_steps, seed=1).diff()
        pred = rets.shift(-lag).loc[1:num_periods] + noise.loc[1:num_periods]
        resp = rets.loc[1:num_periods]
        idx = pd.date_range("2010-01-01", periods=num_periods, freq="T")
        df = pd.DataFrame.from_dict({"x": pred, "y": resp}).set_index(idx)
        return df


class TestUnsupervisedSkLearnModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = cfgb.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sld.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = dtf.UnsupervisedSkLearnModel("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        output_df = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        fit_interval = ("2000-01-03", "2000-01-31")
        predict_interval = ("2000-02-01", "2000-02-25")
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = cfgb.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sld.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = dtf.UnsupervisedSkLearnModel("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        output_df = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(output_df.to_string())

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        return realization


@pytest.mark.skip("Results differ on different environments")
class TestContinuousSarimaxModel(hut.TestCase):
    """
    Warning:
        SARIMAX can give slightly different outputs on different machines.
    """

    def test_fit1(self) -> None:
        data = self._get_data([], [])
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_fit_step_one1(self) -> None:
        """
        Fit on `x = y`.
        """
        data = self._get_data([1], [])
        data["x"] = data["ret_0"]
        config = self._get_config((1, 0, 0))
        config["steps_ahead"] = 1
        config["fit_kwargs"] = {"start_params": [0.9999, 0.0001, 1.57e-11]}
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_fit_with_constant1(self) -> None:
        data = self._get_data([1], [])
        config = self._get_config((1, 0, 0))
        config["steps_ahead"] = 2
        config["add_constant"] = True
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_fit_no_x1(self) -> None:
        """
        Fit without providing an exogenous variable.
        """
        data = self._get_data([], [])
        data.drop(columns=["x"], inplace=True)
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        config["x_vars"] = None
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_compare_to_linear_regression1(self) -> None:
        """
        Compare SARIMAX results to Linear Regression.
        """
        data = self._get_data([1], [])
        data.drop(columns=["x"], inplace=True)
        steps_ahead = 1
        # Train SkLearn model.
        sklearn_config = cfgb.get_config_from_nested_dict(
            {
                "model_func": slm.LinearRegression,
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": steps_ahead,
                "col_mode": "merge_all",
            }
        )
        sklearn_model = dtf.ContinuousSkLearnModel(
            "model", **sklearn_config.to_dict()
        )
        skl_out = sklearn_model.fit(data)["df_out"]
        skl_out.rename(columns=lambda x: "skl_" + x, inplace=True)
        # Train SARIMAX model.
        sarimax_config = self._get_config((1, 0, 0))
        sarimax_config["x_vars"] = None
        sarimax_config["steps_ahead"] = steps_ahead
        sarimax_model = dtf.ContinuousSarimaxModel(
            "model", **sarimax_config.to_dict()
        )
        sarimax_out = sarimax_model.fit(data)["df_out"]
        sarimax_out.rename(columns=lambda x: "sarimax_" + x, inplace=True)
        # Compare outputs.
        output_df = pd.concat([skl_out, sarimax_out], axis=1)
        output_df["skl_sarimax_pred_diff"] = (
            output_df["skl_ret_0_1_hat"] - output_df["sarimax_ret_0_1_hat"]
        )
        output_str = (
            f"{prnt.frame('sklearn_config')}\n{sklearn_config}\n"
            f"{prnt.frame('sarimax_config')}\n{sarimax_config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(output_df, index=True)}"
        )
        self.check_string(output_str)

    def test_compare_to_linear_regression2(self) -> None:
        """
        Compare SARIMAX results to Linear Regression for 3 steps ahead.
        """
        data = self._get_data([1], [])
        data.drop(columns=["x"], inplace=True)
        steps_ahead = 3
        # Train SkLearn model.
        sklearn_config = cfgb.get_config_from_nested_dict(
            {
                "model_func": slm.LinearRegression,
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": steps_ahead,
                "col_mode": "merge_all",
            }
        )
        sklearn_model = dtf.ContinuousSkLearnModel(
            "model", **sklearn_config.to_dict()
        )
        skl_out = sklearn_model.fit(data)["df_out"]
        skl_out.rename(columns=lambda x: "skl_" + x, inplace=True)
        # Train SARIMAX model.
        sarimax_config = self._get_config((1, 0, 0))
        sarimax_config["x_vars"] = None
        sarimax_config["steps_ahead"] = steps_ahead
        sarimax_model = dtf.ContinuousSarimaxModel(
            "model", **sarimax_config.to_dict()
        )
        sarimax_out = sarimax_model.fit(data)["df_out"]
        sarimax_out.rename(columns=lambda x: "sarimax_" + x, inplace=True)
        # Compare outputs.
        output_df = pd.concat([skl_out, sarimax_out], axis=1)
        output_df["skl_sarimax_pred_diff"] = (
            output_df["skl_ret_0_3_hat"] - output_df["sarimax_ret_0_3_hat"]
        )
        output_str = (
            f"{prnt.frame('sklearn_config')}\n{sklearn_config}\n"
            f"{prnt.frame('sarimax_config')}\n{sarimax_config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(output_df, index=True)}"
        )
        self.check_string(output_str)

    def test_predict1(self) -> None:
        data = self._get_data([], [])
        data_fit = data.iloc[:70]
        data_predict = data.iloc[70:]
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_predict2(self) -> None:
        """
        Test AR(1) process.
        """
        data = self._get_data([1], [])
        data_fit = data.iloc[:70]
        data_predict = data.iloc[70:]
        config = self._get_config((1, 0, 0))
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_predict_different_intervals1(self) -> None:
        """
        Verify that predictions on different intervals match.
        """
        data = self._get_data([1], [], periods=120, freq="D")
        config = self._get_config((1, 0, 0))
        data_fit = data.loc[:"2010-03-12"]
        data_predict1 = data.loc["2010-03-12":"2010-04-02"]
        data_predict2 = data.loc["2010-03-16":"2010-04-17"]
        data_predict3 = data.loc["2010-04-01":"2010-04-27"]
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out1 = csm.predict(data_predict1)["df_out"]
        df_out2 = csm.predict(data_predict2)["df_out"]
        df_out3 = csm.predict(data_predict3)["df_out"]
        #
        pd.testing.assert_series_equal(
            df_out1.loc["2010-03-25":"2010-03-29", "ret_0_3_hat"],
            df_out2.loc["2010-03-25":"2010-03-29", "ret_0_3_hat"],
        )
        pd.testing.assert_series_equal(
            df_out2.loc["2010-04-10":"2010-04-13", "ret_0_3_hat"],
            df_out3.loc["2010-04-10":"2010-04-13", "ret_0_3_hat"],
        )
        df_out = pd.concat(
            [df_out1, df_out2["ret_0_3_hat"], df_out3["ret_0_3_hat"]], axis=1
        )
        self.check_string(hut.convert_df_to_string(df_out, index=True))

    def test_predict_no_x1(self) -> None:
        """
        Predict without providing an exogenous variable.
        """
        data = self._get_data([], [])
        data.drop(columns=["x"], inplace=True)
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        config["x_vars"] = None
        data_fit = data.iloc[:70]
        data_predict = data.iloc[70:]
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        output_str = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(output_str)

    def test_predict_different_intervals_no_x1(self) -> None:
        """
        Verify that predictions on different intervals match.
        """
        data = self._get_data([1], [], periods=120, freq="D")
        data.drop(columns=["x"], inplace=True)
        config = self._get_config((1, 0, 0))
        config["x_vars"] = None
        data_fit = data.loc[:"2010-03-12"]
        data_predict1 = data.loc["2010-03-12":"2010-04-02"]
        data_predict2 = data.loc["2010-03-20":"2010-04-17"]
        data_predict3 = data.loc["2010-04-01":"2010-04-27"]
        csm = dtf.ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out1 = csm.predict(data_predict1)["df_out"]
        df_out2 = csm.predict(data_predict2)["df_out"]
        df_out3 = csm.predict(data_predict3)["df_out"]
        #
        pd.testing.assert_series_equal(
            df_out1.loc["2010-03-26":"2010-04-01", "ret_0_3_hat"],
            df_out2.loc["2010-03-26":"2010-04-01", "ret_0_3_hat"],
        )
        pd.testing.assert_series_equal(
            df_out2.loc["2010-04-07":"2010-04-16", "ret_0_3_hat"],
            df_out3.loc["2010-04-07":"2010-04-16", "ret_0_3_hat"],
        )

    @staticmethod
    def _get_data(
        ar_coeffs: List[int],
        ma_coeffs: List[int],
        periods: int = 100,
        freq: str = "M",
        seed: int = 42,
    ) -> pd.DataFrame:
        arma_process = sig_gen.ArmaProcess(ar_coeffs, ma_coeffs)
        date_range_kwargs = {
            "start": "2010-01-01",
            "periods": periods,
            "freq": freq,
        }
        y = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, scale=0.1, seed=seed
        ).rename("ret_0")
        x = sigp.compute_smooth_moving_average(y, 26).rename("x")
        return pd.concat([x, y], axis=1)

    @staticmethod
    def _get_config(
        order: Tuple[int, int, int],
        seasonal_order: Optional[Tuple[int, int, int, int]] = None,
    ) -> cfg.Config:
        config = cfgb.get_config_from_nested_dict(
            {
                "y_vars": ["ret_0"],
                "steps_ahead": 3,
                "init_kwargs": {
                    "order": order,
                    "seasonal_order": seasonal_order,
                },
                "x_vars": ["x"],
                "nan_mode": "drop",
            }
        )
        return config


class TestResidualizer(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = cfgb.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sld.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = dtf.Residualizer("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        output_df = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        fit_interval = ("2000-01-03", "2000-01-31")
        predict_interval = ("2000-02-01", "2000-02-25")
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = cfgb.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sld.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = dtf.Residualizer("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        dag.run_leq_node("sklearn", "fit")
        output_df = dag.run_leq_node("sklearn", "predict")["df_out"]
        self.check_string(output_df.to_string())

    def _get_data(self) -> pd.DataFrame:
        """
        Generate multivariate normal returns.
        """
        mn_process = sig_gen.MultivariateNormalProcess()
        mn_process.set_cov_from_inv_wishart_draw(dim=4, seed=0)
        realization = mn_process.generate_sample(
            {"start": "2000-01-01", "periods": 40, "freq": "B"}, seed=0
        )
        return realization


class TestSmaModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = dtf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        output_df = dag.run_leq_node("sma", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_fit_dag2(self) -> None:
        """
        Specify `tau` parameter.
        """
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["tau"] = 8
        config["nan_mode"] = "drop"
        node = dtf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        output_df = dag.run_leq_node("sma", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = dtf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        dag.run_leq_node("sma", "fit")
        output_df = dag.run_leq_node("sma", "predict")["df_out"]
        self.check_string(output_df.to_string())

    @staticmethod
    def _get_data() -> pd.DataFrame:
        """
        Generate "random returns". Use lag + noise as predictor.
        """
        arma_process = sig_gen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2000-01-01", "periods": 40, "freq": "B"}
        date_range = pd.date_range(**date_range_kwargs)
        realization = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=0
        )
        vol = np.abs(realization) ** 2
        vol.name = "vol"
        df = pd.DataFrame(index=date_range, data=vol)
        return df


class TestVolatilityModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = dtf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        output_df = dag.run_leq_node("vol_model", "fit")["df_out"]
        self.check_string(output_df.to_string())

    def test_fit_dag_correctness1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = dtf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = dtf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        # Z-score.
        zscore_df = dag.run_leq_node("vol_model", "fit")["df_out"]
        # Invert z-scoring.
        ret_0_vol_0_hat = zscore_df["ret_0_vol_2_hat"].shift(2)
        inverted_rets = (ret_0_vol_0_hat * zscore_df["ret_0_zscored"]).rename(
            "ret_0_inverted"
        )
        # Compare results.
        output_df = zscore_df.join(inverted_rets)
        self.check_string(hut.convert_df_to_string(output_df, index=True))

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = dtf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        dag.run_leq_node("vol_model", "fit")
        output_df = dag.run_leq_node("vol_model", "predict")["df_out"]
        self.check_string(output_df.to_string())

    def test_predict_dag_correctness1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = dtf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = dtf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = cfg.Config()
        config["col"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = dtf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        dag.run_leq_node("vol_model", "fit")
        zscore_df = dag.run_leq_node("vol_model", "predict")["df_out"]
        # Invert z-scoring.
        ret_0_vol_0_hat = zscore_df["ret_0_vol_2_hat"].shift(2)
        inverted_rets = (ret_0_vol_0_hat * zscore_df["ret_0_zscored"]).rename(
            "ret_0_inverted"
        )
        # Compare results.
        output_df = zscore_df.join(inverted_rets)
        self.check_string(hut.convert_df_to_string(output_df, index=True))

    @staticmethod
    def _get_data() -> pd.DataFrame:
        """
        Generate "random returns". Use lag + noise as predictor.
        """
        arma_process = sig_gen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2000-01-01", "periods": 40, "freq": "B"}
        date_range = pd.date_range(**date_range_kwargs)
        realization = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=0
        )
        realization.name = "ret_0"
        df = pd.DataFrame(index=date_range, data=realization)
        return df


if True:

    class TestContinuousDeepArModel(hut.TestCase):
        @pytest.mark.skip("Disabled because of PartTask2440")
        def test_fit_dag1(self) -> None:
            dag = self._get_dag()
            #
            output_df = dag.run_leq_node("deepar", "fit")["df_out"]
            self.check_string(output_df.to_string())

        @pytest.mark.skip("Disabled because of PartTask2440")
        def test_predict_dag1(self) -> None:
            dag = self._get_dag()
            #
            dag.run_leq_node("deepar", "fit")
            output_df = dag.run_leq_node("deepar", "predict")["df_out"]
            self.check_string(output_df.to_string())

        def _get_dag(self) -> dtf.DAG:
            mxnet.random.seed(0)
            data, _ = sig_gen.get_gluon_dataset(
                dataset_name="m4_hourly",
                train_length=100,
                test_length=1,
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
            node = dtf.ContinuousDeepArModel(
                "deepar",
                **config.to_dict(),
            )
            dag.add_node(node)
            dag.connect("data", "deepar")
            return dag

    class TestDeepARGlobalModel(hut.TestCase):
        @pytest.mark.skip("Disabled because of PartTask2440")
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
            output_shape = {
                str(key): str(val.shape) for key, val in output.items()
            }
            config_info_output = (
                f"{prnt.frame('config')}\n{config}\n"
                f"{prnt.frame('info')}\n{pprint.pformat(info)}\n"
                f"{prnt.frame('output')}\n{str_output}\n"
                f"{prnt.frame('output_shape')}\n{output_shape}\n"
            )
            self.check_string(config_info_output)

        @pytest.mark.skip("Disabled because of PartTask2440")
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
            config["estimator_kwargs"] = {
                "freq": "T",
                "use_feat_dynamic_real": False,
            }
            config["x_vars"] = self._x_vars
            config["y_vars"] = self._y_vars
            return config
