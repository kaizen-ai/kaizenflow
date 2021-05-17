"""
Import as:

import core.dataflow.test.test_models as dttmod
"""

import collections
import logging
import pprint
from typing import Any, Dict, List, Optional, Tuple

import mxnet
import numpy as np
import pandas as pd
import pytest
import sklearn.decomposition as sdecom
import sklearn.linear_model as slmode

import core.artificial_signal_generators as casgen
import core.config as ccfg
import core.config_builders as ccbuild
import core.dataflow as cdataf
import core.signal_processing as csproc
import helpers.dbg as dbg
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test sklearn - supervised prediction models
# #############################################################################


class TestContinuousSkLearnModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        pred_lag = 1
        # Load test data.
        data = self._get_data(pred_lag)
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = cdataf.ContinuousSkLearnModel(
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
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = cdataf.ContinuousSkLearnModel(
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
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = cdataf.ContinuousSkLearnModel(
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
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = cdataf.ContinuousSkLearnModel(
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
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = self._get_config(pred_lag)
        node = cdataf.ContinuousSkLearnModel(
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


# #############################################################################
# Test sklearn - unsupervised models
# #############################################################################


class TestUnsupervisedSkLearnModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = cdataf.UnsupervisedSkLearnModel("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        df_out = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(df_out.to_string())

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        fit_interval = ("2000-01-03", "2000-01-31")
        predict_interval = ("2000-02-01", "2000-02-25")
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = cdataf.UnsupervisedSkLearnModel("sklearn", **config.to_dict())
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


class TestResidualizer(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = cdataf.Residualizer("sklearn", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sklearn")
        #
        df_out = dag.run_leq_node("sklearn", "fit")["df_out"]
        self.check_string(df_out.to_string())

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        fit_interval = ("2000-01-03", "2000-01-31")
        predict_interval = ("2000-02-01", "2000-02-25")
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Load sklearn config and create modeling node.
        config = ccbuild.get_config_from_nested_dict(
            {
                "x_vars": [0, 1, 2, 3],
                "model_func": sdecom.PCA,
                "model_kwargs": {"n_components": 2},
            }
        )
        node = cdataf.Residualizer("sklearn", **config.to_dict())
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


# #############################################################################
# Test volatility modeling
# #############################################################################


class TestSmaModel(hut.TestCase):
    def test_fit_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        df_out = dag.run_leq_node("sma", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        self._check_results(config, info, df_out)

    def test_fit_dag2(self) -> None:
        """
        Specify `tau` parameter.
        """
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["tau"] = 8
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        df_out = dag.run_leq_node("sma", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        self._check_results(config, info, df_out)

    def test_fit_dag3(self) -> None:
        """
        Specify `col_mode=='merge_all'`.
        """
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["col_mode"] = "merge_all"
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        df_out = dag.run_leq_node("sma", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        self._check_results(config, info, df_out)

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["col"] = ["vol"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        node = cdataf.SmaModel("sma", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "sma")
        #
        dag.run_leq_node("sma", "fit")
        df_out = dag.run_leq_node("sma", "predict")["df_out"]
        info = collections.OrderedDict()
        info["fit"] = cdataf.extract_info(dag, ["fit"])
        info["predict"] = cdataf.extract_info(dag, ["predict"])
        # Package results.
        self._check_results(config, info, df_out)

    def _check_results(
        self,
        config: ccfg.Config,
        info: collections.OrderedDict,
        df_out: pd.DataFrame,
    ) -> None:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act.append(str(ccbuild.get_config_from_nested_dict(info)))
        act.append(hprint.frame("df_out"))
        act.append(hut.convert_df_to_string(df_out, index=True, decimals=2))
        act = "\n".join(act)
        self.check_string(act)

    @staticmethod
    def _get_data() -> pd.DataFrame:
        """
        Generate "random returns".
        """
        arma_process = casgen.ArmaProcess([0.45], [0])
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
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_fit_dag_correctness1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
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
        df_out = zscore_df.join(inverted_rets)
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_predict_dag1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        dag.run_leq_node("vol_model", "fit")
        df_out = dag.run_leq_node("vol_model", "predict")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_predict_dag_correctness1(self) -> None:
        # Load test data.
        data = self._get_data()
        fit_interval = ("2000-01-01", "2000-02-10")
        predict_interval = ("2000-01-20", "2000-02-23")
        data_source_node = cdataf.ReadDataFromDf("data", data)
        data_source_node.set_fit_intervals([fit_interval])
        data_source_node.set_predict_intervals([predict_interval])
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
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
        df_out = zscore_df.join(inverted_rets)
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_col_mode1(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["col_mode"] = "replace_all"
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_col_mode2(self) -> None:
        # Load test data.
        data = self._get_data()
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["col_mode"] = "replace_selected"
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_fit_multiple_columns(self) -> None:
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Specify config and create modeling node.
        config = ccfg.Config()
        config["cols"] = ["ret_0", "ret_0_2"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        #
        df_out = dag.run_leq_node("vol_model", "fit")["df_out"]
        info = cdataf.extract_info(dag, ["fit"])
        # Package results.
        act = self._package_results1(config, info, df_out)
        self.check_string(act)

    def test_multiple_columns_with_specified_tau(self) -> None:
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccfg.Config()
        config["cols"] = ["ret_0", "ret_0_2"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "drop"
        config["tau"] = 10
        # Check if specified tau is used for all columns via learned taus property.
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        node.fit(data)
        dbg.dassert_set_eq(node.taus.values(), [10])

    def test_fit_none_columns(self) -> None:
        # Load test data.
        data = self._get_data()
        data["ret_0_2"] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccfg.Config()
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        # Get outputs with `cols`=None and all specified.
        output_cols_none = self._run_volatility_model(data, config)
        config["cols"] = ["ret_0", "ret_0_2"]
        output_cols_specified = self._run_volatility_model(data, config)
        np.testing.assert_equal(
            output_cols_none.to_string(),
            output_cols_specified.to_string(),
        )

    def test_fit_int_columns(self) -> None:
        # Load test data.
        data = self._get_data()
        data[10] = data.ret_0 + np.random.normal(size=len(data))
        # Specify config.
        config = ccfg.Config()
        config["cols"] = [10]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        # Get output with integer column names.
        output = self._run_volatility_model(data, config)
        self.check_string(output.to_string())

    def test_get_fit_state1(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        df_out = node.fit(data)["df_out"]
        # Package results.
        state = node.get_fit_state()
        act = self._package_results2(config, state, df_out)
        self.check_string(act)

    def test_get_fit_state2(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        state = {
            "_fit_cols": ["ret_0"],
            "_vol_cols": {"ret_0": "ret_0_vol"},
            "_fwd_vol_cols": {"ret_0": "ret_0_vol_2"},
            "_fwd_vol_cols_hat": {"ret_0": "ret_0_vol_2_hat"},
            "_taus": {"ret_0": 10},
            "_info['fit']": None,
        }
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        node.set_fit_state(state)
        df_out = node.predict(data)["df_out"]
        # Package results.
        act = self._package_results2(config, state, df_out)
        self.check_string(act)

    def test_predict_with_predefined_state(self) -> None:
        data = self._get_data()
        config = ccfg.Config()
        config["cols"] = ["ret_0"]
        config["steps_ahead"] = 2
        config["nan_mode"] = "leave_unchanged"
        node_fit = cdataf.VolatilityModel("vol_model", **config.to_dict())
        node_fit.fit(data)
        output_fit = node_fit.predict(data)["df_out"]
        node_predefined = cdataf.VolatilityModel("vol_model", **config.to_dict())
        node_predefined.set_fit_state(node_fit.get_fit_state())
        output_predefined = node_predefined.predict(data)["df_out"]
        pd.testing.assert_frame_equal(output_fit, output_predefined)

    @staticmethod
    def _package_results1(
        config: ccfg.Config,
        info: collections.OrderedDict,
        df_out: pd.DataFrame,
    ) -> str:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act.append(hprint.frame("info"))
        act.append(str(ccbuild.get_config_from_nested_dict(info)))
        act.append(hprint.frame("df_out"))
        act.append(hut.convert_df_to_string(df_out, index=True))
        act = "\n".join(act)
        return act

    @staticmethod
    def _package_results2(
        config: ccfg.Config, state: Dict[str, Any], df_out: pd.DataFrame
    ) -> str:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act.append(hprint.frame("state"))
        act.append(str(state))
        act.append(hprint.frame("df_out"))
        act.append(hut.convert_df_to_string(df_out, index=True))
        act = "\n".join(act)
        return act

    @staticmethod
    def _get_data() -> pd.DataFrame:
        """
        Generate "random returns".

        Use lag + noise as predictor.
        """
        arma_process = casgen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2000-01-01", "periods": 40, "freq": "B"}
        date_range = pd.date_range(**date_range_kwargs)
        realization = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, seed=0
        )
        realization.name = "ret_0"
        df = pd.DataFrame(index=date_range, data=realization)
        return df

    @staticmethod
    def _run_volatility_model(data: pd.DataFrame, config: ccfg.Config) -> str:
        data_source_node = cdataf.ReadDataFromDf("data", data)
        # Create DAG and test data node.
        dag = cdataf.DAG(mode="strict")
        dag.add_node(data_source_node)
        # Create modeling node.
        node = cdataf.VolatilityModel("vol_model", **config.to_dict())
        dag.add_node(node)
        dag.connect("data", "vol_model")
        output = dag.run_leq_node("vol_model", "fit")["df_out"]
        return output


class TestVolatilityModulator(hut.TestCase):
    def test_modulate1(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        # Get mock returns prediction 1 step ahead indexed by knowledge time.
        y_hat = csproc.compute_smooth_moving_average(df_in["ret_0"], 4).shift(-1)
        df_in["ret_1_hat"] = y_hat
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_1_hat"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 1,
                "volatility_steps_ahead": 2,
                "mode": "modulate",
            }
        )
        node = cdataf.VolatilityModulator("modulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def test_demodulate1(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_0"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 0,
                "volatility_steps_ahead": 2,
                "mode": "demodulate",
            }
        )
        node = cdataf.VolatilityModulator("demodulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def test_col_mode1(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_0"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 0,
                "volatility_steps_ahead": 2,
                "mode": "demodulate",
                "col_rename_func": lambda x: f"{x}_zscored",
                "col_mode": "merge_all",
            }
        )
        node = cdataf.VolatilityModulator("demodulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def test_col_mode2(self) -> None:
        steps_ahead = 2
        df_in = self._get_signal_and_fwd_vol(steps_ahead)
        config = ccbuild.get_config_from_nested_dict(
            {
                "signal_cols": ["ret_0"],
                "volatility_col": "vol_2_hat",
                "signal_steps_ahead": 0,
                "volatility_steps_ahead": 2,
                "mode": "demodulate",
                "col_rename_func": lambda x: f"{x}_zscored",
                "col_mode": "replace_selected",
            }
        )
        node = cdataf.VolatilityModulator("demodulate", **config.to_dict())
        df_out = node.fit(df_in)["df_out"]
        # Check results.
        self._check_results(config, df_in, df_out)

    def _check_results(
        self, config: ccfg.Config, df_in: pd.DataFrame, df_out: pd.DataFrame
    ) -> None:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act = "\n".join(act)
        self.check_string(act)
        self.check_dataframe(df_in, tag="df_in", err_threshold=0.01)
        self.check_dataframe(df_out, tag="df_out", err_threshold=0.01)

    @staticmethod
    def _get_signal_and_fwd_vol(
        steps_ahead: int,
    ) -> pd.DataFrame:
        arma_process = casgen.ArmaProcess([0.45], [0])
        date_range_kwargs = {"start": "2010-01-01", "periods": 40, "freq": "B"}
        signal = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, scale=0.1, seed=42
        )
        vol = csproc.compute_smooth_moving_average(signal, 16)
        fwd_vol = vol.shift(steps_ahead)
        return pd.concat(
            [signal.rename("ret_0"), fwd_vol.rename("vol_2_hat")], axis=1
        )


# #############################################################################
# Test gluon-ts - DeepAR
# #############################################################################


if True:

    class TestContinuousDeepArModel(hut.TestCase):
        @pytest.mark.skip("Disabled because of PTask2440")
        def test_fit_dag1(self) -> None:
            dag = self._get_dag()
            #
            df_out = dag.run_leq_node("deepar", "fit")["df_out"]
            self.check_string(df_out.to_string())

        @pytest.mark.skip("Disabled because of PTask2440")
        def test_predict_dag1(self) -> None:
            dag = self._get_dag()
            #
            dag.run_leq_node("deepar", "fit")
            df_out = dag.run_leq_node("deepar", "predict")["df_out"]
            self.check_string(df_out.to_string())

        def _get_dag(self) -> cdataf.DAG:
            mxnet.random.seed(0)
            data, _ = casgen.get_gluon_dataset(
                dataset_name="m4_hourly",
                train_length=100,
                test_length=1,
            )
            fit_idxs = data.iloc[:70].index
            predict_idxs = data.iloc[70:].index
            data_source_node = cdataf.ReadDataFromDf("data", data)
            data_source_node.set_fit_idxs(fit_idxs)
            data_source_node.set_predict_idxs(predict_idxs)
            # Create DAG and test data node.
            dag = cdataf.DAG(mode="strict")
            dag.add_node(data_source_node)
            # Load deepar config and create modeling node.
            config = ccfg.Config()
            config["x_vars"] = None
            config["y_vars"] = ["y"]
            config["trainer_kwargs"] = {"epochs": 1}
            config["estimator_kwargs"] = {"prediction_length": 2}
            node = cdataf.ContinuousDeepArModel(
                "deepar",
                **config.to_dict(),
            )
            dag.add_node(node)
            dag.connect("data", "deepar")
            return dag

    class TestDeepARGlobalModel(hut.TestCase):
        @pytest.mark.skip("Disabled because of PTask2440")
        def test_fit1(self) -> None:
            mxnet.random.seed(0)
            local_ts = self._get_local_ts()
            num_entries = 100
            config = self._get_config()
            deepar = cdataf.DeepARGlobalModel(**config.to_dict())
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

        @pytest.mark.skip("Disabled because of PTask2440")
        def test_fit_dag1(self) -> None:
            mxnet.random.seed(0)
            dag = cdataf.DAG(mode="strict")
            local_ts = self._get_local_ts()
            data_source_node = cdataf.ReadDataFromDf("local_ts", local_ts)
            dag.add_node(data_source_node)
            config = self._get_config()
            deepar = cdataf.DeepARGlobalModel(**config.to_dict())
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


