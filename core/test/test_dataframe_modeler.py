import logging

import core.artificial_signal_generators as sig_gen
import core.config as cfg
import core.dataframe_modeler as dfmod
import helpers.unit_test as hut
import pandas as pd
import sklearn.linear_model as slm

_LOG = logging.getLogger(__name__)


class TestDataFrameModeler(hut.TestCase):
    def test_sklearnmodel_fit_with_oos(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        # Load sklearn config.
        config = self._get_config(pred_lag)
        # Create DataFrame Modeler object.
        df_modeler = dfmod.DataFrameModeler(df=data, oos_start="2010-03-01")
        # Apply sklearn model with fit method.
        output = df_modeler.apply_sklearn_model(**config.to_dict())
        output_df = output.df
        info_fit = output.info["fit"]
        _LOG.info("Info Fit %s", info_fit)
        self.check_string(output_df.to_string())

    def test_sklearnmodel_predict_with_oos(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        # Load sklearn config.
        config = self._get_config(pred_lag)
        # Create DataFrame Modeler object.
        df_modeler = dfmod.DataFrameModeler(df=data, oos_start="2010-03-01")
        # Apply sklearn model with predict method.
        output = df_modeler.apply_sklearn_model(**config.to_dict(), method = "predict")
        output_df = output.df
        info_fit = output.info["fit"]
        info_predict = output.info["predict"]
        _LOG.info("Info Fit %s", info_fit)
        _LOG.info("Info Predict %s", info_predict)
        self.check_string(output_df.to_string())

    def test_sklearnmodel_fit_without_oos(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        # Load sklearn config.
        config = self._get_config(pred_lag)
        # Create DataFrame Modeler object.
        df_modeler = dfmod.DataFrameModeler(df=data)
        # Apply sklearn model with fit method.
        output = df_modeler.apply_sklearn_model(**config.to_dict())
        output_df = output.df
        info_fit = output.info["fit"]
        _LOG.info("Info Fit %s", info_fit)
        self.check_string(output_df.to_string())

    def test_sklearnmodel_predict_without_oos(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        # Load sklearn config.
        config = self._get_config(pred_lag)
        # Create DataFrame Modeler object.
        df_modeler = dfmod.DataFrameModeler(df=data)
        # Apply sklearn model with predict method.
        output = df_modeler.apply_sklearn_model(**config.to_dict(), method = "predict")
        output_df = output.df
        info_fit = output.info["fit"]
        info_predict = output.info["predict"]
        _LOG.info("Info Fit %s", info_fit)
        _LOG.info("Info Predict %s", info_predict)
        self.check_string(output_df.to_string())

    def _get_config(self, steps_ahead: int) -> cfg.Config:
        config = cfg.Config()
        config["x_vars"] = ["x"]
        config["y_vars"] = ["y"]
        config["steps_ahead"] = steps_ahead
        config["model_func"] = slm.LinearRegression
        return config

    def _get_data(self, lag: int) -> pd.DataFrame:
        """
        Generate "random returns". Use lag + noise as predictor.
        """
        num_periods = 100
        total_steps = num_periods + lag + 1
        rets = sig_gen.get_gaussian_walk(0, 0.2, total_steps, seed=10).diff()
        noise = sig_gen.get_gaussian_walk(0, 0.02, total_steps, seed=1).diff()
        pred = rets.shift(-lag).loc[1:num_periods] + noise.loc[1:num_periods]
        resp = rets.loc[1:num_periods]
        idx = pd.date_range("2010-01-01", periods=num_periods, freq="D")
        df = pd.DataFrame.from_dict({"x": pred, "y": resp}).set_index(idx)
        return df
