import logging

import core.artificial_signal_generators as sig_gen
import core.config as cfg
import core.dataframe_modeler as dfmod
import helpers.unit_test as hut
import pandas as pd
import sklearn.linear_model as slm

_LOG = logging.getLogger(__name__)


class TestDataFrameModeler(hut.TestCase):
    def test_fit(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        # Load sklearn config.
        config = self._get_config(pred_lag)
        # Create DataFrame Modeler object.
        df_modeler = dfmod.DataFrameModeler(
            df=data, oos_start=config["oos_start"]
        )
        # Apply sklearn model with fit method.
        output = df_modeler.apply_sklearn_model(
            model_func=slm.LinearRegression,
            x_vars=config["x"],
            y_vars=config["y"],
            steps_ahead=config["steps_ahead"],
            method="fit",
        )
        output_df = output.df
        info_fit = output.get_info("fit")
        _LOG.info("Info Fit %s", info_fit)
        self.check_string(output_df.to_string())

    def test_predict(self) -> None:
        pred_lag = 2
        # Load test data.
        data = self._get_data(pred_lag)
        # Load sklearn config.
        config = self._get_config(pred_lag)
        # Create DataFrame Modeler object.
        df_modeler = dfmod.DataFrameModeler(
            df=data, oos_start=config["oos_start"]
        )
        # Apply sklearn model with predict method.
        output_df = df_modeler.apply_sklearn_model(
            model_func=slm.LinearRegression,
            x_vars=config["x"],
            y_vars=config["y"],
            steps_ahead=config["steps_ahead"],
            method="predict",
        )
        output_df = output.df
        info_fit = output.get_info("fit")
        info_predict = output.get_info("predict")
        _LOG.info("Info Fit %s", info_fit)
        _LOG.info("Info Predict %s", info_predict)
        self.check_string(output_df.to_string())

    def _get_config(self, steps_ahead: int) -> cfg.Config:
        config = cfg.Config()
        config["x_vars"] = ["x"]
        config["y_vars"] = ["y"]
        config["steps_ahead"] = steps_ahead
        config["oos_start"] = "2010-02-30"
        config_kwargs = config.add_subconfig("model_kwargs")
        config_kwargs["alpha"] = 0.5
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
