import logging

import core.artificial_signal_generators as sig_gen
import core.config as cfg
import core.dataframe_modeler as dfmod
import helpers.printing as prnt
import helpers.unit_test as hut
import pandas as pd
import sklearn.linear_model as slm

_LOG = logging.getLogger(__name__)


class TestDataFrameModeler(hut.TestCase):
    def test_apply_sklearn_model_fit_with_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data, oos_start="2010-03-01")
        output = df_modeler.apply_sklearn_model(**config.to_dict())
        output_df = output.df
        info_fit = output.info["fit"]
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(output_df, index=True)}\n"
            f"{hut.convert_info_to_string(info_fit)}"
        )
        self.check_string(str_output)

    def test_apply_sklearn_model_predict_with_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data, oos_start="2010-03-01")
        output = df_modeler.apply_sklearn_model(
            **config.to_dict(), method="predict"
        )
        output_df = output.df
        info_fit = output.info["fit"]
        output.info["predict"]
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(output_df, index=True)}\n"
            f"{hut.convert_info_to_string(info_fit)}"
        )
        self.check_string(str_output)

    def test_apply_sklearn_model_fit_without_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data)
        output = df_modeler.apply_sklearn_model(**config.to_dict())
        output_df = output.df
        info_fit = output.info["fit"]
        str_output = (
            f"{prnt.frame('config')}\n{config}\n"
            f"{prnt.frame('df_out')}\n{hut.convert_df_to_string(output_df, index=True)}\n"
            f"{hut.convert_info_to_string(info_fit)}"
        )
        self.check_string(str_output)

    def test_apply_sklearn_model_predict_without_oos(self) -> None:
        pred_lag = 2
        data = self._get_data(pred_lag)
        config = self._get_config(pred_lag)
        df_modeler = dfmod.DataFrameModeler(df=data)
        with self.assertRaises(AssertionError):
            output = df_modeler.apply_sklearn_model(
                **config.to_dict(), method="predict"
            )

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
