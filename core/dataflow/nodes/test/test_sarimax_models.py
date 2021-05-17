import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import sklearn.linear_model as slmode

import core.artificial_signal_generators as casgen
import core.config as ccfg
import core.config_builders as ccbuild
import core.signal_processing as csproc
import helpers.printing as hprint
import helpers.unit_test as hut
from core.dataflow.nodes.sarimax_models import (
    ContinuousSarimaxModel,
    MultihorizonReturnsPredictionProcessor,
)
from core.dataflow.nodes.sklearn_models import ContinuousSkLearnModel

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test statsmodels - SARIMAX
# #############################################################################


class TestContinuousSarimaxModel(hut.TestCase):
    """
    Warning: SARIMAX can give slightly different outputs on different machines.
    """

    def test_fit1(self) -> None:
        data = self._get_data([], [])
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        # Check results.
        self._check_results(config, df_out)

    def test_fit_step_one1(self) -> None:
        """
        Fit on `x = y`.
        """
        data = self._get_data([1], [])
        data["x"] = data["ret_0"]
        config = self._get_config((1, 0, 0))
        config["steps_ahead"] = 1
        config["fit_kwargs"] = {"start_params": [0.9999, 0.0001, 1.57e-11]}
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        # Check results.
        self._check_results(config, df_out)

    def test_fit_with_constant1(self) -> None:
        data = self._get_data([1], [])
        config = self._get_config((1, 0, 0))
        config["steps_ahead"] = 2
        config["add_constant"] = True
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        # Check results.
        self._check_results(config, df_out)

    def test_fit_no_x1(self) -> None:
        """
        Fit without providing an exogenous variable.
        """
        data = self._get_data([], [])
        data.drop(columns=["x"], inplace=True)
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        config["x_vars"] = None
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        df_out = csm.fit(data)["df_out"]
        # Check results.
        self._check_results(config, df_out, err_threshold=0.05)

    def test_compare_to_linear_regression1(self) -> None:
        """
        Compare SARIMAX results to Linear Regression.
        """
        data = self._get_data([1], [])
        data.drop(columns=["x"], inplace=True)
        steps_ahead = 1
        # Train SkLearn model.
        sklearn_config = ccbuild.get_config_from_nested_dict(
            {
                "model_func": slmode.LinearRegression,
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": steps_ahead,
                "col_mode": "merge_all",
            }
        )
        sklearn_model = ContinuousSkLearnModel(
            "model", **sklearn_config.to_dict()
        )
        skl_out = sklearn_model.fit(data)["df_out"]
        skl_out.rename(columns=lambda x: "skl_" + x, inplace=True)
        # Train SARIMAX model.
        sarimax_config = self._get_config((1, 0, 0))
        sarimax_config["x_vars"] = None
        sarimax_config["steps_ahead"] = steps_ahead
        sarimax_model = ContinuousSarimaxModel(
            "model", **sarimax_config.to_dict()
        )
        sarimax_out = sarimax_model.fit(data)["df_out"]
        sarimax_out.rename(columns=lambda x: "sarimax_" + x, inplace=True)
        # Compare outputs.
        df_out = pd.concat([skl_out, sarimax_out], axis=1)
        df_out["skl_sarimax_pred_diff"] = (
            df_out["skl_ret_0_1_hat"] - df_out["sarimax_ret_0_1_hat"]
        )
        # TODO(gp): Factor this out.
        act = (
            f"{hprint.frame('sklearn_config')}\n{sklearn_config}\n"
            f"{hprint.frame('sarimax_config')}\n{sarimax_config}\n"
            f"{hprint.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(act)

    def test_compare_to_linear_regression2(self) -> None:
        """
        Compare SARIMAX results to Linear Regression for 3 steps ahead.
        """
        data = self._get_data([1], [])
        data.drop(columns=["x"], inplace=True)
        steps_ahead = 3
        # Train SkLearn model.
        sklearn_config = ccbuild.get_config_from_nested_dict(
            {
                "model_func": slmode.LinearRegression,
                "x_vars": ["ret_0"],
                "y_vars": ["ret_0"],
                "steps_ahead": steps_ahead,
                "col_mode": "merge_all",
            }
        )
        sklearn_model = ContinuousSkLearnModel(
            "model", **sklearn_config.to_dict()
        )
        skl_out = sklearn_model.fit(data)["df_out"]
        skl_out.rename(columns=lambda x: "skl_" + x, inplace=True)
        # Train SARIMAX model.
        sarimax_config = self._get_config((1, 0, 0))
        sarimax_config["x_vars"] = None
        sarimax_config["steps_ahead"] = steps_ahead
        sarimax_model = ContinuousSarimaxModel(
            "model", **sarimax_config.to_dict()
        )
        sarimax_out = sarimax_model.fit(data)["df_out"]
        sarimax_out.rename(columns=lambda x: "sarimax_" + x, inplace=True)
        # Compare outputs.
        df_out = pd.concat([skl_out, sarimax_out], axis=1)
        df_out["skl_sarimax_pred_diff"] = (
            df_out["skl_ret_0_3_hat"] - df_out["sarimax_ret_0_3_hat"]
        )
        # TODO(gp): Factor this out.
        act = (
            f"{hprint.frame('sklearn_config')}\n{sklearn_config}\n"
            f"{hprint.frame('sarimax_config')}\n{sarimax_config}\n"
            f"{hprint.frame('df_out')}\n"
            f"{hut.convert_df_to_string(df_out, index=True)}"
        )
        self.check_string(act)

    def test_predict1(self) -> None:
        data = self._get_data([], [])
        data_fit = data.iloc[:70]
        data_predict = data.iloc[70:]
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        # Check results.
        self._check_results(config, df_out, err_threshold=0.40)

    def test_predict2(self) -> None:
        """
        Test AR(1) process.
        """
        data = self._get_data([1], [])
        data_fit = data.iloc[:70]
        data_predict = data.iloc[70:]
        config = self._get_config((1, 0, 0))
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        # Check results.
        self._check_results(config, df_out)

    def test_predict_with_nan(self) -> None:
        """
        Test AR(1) process with NaNs in the target.
        """
        data = self._get_data([1], [])
        data.iloc[10:12, 1] = np.nan
        data.iloc[80, 1] = np.nan
        data_fit = data.iloc[:70]
        data_predict = data.iloc[70:]
        config = self._get_config((1, 0, 0))
        config["nan_mode"] = "leave_unchanged"
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        # Check results.
        self._check_results(config, df_out)

    def test_predict_different_intervals1(self) -> None:
        """
        Verify that predictions on different intervals match.
        """
        data = self._get_data([1], [], periods=120, freq="D")
        config = self._get_config((1, 0, 0))
        data_fit = data.loc[:"2010-03-12"]  # type: ignore
        data_predict1 = data.loc["2010-03-12":"2010-04-02"]  # type: ignore
        data_predict2 = data.loc["2010-03-16":"2010-04-17"]  # type: ignore
        data_predict3 = data.loc["2010-04-01":"2010-04-27"]  # type: ignore
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out1 = csm.predict(data_predict1)["df_out"]
        df_out2 = csm.predict(data_predict2)["df_out"]
        df_out3 = csm.predict(data_predict3)["df_out"]
        #
        pd.testing.assert_series_equal(
            df_out1.loc["2010-03-25":"2010-03-29", "ret_0_3_hat"],  # type: ignore
            df_out2.loc["2010-03-25":"2010-03-29", "ret_0_3_hat"],  # type: ignore
        )
        pd.testing.assert_series_equal(
            df_out2.loc["2010-04-10":"2010-04-13", "ret_0_3_hat"],  # type: ignore
            df_out3.loc["2010-04-10":"2010-04-13", "ret_0_3_hat"],  # type: ignore
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
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out = csm.predict(data_predict)["df_out"]
        # Check results.
        self._check_results(config, df_out, err_threshold=0.20)

    def test_predict_different_intervals_no_x1(self) -> None:
        """
        Verify that predictions on different intervals match.
        """
        data = self._get_data([1], [], periods=120, freq="D")
        data.drop(columns=["x"], inplace=True)
        config = self._get_config((1, 0, 0))
        config["x_vars"] = None
        data_fit = data.loc[:"2010-03-12"]  # type: ignore
        data_predict1 = data.loc["2010-03-12":"2010-04-02"]  # type: ignore
        data_predict2 = data.loc["2010-03-20":"2010-04-17"]  # type: ignore
        data_predict3 = data.loc["2010-04-01":"2010-04-27"]  # type: ignore
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data_fit)
        df_out1 = csm.predict(data_predict1)["df_out"]
        df_out2 = csm.predict(data_predict2)["df_out"]
        df_out3 = csm.predict(data_predict3)["df_out"]
        #
        pd.testing.assert_series_equal(
            df_out1.loc["2010-03-26":"2010-04-01", "ret_0_3_hat"],  # type: ignore
            df_out2.loc["2010-03-26":"2010-04-01", "ret_0_3_hat"],  # type: ignore
        )
        pd.testing.assert_series_equal(
            df_out2.loc["2010-04-07":"2010-04-16", "ret_0_3_hat"],  # type: ignore
            df_out3.loc["2010-04-07":"2010-04-16", "ret_0_3_hat"],  # type: ignore
        )

    def test_summary(self) -> None:
        data = self._get_data([], [])
        config = self._get_config((1, 0, 1), (1, 0, 1, 3))
        csm = ContinuousSarimaxModel("model", **config.to_dict())
        csm.fit(data)
        info = csm.get_info("fit")["model_summary"]
        # TODO(gp): Use the idiom like `_check_results()` instead of all
        #  these unreadable f-strings.
        act = (
            f"{hut.convert_df_to_string(info['info'], index=True)}\n"
            f"{hut.convert_df_to_string(info['tests'], index=True)}\n"
            f"{hut.convert_df_to_string(info['coefs'], index=True)}"
        )
        self.check_string(act)

    def _check_results(
        self,
        config: ccfg.Config,
        df_out: pd.DataFrame,
        err_threshold: float = 0.01,
    ) -> None:
        act: List[str] = []
        act.append(hprint.frame("config"))
        act.append(str(config))
        act = "\n".join(act)
        self.check_string(act)
        #
        self.check_dataframe(df_out, err_threshold=err_threshold)

    @staticmethod
    def _get_data(
        ar_coeffs: List[int],
        ma_coeffs: List[int],
        periods: int = 100,
        freq: str = "M",
        seed: int = 42,
    ) -> pd.DataFrame:
        arma_process = casgen.ArmaProcess(ar_coeffs, ma_coeffs)
        date_range_kwargs = {
            "start": "2010-01-01",
            "periods": periods,
            "freq": freq,
        }
        y = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, scale=0.1, seed=seed
        ).rename("ret_0")
        x = csproc.compute_smooth_moving_average(y, 26).rename("x")
        return pd.concat([x, y], axis=1)

    @staticmethod
    def _get_config(
        order: Tuple[int, int, int],
        seasonal_order: Optional[Tuple[int, int, int, int]] = None,
    ) -> ccfg.Config:
        config = ccbuild.get_config_from_nested_dict(
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


class TestMultihorizonReturnsPredictionProcessor(hut.TestCase):
    def test1(self) -> None:
        model_output = self._get_multihorizon_model_output(3)
        config = ccbuild.get_config_from_nested_dict(
            {
                "target_col": "ret_0_zscored",
                "prediction_cols": [
                    "ret_0_zscored_1_hat",
                    "ret_0_zscored_2_hat",
                    "ret_0_zscored_3_hat",
                ],
                "volatility_col": "vol_1_hat",
            }
        )
        mrpp = MultihorizonReturnsPredictionProcessor(
            "process_results", **config.to_dict()
        )
        cum_y_yhat = mrpp.fit(model_output)["df_out"]
        # TODO(Julia): Ask about creating a `TestFitPredictNode(hut.TestCase)`
        #  class that will take care of this piece.
        # TODO(gp): Use the idiom like `_check_results()` instead of all
        #  these unreadable f-strings.
        act = (
            f"{hprint.frame('config')}\n{config}\n"
            f"{hprint.frame('df_in')}\n"
            f"{hut.convert_df_to_string(model_output, index=True)}\n"
            f"{hprint.frame('df_out')}\n"
            f"{hut.convert_df_to_string(cum_y_yhat, index=True)}\n"
        )
        self.check_string(act)

    def test_invert_zret_0_zscoring1(self) -> None:
        model_output = self._get_multihorizon_model_output(1)
        config = ccbuild.get_config_from_nested_dict(
            {
                "target_col": "ret_0_zscored",
                "prediction_cols": ["ret_0_zscored_1_hat"],
                "volatility_col": "vol_1_hat",
            }
        )
        mrpp = MultihorizonReturnsPredictionProcessor(
            "process_results", **config.to_dict()
        )
        cum_y_yhat = mrpp.fit(model_output)["df_out"]
        #
        ret_0 = model_output["ret_0"]
        fwd_ret_0 = ret_0.shift(-1).rename("cumret_1_original")
        ret_0_from_result = cum_y_yhat[["cumret_1"]]
        df_out = ret_0_from_result.join(fwd_ret_0, how="outer")
        act = hut.convert_df_to_string(df_out, index=True)
        self.check_string(act)

    def test_invert_zret_3_zscoring1(self) -> None:
        model_output = self._get_multihorizon_model_output(3)
        config = ccbuild.get_config_from_nested_dict(
            {
                "target_col": "ret_0_zscored",
                "prediction_cols": [
                    "ret_0_zscored_1_hat",
                    "ret_0_zscored_2_hat",
                    "ret_0_zscored_3_hat",
                ],
                "volatility_col": "vol_1_hat",
            }
        )
        mrpp = MultihorizonReturnsPredictionProcessor(
            "process_results", **config.to_dict()
        )
        cum_y_yhat = mrpp.fit(model_output)["df_out"]
        #
        ret_0 = model_output["ret_0"]
        cumret_3 = csproc.accumulate(ret_0, 3)
        fwd_cumret_3 = cumret_3.shift(-3).rename("cumret_3_original")
        #
        cumret_3_from_result = cum_y_yhat[["cumret_3"]]
        df_out = cumret_3_from_result.join(fwd_cumret_3, how="outer")
        act = hut.convert_df_to_string(df_out, index=True)
        self.check_string(act)

    @staticmethod
    def _get_series(seed: int = 24) -> pd.Series:
        arma_process = casgen.ArmaProcess([1], [1])
        date_range_kwargs = {"start": "2010-01-01", "periods": 50, "freq": "D"}
        series = arma_process.generate_sample(
            date_range_kwargs=date_range_kwargs, scale=0.1, seed=seed
        )
        return series

    @staticmethod
    def _get_multihorizon_model_output(
        steps_ahead: int, seed: int = 42
    ) -> pd.DataFrame:
        # Get returns.
        rets = TestMultihorizonReturnsPredictionProcessor._get_series(
            seed=seed
        ).rename("ret_0")
        # Get volatility estimate indexed by knowledge time. Volatility delay
        # should be one.
        fwd_vol = csproc.compute_smooth_moving_average(rets, 16).rename(
            "vol_1_hat"
        )
        rets_zscored = (rets / fwd_vol.shift(1)).to_frame(name="ret_0_zscored")
        fwd_rets_zscored = rets_zscored.shift(-steps_ahead).rename(
            lambda x: f"{x}_{steps_ahead}", axis=1
        )
        # Get mock returns predictions.
        model_output = [rets, fwd_vol, rets_zscored, fwd_rets_zscored]
        for i in range(1, steps_ahead + 1):
            ret_hat = csproc.compute_smooth_moving_average(
                rets_zscored, tau=i + 1
            ).rename(lambda x: f"{x}_{i}_hat", axis=1)
            model_output.append(ret_hat)
        return pd.concat(model_output, axis=1)
