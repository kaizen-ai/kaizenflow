import logging
from typing import Iterable

import gluonts

# TODO(*): gluon needs these imports to work properly.
import gluonts.model.deepar as gmd  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.model.predictor as gmp  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.trainer as gt  # isort: skip # noqa: F401 # pylint: disable=unused-import
import numpy as np
import pandas as pd
import statsmodels as sm
import statsmodels.tsa.arima_process as smarima  # isort: skip # noqa: F401 # pylint: disable=unused-import

import core.backtest as btest
import core.config as cfg
import core.data_adapters as adpt
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestGeneratePredictions(hut.TestCase):
    @staticmethod
    def _generate_test_series(
        random_state: int = 42,
        n_periods: int = 20,
        ar: Iterable[float] = np.array([0.462, -0.288]),
        ma: Iterable[float] = np.array([0.01]),
    ) -> np.array:
        np.random.seed(random_state)
        return sm.tsa.arima_process.arma_generate_sample(
            ar=ar, ma=ma, nsample=n_periods, burnin=10
        )

    @staticmethod
    def _generate_input_data(
        n_periods: int = 20, base_random_state: int = 0, shift: int = 4,
    ) -> pd.DataFrame:
        np.random.seed(base_random_state)
        n_x_vars = 2
        # Generate `x_vals`.
        x_vals = [
            TestGeneratePredictions._generate_test_series(
                random_state=base_random_state + i, n_periods=n_periods + shift
            )
            for i in range(n_x_vars)
        ]
        x_vals = np.vstack(x_vals).T
        # Generate `y` as linear combination of `x_i`.
        weights = np.random.dirichlet(np.ones(n_x_vars), 1).flatten()
        y = np.average(x_vals, axis=1, weights=weights)
        # Shift `y` (`y = weighted_sum(x).shift(shift)`).
        x = x_vals[shift:]
        y = y[:-shift]
        # Generate a dataframe.
        x_y = np.hstack([x, y.reshape(-1, 1)])
        idx = pd.date_range("2010-01-01", periods=n_periods, freq="T")
        x_cols = [f"x{i}" for i in range(n_x_vars)]
        return pd.DataFrame(x_y, index=idx, columns=x_cols + ["y"])

    @staticmethod
    def _train_model(
        prediction_length: int = 3, use_feat_dynamic_real: bool = False
    ) -> gluonts.model.predictor.Predictor:
        df = TestGeneratePredictions._generate_input_data(base_random_state=42)
        x_vars = df.columns.tolist()[:-1]
        y_vars = df.columns.tolist()[-1:]
        train_ts = adpt.transform_to_gluon(df, x_vars, y_vars, "T")
        config = cfg.Config()
        config["trainer_kwargs"] = {"epochs": 1}
        config["estimator_kwargs"] = {
            "freq": "T",
            "use_feat_dynamic_real": use_feat_dynamic_real,
        }
        trainer = gluonts.trainer.Trainer(**config["trainer_kwargs"])
        estimator = gluonts.model.deepar.DeepAREstimator(
            prediction_length=prediction_length,
            trainer=trainer,
            **config["estimator_kwargs"],
        )
        return estimator.train(train_ts)

    def test1(self) -> None:
        prediction_length = 3
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length
        )
        test_df = TestGeneratePredictions._generate_input_data(
            base_random_state=0
        )
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)

    def test_single_value1(self) -> None:
        prediction_length = 3
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length
        )
        test_df = TestGeneratePredictions._generate_input_data(
            n_periods=1, base_random_state=0
        )
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)

    def test_two_values1(self) -> None:
        prediction_length = 3
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length
        )
        test_df = TestGeneratePredictions._generate_input_data(
            n_periods=2, base_random_state=0
        )
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)

    def test_none_x_vars1(self) -> None:
        prediction_length = 3
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length
        )
        test_df = TestGeneratePredictions._generate_input_data(
            base_random_state=0
        )
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)

    def test_use_feat_dynamic_real1(self) -> None:
        prediction_length = 6
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length, use_feat_dynamic_real=True
        )
        test_df = TestGeneratePredictions._generate_input_data(
            base_random_state=0
        )
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, True, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)
