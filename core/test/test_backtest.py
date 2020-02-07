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
        num_x_vars: int = 2,
        n_periods: int = 20,
        base_random_state: int = 0,
        shift: int = 1,
    ) -> pd.DataFrame:
        """
        Generate dataframe of predictors and response.

        Example data:
                                   x0        x1         y
        2010-01-01 00:00:00  0.027269  0.010088  0.014319
        2010-01-01 00:01:00  0.024221 -0.017519  0.034699
        2010-01-01 00:02:00  0.047438 -0.014653  0.036345
        2010-01-01 00:03:00  0.025131 -0.028136  0.024469
        2010-01-01 00:04:00  0.022443 -0.016625  0.025981

        :param n_periods: number of time series sampling points
        :param base_random_state:
        :param shift:
        :return:
        """
        np.random.seed(base_random_state)
        num_x_vars = 2
        # Generate `x_vals`.
        x_vals = [
            TestGeneratePredictions._generate_test_series(
                random_state=base_random_state + i, n_periods=n_periods + shift
            )
            for i in range(num_x_vars)
        ]
        x_vals = np.vstack(x_vals).T
        # Generate `y` as linear combination of `x_i`.
        weights = np.random.dirichlet(np.ones(num_x_vars), 1).flatten()
        y = np.average(x_vals, axis=1, weights=weights)
        # Shift `y` (`y = weighted_sum(x).shift(shift)`).
        x = x_vals[shift:]
        y = y[:-shift]
        # Generate a dataframe.
        x_y = np.hstack([x, y.reshape(-1, 1)])
        idx = pd.date_range("2010-01-01", periods=n_periods, freq="T")
        x_cols = [f"x{i}" for i in range(num_x_vars)]
        return pd.DataFrame(x_y, index=idx, columns=x_cols + ["y"])

    def test1(self) -> None:
        """
        """
        df = TestGeneratePredictions._generate_input_data(
            num_x_vars=1, base_random_state=42
        )
        x_vars = ["x0"]
        y_vars = ["y"]
        train_ts = adpt.transform_to_gluon(df, x_vars, y_vars, "T")
        #
        trainer = gluonts.trainer.Trainer(epochs=1)
        prediction_length = 3
        estimator = gluonts.model.deepar.DeepAREstimator(
            prediction_length=prediction_length,
            trainer=trainer,
            freq="T",
            use_feat_dynamic_real=True,
        )
        predictor = estimator.train(train_ts)
        #
        test_df = TestGeneratePredictions._generate_input_data(
            base_random_state=0
        )
        yhat, y = btest.generate_predictions(
            predictor=predictor,
            df=test_df,
            y_vars=y_vars,
            prediction_length=prediction_length,
            num_samples=4,
            x_vars=x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
        )
        self.check_string(str_output)

    def test2(self) -> None:
        """
            """
        df = TestGeneratePredictions._generate_input_data(
            num_x_vars=2, base_random_state=42
        )
        x_vars = ["x0", "x1"]
        y_vars = ["y"]
        train_ts = adpt.transform_to_gluon(df, x_vars, y_vars, "T")
        #
        trainer = gluonts.trainer.Trainer(epochs=1)
        prediction_length = 3
        estimator = gluonts.model.deepar.DeepAREstimator(
            prediction_length=prediction_length,
            trainer=trainer,
            freq="T",
            use_feat_dynamic_real=True,
        )
        predictor = estimator.train(train_ts)
        #
        test_df = TestGeneratePredictions._generate_input_data(
            base_random_state=0
        )
        yhat, y = btest.generate_predictions(
            predictor=predictor,
            df=test_df,
            y_vars=y_vars,
            prediction_length=prediction_length,
            num_samples=4,
            x_vars=x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
        )
        self.check_string(str_output)

    def test3(self) -> None:
        """
        Test autoregressive behavior only (no predictors used).
        """
        df = TestGeneratePredictions._generate_input_data(base_random_state=42)
        y_vars = ["y"]
        train_ts = adpt.transform_to_gluon(df, None, y_vars, "T")
        #
        trainer = gluonts.trainer.Trainer(epochs=1)
        prediction_length = 3
        estimator = gluonts.model.deepar.DeepAREstimator(
            prediction_length=prediction_length, trainer=trainer, freq="T",
        )
        predictor = estimator.train(train_ts)
        #
        test_df = TestGeneratePredictions._generate_input_data(
            base_random_state=0
        )
        yhat, y = btest.generate_predictions(
            predictor=predictor,
            df=test_df,
            y_vars=y_vars,
            prediction_length=prediction_length,
            num_samples=4,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}\n\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n\n"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
        )
        self.check_string(str_output)
