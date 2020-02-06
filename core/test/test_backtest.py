import logging

import gluonts

# TODO(*): gluon needs these imports to work properly.
import gluonts.model.deepar as gmd  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.model.predictor as gmp  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.trainer as gt  # isort: skip # noqa: F401 # pylint: disable=unused-import
import numpy as np
import pandas as pd

import core.backtest as btest
import core.config as cfg
import core.data_adapters as adpt
import helpers.printing as prnt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestGeneratePredictions(hut.TestCase):
    @staticmethod
    def _generate_input_data(
        n_periods: int = 20, random_state: int = 42,
    ) -> pd.DataFrame:
        np.random.seed(random_state)
        idx = pd.date_range("2010-01-01", periods=n_periods, freq="T")
        df = pd.DataFrame(np.random.randn(n_periods, 3), index=idx)
        return df

    @staticmethod
    def _train_model(
        prediction_length: int = 3, use_feat_dynamic_real: bool = False
    ) -> gluonts.model.predictor.Predictor:
        df = TestGeneratePredictions._generate_input_data()
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
        test_df = TestGeneratePredictions._generate_input_data(random_state=0)
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}"
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
            n_periods=1, random_state=0
        )
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}"
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
            n_periods=2, random_state=0
        )
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)

    def test_none_x_vars1(self) -> None:
        prediction_length = 3
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length
        )
        test_df = TestGeneratePredictions._generate_input_data(random_state=0)
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, False,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)

    def test_use_feat_dynamic_real1(self) -> None:
        prediction_length = 6
        predictor = TestGeneratePredictions._train_model(
            prediction_length=prediction_length, use_feat_dynamic_real=True
        )
        test_df = TestGeneratePredictions._generate_input_data(random_state=0)
        x_vars = test_df.columns.tolist()[:-1]
        y_vars = test_df.columns.tolist()[-1:]
        yhat, y = btest.generate_predictions(
            predictor, test_df, y_vars, prediction_length, 4, True, x_vars,
        )
        str_output = (
            f"{prnt.frame('df')}\n{test_df.to_string()}"
            f"{prnt.frame('yhat')}\n{yhat.to_string()}\n"
            f"{prnt.frame('y')}\n{y.to_string()}\n"
        )
        self.check_string(str_output)
