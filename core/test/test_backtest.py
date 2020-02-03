import logging

import gluonts
import gluonts.model.deepar

# TODO(*): gluon needs this import to work properly.
import gluonts.model.forecast as gmf  # isort: skip # noqa: F401 # pylint: disable=unused-import
import gluonts.model.predictor
import gluonts.trainer
import numpy as np
import pandas as pd

import core.backtest as btest
import core.config as cfg
import core.data_adapters as adpt
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestGeneratePredictions(hut.TestCase):
    @staticmethod
    def _generate_input_data(
        n_periods: int = 100, random_state: int = 42,
    ) -> pd.DataFrame:
        np.random.seed(random_state)
        idx = pd.date_range("2010-01-01", periods=n_periods, freq="T")
        df = pd.DataFrame(np.random.randn(n_periods, 3), index=idx)
        return df

    @staticmethod
    def _train_model(
        prediction_length: int = 3,
    ) -> gluonts.model.predictor.Predictor:
        df = TestGeneratePredictions._generate_input_data()
        x_vars = df.columns[:-1].tolist()
        y_vars = df.columns[-1:].tolist()
        train_ts = adpt.transform_to_gluon(df, x_vars, y_vars, "T")
        config = cfg.Config()
        config["trainer_kwargs"] = {"epochs": 1}
        config["estimator_kwargs"] = {"freq": "T", "use_feat_dynamic_real": False}
        trainer = gluonts.trainer.Trainer(**config["trainer_kwargs"])
        estimator = gluonts.model.deepar.DeepAREstimator(
            prediction_length=prediction_length,
            trainer=trainer,
            **config["estimator_kwargs"]
        )
        return estimator.train(train_ts)

    def test1(self):
        predictor = TestGeneratePredictions._train_model()
        self.predictor = predictor
        test_df = TestGeneratePredictions._generate_input_data(random_state=0)
        x_vars = test_df.columns[:-1].tolist()
        y_vars = test_df.columns[-1:].tolist()
        # return btest.generate_predictions(predictor, test_df, x_vars, y_vars, 10, 15, "T", 4)
        yhat, y = btest.generate_predictions(
            predictor, test_df, x_vars, y_vars, 10, 15, "T", 4
        )
