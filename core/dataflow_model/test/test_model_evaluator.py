import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

import core.config as cconfig
import core.dataflow_model.model_evaluator as modeval
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestModelEvaluator(hut.TestCase):
    def test_dump_json1(self) -> None:
        input_config = self._get_input_data()
        evaluator = modeval.ModelEvaluator(**input_config.to_dict())
        actual = evaluator.dump_json()
        self.check_string(actual)

    def test_load_json1(self) -> None:
        input_config = self._get_input_data()
        evaluator = modeval.ModelEvaluator(**input_config.to_dict())
        json_str = evaluator.dump_json()
        loaded_evaluator = modeval.ModelEvaluator.load_json(json_str)
        self._assert_series_dict_equal(evaluator.rets, loaded_evaluator.rets)
        self._assert_series_dict_equal(evaluator.preds, loaded_evaluator.preds)
        self.assertEqual(
            evaluator.target_volatility, loaded_evaluator.target_volatility
        )
        self.assertEqual(evaluator.oos_start, loaded_evaluator.oos_start)

    @staticmethod
    def _get_input_data(periods: int = 5) -> cconfig.Config:
        date_range = pd.date_range(start="2010-01-01", periods=periods)
        keys = [0, 1]
        config = cconfig.Config()
        config["returns"] = {
            key: pd.Series(np.array(range(periods)) + key, index=date_range)
            for key in keys
        }
        config["predictions"] = {
            key: pd.Series(np.array(range(periods)) / 2 + key, index=date_range)
            for key in keys
        }
        config["target_volatility"] = 0.1
        config["oos_start"] = pd.Timestamp("2010-01-03")
        return config

    def _assert_series_dict_equal(
        self, dict1: Dict[Any, pd.Series], dict2: Dict[Any, pd.Series]
    ) -> None:
        self.assertListEqual(list(dict1), list(dict2))
        for key in dict1.keys():
            pd.testing.assert_series_equal(dict1[key], dict2[key])
