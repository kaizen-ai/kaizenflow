import collections
import logging

import pandas as pd

import core.config as cfg
import core.config_builders as cfgb
import core.dataflow as dtf
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestPredictionResultBundle(hut.TestCase):
    def test_to_config1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual_config = prb.to_config(commit_hash=False)
        self.check_string(f"config without 'commit_hash' field:\n{actual_config}")

    def test_get_feature_col_names1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_feature_col_names()
        expected = ["col0"]
        self.assertListEqual(actual, expected)

    def test_get_target_col_names1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_target_col_names()
        expected = ["col1", "col2"]
        self.assertListEqual(actual, expected)

    def test_get_prediction_col_names1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_prediction_col_names()
        expected = ["col3", "col4"]
        self.assertListEqual(actual, expected)

    def test_get_target_and_prediction_col_names_for_tags1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_target_and_prediction_col_names_for_tags(tags=[0, 1])
        expected = {0: ("col1", "col3"), 1: ("col2", "col4")}
        self.assertDictEqual(actual, expected)

    def test_get_target_and_prediction_col_names_for_tags2(self) -> None:
        """
        Try to extract columns with no target column for given tag.
        """
        init_config = self._get_init_config()
        init_config["column_to_tags"].pop("col1")
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        with self.assertRaises(AssertionError):
            prb.get_target_and_prediction_col_names_for_tags(tags=[0])

    def test_get_target_and_prediction_col_names_for_tags3(self) -> None:
        """
        Extract columns with no target column for another tag.
        """
        init_config = self._get_init_config()
        init_config["column_to_tags"].pop("col1")
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_target_and_prediction_col_names_for_tags(tags=[1])
        expected = {1: ("col2", "col4")}
        self.assertDictEqual(actual, expected)

    @staticmethod
    def _get_init_config() -> cfg.Config:
        init_config = cfg.Config()
        init_config["config"] = cfgb.get_config_from_nested_dict({"key": "val"})
        init_config["result_nid"] = "leaf_node"
        init_config["method"] = "fit"
        df = pd.DataFrame([range(5)], columns=[f"col{i}" for i in range(5)])
        init_config["result_df"] = df
        init_config["column_to_tags"] = {
            "col0": ["feature_col"],
            "col1": ["target_col", 0],
            "col2": ["target_col", 1],
            "col3": ["prediction_col", 0],
            "col4": ["prediction_col", 1],
        }
        init_config["info"] = collections.OrderedDict(
            {"df_info": dtf.get_df_info_as_string(df)}
        )
        return init_config
