import collections
import logging

import pandas as pd

import core.config as cfg
import core.config_builders as cfgb
import core.dataflow as dtf
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestResultBundle(hut.TestCase):
    def test_to_config1(self) -> None:
        init_config = self._get_init_config()
        # Initialize a `ResultBundle` using params from `init_config`.
        rb = dtf.ResultBundle(**init_config.to_dict())
        # Check.
        actual_config = rb.to_config(commit_hash=False)
        act = f"config without 'commit_hash' field:\n{actual_config}"
        exp = r"""
config without 'commit_hash' field:
config: OrderedDict([('key', 'val')])
result_nid: leaf_node
method: fit
result_df:    col0  col1  col2  col3  col4
0     0     1     2     3     4
column_to_tags: {'col0': ['feature_col'], 'col1': ['target_col', 'step_0'], 'col2': ['target_col', 'step_1'], 'col3': ['prediction_col', 'step_0'], 'col4': ['prediction_col', 'step_1']}
info:
  df_info: <class 'pandas.core.frame.DataFrame'>
  RangeIndex: 1 entries, 0 to 0
  Data columns (total 5 columns):
   #   Column  Non-Null Count  Dtype
  ---  ------  --------------  -----
   0   col0    1 non-null      int64
   1   col1    1 non-null      int64
   2   col2    1 non-null      int64
   3   col3    1 non-null      int64
   4   col4    1 non-null      int64
  dtypes: int64(5)
payload: None
class: ResultBundle
        """.lstrip().rstrip()
        self.assert_equal(act, exp)

    def test_from_config1(self) -> None:
        """
        Initialize a `ResultBundle` from a config.
        """
        init_config = self._get_init_config()
        # Initialize a `ResultBundle` from a config.
        rb = dtf.ResultBundle.from_config(init_config)
        # Check.
        actual_config = rb.to_config(commit_hash=False)
        self.check_string(f"config without 'commit_hash' field:\n{actual_config}")

    def test_get_tags_for_column1(self) -> None:
        init_config = self._get_init_config()
        rb = dtf.ResultBundle(**init_config.to_dict())
        actual = rb.get_tags_for_column("col2")
        expected = ["target_col", "step_1"]
        self.assertListEqual(actual, expected)

    def test_get_columns_for_tag1(self) -> None:
        init_config = self._get_init_config()
        rb = dtf.ResultBundle(**init_config.to_dict())
        actual = rb.get_columns_for_tag("step_1")
        expected = ["col2", "col4"]
        self.assertListEqual(actual, expected)

    @staticmethod
    def _get_init_config() -> cfg.Config:
        # TODO(gp): Factor out common part.
        init_config = cfg.Config()
        init_config["config"] = cfgb.get_config_from_nested_dict({"key": "val"})
        init_config["result_nid"] = "leaf_node"
        init_config["method"] = "fit"
        df = pd.DataFrame([range(5)], columns=[f"col{i}" for i in range(5)])
        init_config["result_df"] = df
        init_config["column_to_tags"] = {
            "col0": ["feature_col"],
            "col1": ["target_col", "step_0"],
            "col2": ["target_col", "step_1"],
            "col3": ["prediction_col", "step_0"],
            "col4": ["prediction_col", "step_1"],
        }
        init_config["info"] = collections.OrderedDict(
            {"df_info": dtf.get_df_info_as_string(df)}
        )
        init_config["payload"] = None
        return init_config


class TestPredictionResultBundle(hut.TestCase):
    def test_to_config1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual_config = prb.to_config(commit_hash=False)
        self.check_string(f"config without 'commit_hash' field:\n{actual_config}")

    def test_feature_col_names1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.feature_col_names
        expected = ["col0"]
        self.assertListEqual(actual, expected)

    def test_target_col_names1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.target_col_names
        expected = ["col1", "col2"]
        self.assertListEqual(actual, expected)

    def test_prediction_col_names1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.prediction_col_names
        expected = ["col3", "col4"]
        self.assertListEqual(actual, expected)

    def test_get_target_and_prediction_col_names_for_tags1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_target_and_prediction_col_names_for_tags(
            tags=["step_0", "step_1"]
        )
        expected = {"step_0": ("col1", "col3"), "step_1": ("col2", "col4")}
        self.assertDictEqual(actual, expected)

    def test_get_target_and_prediction_col_names_for_tags2(self) -> None:
        """
        Try to extract columns with no target column for given tag.
        """
        init_config = self._get_init_config()
        init_config["column_to_tags"].pop("col1")
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        with self.assertRaises(AssertionError):
            prb.get_target_and_prediction_col_names_for_tags(tags=["step_0"])

    def test_get_target_and_prediction_col_names_for_tags3(self) -> None:
        """
        Extract columns with no target column for another tag.
        """
        init_config = self._get_init_config()
        init_config["column_to_tags"].pop("col1")
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_target_and_prediction_col_names_for_tags(tags=["step_1"])
        expected = {"step_1": ("col2", "col4")}
        self.assertDictEqual(actual, expected)

    def test_get_targets_and_predictions_for_tags1(self) -> None:
        init_config = self._get_init_config()
        prb = dtf.PredictionResultBundle(**init_config.to_dict())
        actual = prb.get_targets_and_predictions_for_tags(
            tags=["step_0", "step_1"]
        )
        expected = {
            "step_0": (pd.Series([1], name="col1"), pd.Series([3], name="col3")),
            "step_1": (pd.Series([2], name="col2"), pd.Series([4], name="col4")),
        }
        # Compare dicts.
        self.assertListEqual(list(actual.keys()), list(expected.keys()))
        for tag, (target, prediction) in actual.items():
            pd.testing.assert_series_equal(target, expected[tag][0])
            pd.testing.assert_series_equal(prediction, expected[tag][1])

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
            "col1": ["target_col", "step_0"],
            "col2": ["target_col", "step_1"],
            "col3": ["prediction_col", "step_0"],
            "col4": ["prediction_col", "step_1"],
        }
        init_config["info"] = collections.OrderedDict(
            {"df_info": dtf.get_df_info_as_string(df)}
        )
        return init_config
