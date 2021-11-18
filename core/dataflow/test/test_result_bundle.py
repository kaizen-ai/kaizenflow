import collections
import logging
import os

import pandas as pd

import core.config as cconfig

# TODO(gp): Use
# import core.dataflow.result_bundle as cdtfrb
import core.dataflow as dtf
import helpers.printing as hprint
import helpers.unit_test as hut

_LOG = logging.getLogger(__name__)


class TestResultBundle(hut.TestCase):
    def test_to_config1(self) -> None:
        """
        Convert a `ResultBundle` to a config.
        """
        rb = self._get_result_bundle()
        # Check.
        actual_config = rb.to_config(commit_hash=False)
        txt = f"config without 'commit_hash' field:\n{actual_config}"
        self.check_string(txt)

    def test_from_config1(self) -> None:
        """
        Initialize a `ResultBundle` from a config.
        """
        # Initialize a `ResultBundle` from a config.
        init_config = self._get_init_config()
        rb = dtf.ResultBundle.from_config(init_config)
        # Check.
        actual_config = rb.to_config(commit_hash=False)
        txt = f"config without 'commit_hash' field:\n{actual_config}"
        self.check_string(txt)

    def test_to_dict_and_back(self) -> None:
        """
        Round-trip conversion using `from_config()` and `to_config()`.
        """
        # Initialize a `ResultBundle` from a config.
        init_config = self._get_init_config()
        result_bundle = dtf.ResultBundle.from_config(init_config)
        # This pattern is used in `master_experiment.py` before pickling.
        rb_as_dict = result_bundle.to_config().to_dict()
        # After unpickling, we convert to a `Config`, then to a `ResultBundle`.
        result_bundle_2 = dtf.ResultBundle.from_config(
            cconfig.get_config_from_nested_dict(rb_as_dict)
        )
        # Check.
        self.assert_equal(str(result_bundle), str(result_bundle_2))

    def test_pickle1(self) -> None:
        rb = self._get_result_bundle()
        # Serialize.
        dir_name = self.get_scratch_space()
        file_name = os.path.join(dir_name, "result_bundle.pkl")
        rb.to_pickle(file_name, use_pq=False)
        # Compute the signature of the dir.
        actual = hut.get_dir_signature(dir_name, include_file_content=False)
        # Check.
        expected = """
        # Dir structure
        $GIT_ROOT/core/dataflow/test/TestResultBundle.test_pickle1/tmp.scratch
        $GIT_ROOT/core/dataflow/test/TestResultBundle.test_pickle1/tmp.scratch/result_bundle.v1_0.pkl
        """
        expected = hprint.dedent(expected)
        self.assert_equal(str(actual), str(expected), purify_text=True)

    def test_get_tags_for_column1(self) -> None:
        rb = self._get_result_bundle()
        #
        actual = rb.get_tags_for_column("col2")
        expected = ["target_col", "step_1"]
        self.assert_equal(str(actual), str(expected))

    def test_get_columns_for_tag1(self) -> None:
        rb = self._get_result_bundle()
        #
        actual = rb.get_columns_for_tag("step_1")
        expected = ["col2", "col4"]
        self.assert_equal(str(actual), str(expected))

    @staticmethod
    def _get_init_config() -> cconfig.Config:
        # TODO(gp): Factor out common part.
        df = pd.DataFrame([range(5)], columns=[f"col{i}" for i in range(5)])
        init_config = cconfig.get_config_from_nested_dict(
            {
                "config": {
                    "key": "val",
                },
                "result_nid": "leaf_node",
                "method": "fit",
                "result_df": df,
                "column_to_tags": {
                    "col0": ["feature_col"],
                    "col1": ["target_col", "step_0"],
                    "col2": ["target_col", "step_1"],
                    "col3": ["prediction_col", "step_0"],
                    "col4": ["prediction_col", "step_1"],
                },
                "info": {"df_info": dtf.get_df_info_as_string(df)},
                "payload": None,
            }
        )
        return init_config

    def _get_result_bundle(self) -> dtf.ResultBundle:
        """
        Initialize a `ResultBundle` from a config.
        """
        init_config = self._get_init_config()
        rb = dtf.ResultBundle.from_config(init_config)
        return rb


# ##################################################################################


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
    def _get_init_config() -> cconfig.Config:
        init_config = cconfig.Config()
        init_config["config"] = cconfig.get_config_from_nested_dict(
            {"key": "val"}
        )
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
