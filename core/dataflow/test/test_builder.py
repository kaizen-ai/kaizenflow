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
        rb = dtf.ResultBundle(**init_config.to_dict())
        actual_config = rb.to_config()
        actual_config.pop("commit_hash")
        self.check_string(f"config without 'commit_hash' field:\n{actual_config}")

    def test_from_config1(self) -> None:
        init_config = self._get_init_config()
        rb = dtf.ResultBundle.from_config(init_config)
        actual_config = rb.to_config()
        actual_config.pop("commit_hash")
        self.check_string(f"config without 'commit_hash' field:\n{actual_config}")

    def test_get_tags_for_column1(self) -> None:
        init_config = self._get_init_config()
        rb = dtf.ResultBundle(**init_config.to_dict())
        actual = rb.get_tags_for_column("col2")
        expected = ["target_col", 1]
        self.assertListEqual(actual, expected)

    def test_get_columns_for_tag1(self) -> None:
        init_config = self._get_init_config()
        rb = dtf.ResultBundle(**init_config.to_dict())
        actual = rb.get_columns_for_tag(1)
        expected = ["col2", "col4"]
        self.assertListEqual(actual, expected)

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
