import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

import helpers.dbg as dbg
from core.dataflow.builder import ResultBundle

_LOG = logging.getLogger(__name__)


class PredictionResultBundle(ResultBundle):
    @property
    def feature_col_names(self) -> List[Any]:
        return self.get_columns_for_tag("feature_col")

    @property
    def target_col_names(self) -> List[Any]:
        return self.get_columns_for_tag("target_col")

    @property
    def prediction_col_names(self) -> List[Any]:
        return self.get_columns_for_tag("prediction_col")

    def get_target_and_prediction_col_names_for_tags(
        self, tags: List[Any]
    ) -> Dict[Any, Tuple[Any, Any]]:
        dbg.dassert_isinstance(tags, list)
        target_cols = set(self.target_col_names)
        prediction_cols = set(self.prediction_col_names)
        tags_to_target_and_prediction_cols: Dict[Any, Tuple[Any, Any]] = {}
        for tag in tags:
            cols_for_tag = self.get_columns_for_tag(tag)
            target_cols_for_tag = list(target_cols.intersection(cols_for_tag))
            dbg.dassert_eq(
                len(target_cols_for_tag),
                1,
                "Found `%s`!=`1` target columns for tag '%s'.",
                len(target_cols_for_tag),
                tag,
            )
            prediction_cols_for_tag = list(
                prediction_cols.intersection(cols_for_tag)
            )
            dbg.dassert_eq(
                len(prediction_cols_for_tag),
                1,
                "Found `%s`!=`1` prediction columns for tag '%s'.",
                len(prediction_cols_for_tag),
                tag,
            )
            tags_to_target_and_prediction_cols[tag] = (
                target_cols_for_tag[0],
                prediction_cols_for_tag[0],
            )
        return tags_to_target_and_prediction_cols

    @property
    def features(self) -> pd.DataFrame:
        return self.result_df[self.feature_col_names]

    @property
    def targets(self) -> pd.DataFrame:
        return self.result_df[self.target_col_names]

    @property
    def predictions(self) -> pd.DataFrame:
        return self.result_df[self.prediction_col_names]

    def get_targets_and_predictions_for_tags(
        self, tags: List[Any]
    ) -> Dict[Any, Tuple[pd.Series, pd.Series]]:
        tags_to_target_and_prediction_cols = (
            self.get_target_and_prediction_col_names_for_tags(tags)
        )
        targets_and_predictions_for_tags = {
            tag: (self.result_df[target_col], self.result_df[prediction_col])
            for tag, (
                target_col,
                prediction_col,
            ) in tags_to_target_and_prediction_cols.items()
        }
        return targets_and_predictions_for_tags
