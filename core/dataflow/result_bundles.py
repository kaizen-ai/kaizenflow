from __future__ import annotations

import abc
import collections
import copy
import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import core.config as cfg
import core.config_builders as cfgb
import helpers.dbg as dbg
import helpers.git as git

_LOG = logging.getLogger(__name__)


class ResultBundle(abc.ABC):
    """
    Abstract class for storing DAG results.
    """

    def __init__(
        self,
        config: cfg.Config,
        result_nid: str,
        method: str,
        result_df: pd.DataFrame,
        column_to_tags: Optional[Dict[Any, List[Any]]] = None,
        info: Optional[collections.OrderedDict] = None,
    ) -> None:
        """
        :param config: DAG config
        :param result_nid: identifier of terminal node for which DAG was
            executed
        :param method: method which was executed
        :param result_df: dataframe with results
        :param column_to_tags: mapping of column names to tags
        :param info: DAG execution info
        """
        self._config = config
        self._result_nid = result_nid
        self._method = method
        self._result_df = result_df
        self._column_to_tags = column_to_tags
        self._info = info

    @property
    def config(self) -> Optional[cfg.Config]:
        if self._config is not None:
            return self._config.copy()

    @property
    def result_nid(self) -> Optional[str]:
        return self._result_nid

    @property
    def method(self) -> Optional[str]:
        return self._method

    @property
    def result_df(self) -> Optional[pd.DataFrame]:
        if self._result_df is not None:
            return self._result_df.copy()

    @property
    def column_to_tags(self) -> Optional[Dict[Any, List[Any]]]:
        return copy.deepcopy(self._column_to_tags)

    @property
    def tag_to_columns(self) -> Optional[Dict[Any, List[Any]]]:
        if self._column_to_tags is not None:
            tag_to_columns: Dict[Any, List[Any]] = {}
            for column, tags in self._column_to_tags.items():
                for tag in tags:
                    tag_to_columns.setdefault(tag, []).append(column)
            return tag_to_columns

    @property
    def info(self) -> Optional[collections.OrderedDict]:
        if self._info is not None:
            return self._info.copy()

    def to_config(self, commit_hash: bool = True) -> cfg.Config:
        """
        Represent class state as config.

        :param commit_hash: whether to include current commit hash
        """
        serialized_bundle = cfg.Config()
        serialized_bundle["config"] = self._config
        serialized_bundle["result_nid"] = self._result_nid
        serialized_bundle["method"] = self._method
        serialized_bundle["result_df"] = self._result_df
        serialized_bundle["column_to_tags"] = self._column_to_tags
        info = self._info
        if info is not None:
            info = cfgb.get_config_from_nested_dict(info)
        serialized_bundle["info"] = info
        serialized_bundle["class"] = self.__class__.__name__
        if commit_hash:
            serialized_bundle["commit_hash"] = git.get_current_commit_hash()
        return serialized_bundle

    @classmethod
    def from_config(cls, serialized_bundle: cfg.Config) -> ResultBundle:
        """
        Initialize `ResultBundle` from config.
        """
        rb = cls(
            config=serialized_bundle["config"],
            result_nid=serialized_bundle["result_nid"],
            method=serialized_bundle["method"],
            result_df=serialized_bundle["result_df"],
            column_to_tags=serialized_bundle["column_to_tags"],
            info=serialized_bundle["info"],
        )
        return rb

    def get_tags_for_column(self, column: Any) -> Optional[List[Any]]:
        return ResultBundle._search_mapping(column, self._column_to_tags)

    def get_columns_for_tag(self, tag: Any) -> Optional[List[Any]]:
        return ResultBundle._search_mapping(tag, self.tag_to_columns)

    @staticmethod
    def _search_mapping(
        value: Any, mapping: Optional[Dict[Any, List[Any]]]
    ) -> Optional[List[Any]]:
        if mapping is None:
            _LOG.warning("No mapping provided.")
            return None
        if value not in mapping:
            _LOG.warning("'%s' not in `mapping`='%s'.", value, mapping)
            return None
        return mapping[value]


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
