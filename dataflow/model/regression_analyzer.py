"""
Import as:

import dataflow.model.regression_analyzer as dtfmoreana
"""

from __future__ import annotations

import datetime
import logging
from typing import List, Optional, Union

import pandas as pd
import seaborn as sns
from tqdm.autonotebook import tqdm

import core.statistics as costatis
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


class RegressionAnalyzer:
    """
    Regress target col against feature cols independently.
    """

    def __init__(
        self,
        target_col: Union[int, str],
        feature_cols: List[Union[int, str]],
        *,
        feature_lag: int = 0,
    ) -> None:
        """
        Initialize column names.
        """
        hdbg.dassert_isinstance(feature_cols, list)
        self._target_col = target_col
        self._feature_cols = feature_cols
        self._df_cols = self._feature_cols + [self._target_col]
        self._feature_lag = feature_lag

    def compute_regression_coefficients(
        self,
        df: pd.DataFrame,
        *,
        start_datetime: Optional[pd.Timestamp] = None,
        end_datetime: Optional[pd.Timestamp] = None,
        start_time: datetime.time = datetime.time(9, 30),
        end_time: datetime.time = datetime.time(16, 00),
    ) -> pd.DataFrame:
        """
        Compute regression coefficients.
        """
        self._validate_data_df(df)
        df = df[self._df_cols]
        df = df.loc[start_datetime:end_datetime]
        df = df.between_time(start_time, end_time)
        coeffs = {}
        asset_ids = df.columns.levels[1].to_list()
        _LOG.debug("Num asset_ids=%d", len(asset_ids))
        for asset_id in tqdm(asset_ids, desc="Processing assets"):
            asset_df = df.T.xs(asset_id, level=1).T
            if asset_df.empty:
                _LOG.debug("Empty dataframe for asset_id=%d", asset_id)
                continue
            if self._feature_lag != 0:
                features = asset_df[self._feature_cols].shift(self._feature_lag)
                target = asset_df[[self._target_col]]
                asset_df = features.merge(
                    target, left_index=True, right_index=True
                )
            coeff = costatis.compute_regression_coefficients(
                asset_df, self._feature_cols, self._target_col
            )
            coeffs[asset_id] = coeff
        out_df = pd.concat(coeffs)
        return out_df

    @staticmethod
    def compute_moments(df: pd.DataFrame, stats: List[str]) -> pd.DataFrame:
        """
        Compute moments by feature for a given statistic.

        Dataframe columns are statistics (e.g., "beta"). Rows are
        multiindex, with level 0 equal to the name and level 1 equal to
        the feature.
        """
        all_moments = {}
        for stat in stats:
            moments = []
            for feature in df.index.unique(level=1):
                val = costatis.compute_moments(
                    df[stat].xs(feature, level=1)
                ).rename(feature)
                moments.append(val)
            moments = pd.concat(moments, axis=1)
            all_moments[stat] = moments
        out_df = pd.concat(all_moments).transpose()
        return out_df

    def combine_features(
        self,
        df: pd.DataFrame,
        weights: List[float],
    ) -> pd.DataFrame:
        """
        Generate one column per asset by combining features with weights.
        """
        self._validate_data_df(df)
        df = df[self._df_cols]
        weight_srs = pd.Series(weights, self._feature_cols, name="weight")
        _LOG.debug("weights=\n%s", weight_srs)
        predictions = {}
        # TODO(Paul): Consider performing the calculation as in
        #  `ForecastMixer.generate_portfolio_bar_metrics_df()`.
        asset_ids = df.columns.levels[1].to_list()
        for asset_id in tqdm(asset_ids, desc="Processing assets"):
            asset_df = df.T.xs(asset_id, level=1).T
            if asset_df.empty:
                _LOG.debug("Empty dataframe for asset_id=%d", asset_id)
                continue
            prediction = (asset_df * weight_srs).sum(axis=1, min_count=1)
            predictions[asset_id] = prediction
        out_df = pd.concat(predictions).unstack(level=0)
        return out_df

    def show_pairplot(
        self,
        df: pd.DataFrame,
        statistic: str,
        feature: Union[int, str],
        *,
        start_datetime_1: Optional[pd.Timestamp] = None,
        end_datetime_1: Optional[pd.Timestamp] = None,
        start_datetime_2: Optional[pd.Timestamp] = None,
        end_datetime_2: Optional[pd.Timestamp] = None,
        start_time_1: datetime.time = datetime.time(9, 30),
        end_time_1: datetime.time = datetime.time(16, 00),
        start_time_2: datetime.time = datetime.time(9, 30),
        end_time_2: datetime.time = datetime.time(16, 00),
    ) -> None:
        """
        Show a paired plot of `statistics` for `feature`.
        """
        split1 = self.compute_regression_coefficients(
            df,
            start_datetime=start_datetime_1,
            end_datetime=end_datetime_1,
            start_time=start_time_1,
            end_time=end_time_1,
        )
        split2 = self.compute_regression_coefficients(
            df,
            start_datetime=start_datetime_2,
            end_datetime=end_datetime_2,
            start_time=start_time_2,
            end_time=end_time_2,
        )
        srs1 = split1[statistic].xs(feature, level=1).rename("split1")
        srs2 = split2[statistic].xs(feature, level=1).rename("split2")
        paired_df = pd.concat([srs1, srs2], join="inner", axis=1)
        sns.pairplot(paired_df)

    def _validate_data_df(self, df):
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert_eq(df.columns.nlevels, 2)
        for col in self._df_cols:
            hdbg.dassert_in(col, df.columns)