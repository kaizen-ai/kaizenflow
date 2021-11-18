"""
Import as:

import core.dataflow_model.regression_analyzer as cdtfmorean
"""

from __future__ import annotations

import logging
from typing import List, Optional, Union

import pandas as pd

import core.dataflow_model.utils as cdtfmouti
import core.statistics as costatis
import helpers.datetime_ as hdateti

_LOG = logging.getLogger(__name__)


def compute_moments(df: pd.DataFrame, stats: List[str]) -> pd.DataFrame:
    """
    Compute moments by feature for a given statistic.

    Dataframe columns are statistics (e.g., "beta"). Rows are multiindex, with
    level 0 equal to the name and level 1 equal to the feature.
    """
    all_moments = {}
    for stat in stats:
        moments = []
        for feature in df.index.unique(level=1):
            val = costatis.compute_moments(df[stat].xs(feature, level=1)).rename(
                feature
            )
            moments.append(val)
        moments = pd.concat(moments, axis=1)
        all_moments[stat] = moments
    out_df = pd.concat(all_moments).transpose()
    return out_df


def compute_coefficients(
    src_dir: str,
    file_name: str,
    feature_cols: List[Union[int, str]],
    target_col: str,
    start: Optional[hdateti.Datetime],
    end: Optional[hdateti.Datetime],
) -> pd.DataFrame:
    coeffs = {}
    artifact_iter = cdtfmouti.yield_experiment_artifacts(
        src_dir=src_dir,
        file_name=file_name,
        load_rb_kwargs={"columns": feature_cols + [target_col]},
    )
    for key, artifact in artifact_iter:
        if df.empty:
            continue
        df = artifact.result_df.loc[start:end].copy()
        coeff = costatis.compute_regression_coefficients(
            df, feature_cols, target_col
        )
        coeffs[key] = coeff
    out_df = pd.concat(coeffs)
    return out_df
