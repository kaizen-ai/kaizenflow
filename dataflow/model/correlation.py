"""
Import as:

import dataflow.model.correlation as dtfmodcorr
"""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

import core.signal_processing.outliers as csiprout
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


def compute_correlations(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    trim_outliers: bool = False,
    remove_outliers_columns: Optional[List[str]] = None,
    remove_outliers_quantiles: Tuple[Optional[float], Optional[float]] = None,
    allow_unequal_indices: bool = False,
    allow_unequal_columns: bool = False,
) -> pd.DataFrame:
    """
    Compute column-wise correlations between `df1` and `df2`.

    If `df1` and `df2` have two column levels, do this for each level
    zero column grouping.
    """
    if trim_outliers:
        df1 = remove_outliers(
            df1, remove_outliers_columns, remove_outliers_quantiles
        )
        df2 = remove_outliers(
            df2, remove_outliers_columns, remove_outliers_quantiles
        )
    # hpandas.dassert_axes_equal(df1, df2, sort_cols=True)
    if allow_unequal_indices:
        idx = df1.index.intersection(df2.index)
        _LOG.debug("Index intersection size=%d", idx.size)
        hdbg.dassert_lt(0, idx.size, "Intersection of indices is empty.")
        # Restrict dataframes to common index.
        df1 = df1.loc[idx]
        df2 = df2.loc[idx]
    if allow_unequal_columns:
        hpandas.dassert_indices_equal(df1, df2)
    else:
        hpandas.dassert_axes_equal(df1, df2, sort_cols=True)
    # Switch to trim the outliers.
    corrs = []
    n_col_levels = df1.columns.nlevels
    if n_col_levels == 2:
        for col in df1.columns.levels[0]:
            if col in df2.columns.levels[0]:
                _LOG.debug("Compute correlation for col=%s", col)
            else:
                _LOG.info("Skipping col=%s", col)
                continue
            corr = df1[col].corrwith(df2[col])
            hdbg.dassert_isinstance(corr, pd.Series)
            corr.name = col
            corrs.append(corr)
        corrs = pd.concat(corrs, axis=1)
    elif n_col_levels == 1:
        corrs = df1.corrwith(df2)
        hdbg.dassert_isinstance(corrs, pd.Series)
        corrs.name = "correlation"
        corrs = corrs.to_frame()
    else:
        raise ("Number of column levels must be 1 or 2 but is=%d", n_col_levels)
    return corrs


def detect_outliers(
    df: pd.DataFrame,
    remove_outliers_columns: List[str],
    remove_outliers_quantiles: Tuple[Optional[float], Optional[float]],
) -> Dict[str, pd.Series]:
    """
    Collect the outliers for given columns.

    :param remove_outliers_columns: see description in `remove_outliers()`
    :param remove_outliers_quantiles: lower and upper quantiles (see description in `csiprout.remove_outliers()`)
    """
    outlier_placeholder = {}
    # Assert that `remove_outliers_columns` in df.columns.
    hdbg.dassert_is_subset(remove_outliers_columns, df.columns)
    for col in remove_outliers_columns:
        srs_temp = df[col]
        # Collect the number of NaNs in the initial data.
        len(srs_temp[srs_temp.isna()])
        # Drop NaNs in the initial data, so they don't confuse the results later.
        srs_temp = srs_temp.dropna()
        # Identify outliers.
        srs_cleaned = csiprout.process_outliers(
            srs=srs_temp,
            mode="set_to_nan",
            lower_quantile=remove_outliers_quantiles[0],
            upper_quantile=remove_outliers_quantiles[1],
        )
        # Extract outliers' indices.
        outlier_placeholder[col] = srs_cleaned[srs_cleaned.isna()].index
    return outlier_placeholder


def remove_outliers(
    df: pd.DataFrame,
    remove_outliers_columns: List[str],
    remove_outliers_quantiles: Tuple[Optional[float], Optional[float]],
) -> pd.DataFrame:
    """
    Remove rows with outliers for each given column.

    :param remove_outliers_columns: list of columns to proceed
    :param remove_outliers_quantiles: lower and upper quantiles (see description in `csiprout.remove_outliers()`)
    :return: Data with removed rows with outliers
    """
    outlier_index_placeholder = detect_outliers(
        df, remove_outliers_columns, remove_outliers_quantiles
    )
    # Collect indices that correspond to outlier values.
    indices_to_remove = []
    for key in outlier_index_placeholder.keys():
        value_temp = list(outlier_index_placeholder[key])
        indices_to_remove = indices_to_remove + value_temp
    indices_to_remove = list(set(indices_to_remove))
    # Remove outliers from initial df.
    df = df.drop(indices_to_remove)
    return df
