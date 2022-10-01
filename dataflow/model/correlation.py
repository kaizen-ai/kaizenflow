"""
Import as:

import dataflow.model.correlation as dtfmodcorr
"""

import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


def compute_correlations(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    allow_unequal_indices: bool = False,
    allow_unequal_columns: bool = False,
) -> pd.DataFrame:
    """
    Compute column-wise correlations between `df1` and `df2`.

    If `df1` and `df2` have two column levels, do this for each level
    zero column grouping.
    """
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