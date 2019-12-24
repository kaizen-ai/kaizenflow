"""
Import as:

import core.statistics as stats
"""

import functools
import logging
import math
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import scipy as sp
import sklearn.model_selection
import statsmodels as sm

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)

MEAN_COL = "mean"
STD_COL = "std"
SKEW_COL = "skew"
KURT_COL = "kurt"
PVAL_COL = "pvals"
TVAL_COL = "tvals"
ADJ_PVAL_COL = "adj_pvals"


# #############################################################################
# Descriptive statistics
# #############################################################################


# TODO(Paul): Double-check axes in used in calculation.
# Consider exposing `nan_policy`.
def moments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates, mean, standard deviation, skew, and kurtosis.
    """
    mean = df.mean()
    std = df.std()
    skew = sp.stats.skew(df, nan_policy="omit")
    kurt = sp.stats.kurtosis(df, nan_policy="omit")
    result = pd.DataFrame(
        {MEAN_COL: mean, STD_COL: std, SKEW_COL: skew, KURT_COL: kurt},
        index=df.columns,
    )
    return result


# TODO(Paul): Refactor so that these work with dataframes as well. The
# underlying numpy implementations extend to multidimensional arrays.
def replace_infs_with_nans(
    srs: pd.Series
) -> pd.Series:
    """
    Replace infs with nans in a copy of `srs`.
    """
    return srs.replace([np.inf, -np.inf], np.nan)


def compute_frac_zero(
    srs: pd.Series,
    rtol: float = 0.0,
    atol: float = 0.0,
) -> float:
    """
    Calculate fraction of zeros in `srs`.

    The size of `srs` is used as the denominator.

    :param rtol: absolute tolerance, as in `np.isclose`
    :param atol: relative tolerance, as in `np.isclose`
    """
    if series.empty:
        _LOG.warning("Series is empty")
        return np.nan
    zeros = np.zeros(srs.size)
    num_zeros = np.isclose(srs.values, zeros, rtol, atol).sum()
    return num_zeros / srs.size


def compute_frac_nan(srs: pd.Series) -> float:
    """
    Calculate fraction of nans in `srs`.

    The size of `srs` is used as the denominator.

    :param mode: keep_orig - keep series without any change, so the denominator
        of the fraction is computed in the normal way.
        drop_inf - don't count inf rows for the denominator
    """
    if series.empty:
        _LOG.warning("Series is empty")
        return np.nan
    num_nans = srs.isna().sum()
    return num_nans / srs.size


def compute_frac_inf(srs: pd.Series) -> float:
    """
    Count fraction of infs in a given time series.

    :param mode: keep_orig - keep series (denominator) without any change
        drop_na - drop nans before counting series rows for the denominator
    """
    if series.empty:
        _LOG.warning("Series is empty")
        return np.nan
    num_infs = np.isinf(srs.values).sum()
    return num_infs / srs.size


def compute_frac_constant(
    series: pd.Series, mode: str = 'keep_orig'
) -> float:
    """
    Compute fraction of values in the series that changes at the next timestamp.

    :param mode: keep_orig - keep series without any change
        drop_na_inf - drop nans and infs
    """
    if series.empty:
        _LOG.warning("Series is empty")
        return np.nan
    else:
        if mode == 'drop_na_inf':
            series = replace_infs_with_nans(series).dropna()
        elif mode == 'keep_orig':
            pass
        else:
            raise ValueError("Unsupported mode=`%s`" % mode)
        changes = series.dropna().diff()
        num_changes = changes[changes != 0].shape[0]
        frac_changes = 1 - num_changes / series.shape[0]
    return frac_changes


def count_num_finite_samples(
    series: pd.Series, mode: str = 'drop_inf'
) -> int:
    """
    Count number of finite data points in a given time series.

    :param mode: drop_inf - drop infs
        drop_na_inf - drop nans and infs
    """
    if series.empty:
        _LOG.warning("Series is empty")
        num_samples = np.nan
    else:
        if mode == 'drop_na_inf':
            series = replace_infs_with_nans(series).dropna()
        elif mode == 'drop_inf':
            series = replace_infs_with_nans(series)
        else:
            raise ValueError("Unsupported mode=`%s`" % mode)
        num_samples = series.shape[0]
    return num_samples


def count_num_unique_values(
    series: pd.Series, mode: str = 'keep_orig'
) -> int:
    """
    Count number of unique values in the series.

    :param mode: keep_orig - keep series without any change
        drop_na_inf - drop nans and infs
    """
    if series.empty:
        _LOG.warning("Series is empty")
        num_unique_values = np.nan
    else:
        if mode == 'drop_na_inf':
            series = replace_infs_with_nans(series).dropna()
        elif mode == 'keep_orig':
            pass
        else:
            raise ValueError("Unsupported mode=`%s`" % mode)
        num_unique_values = len(series.unique())
    return num_unique_values


# #############################################################################
# Cross-validation
# #############################################################################


def get_rolling_splits(
    idx: pd.Index, n_splits: int
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Partition index into chunks and returns pairs of successive chunks.

    If the index looks like
        [0, 1, 2, 3, 4, 5, 6]
    and n_splits = 4, then the splits would be
        [([0, 1], [2, 3]),
         ([2, 3], [4, 5]),
         ([4, 5], [6])]

    A typical use case is where the index is a monotonic increasing datetime
    index. For such cases, causality is respected by the splits.
    """
    dbg.dassert_monotonic_index(idx)
    n_chunks = n_splits + 1
    dbg.dassert_lte(1, n_splits)
    # Split into equal chunks.
    chunk_size = int(math.ceil(idx.size / n_chunks))
    dbg.dassert_lte(1, chunk_size)
    chunks = [idx[i : i + chunk_size] for i in range(0, idx.size, chunk_size)]
    dbg.dassert_eq(len(chunks), n_chunks)
    #
    splits = list(zip(chunks[:-1], chunks[1:]))
    return splits


def get_oos_start_split(
    idx: pd.Index, datetime_
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Split index using OOS (out-of-sample) start datetime.
    """
    dbg.dassert_monotonic_index(idx)
    ins_mask = idx < datetime_
    dbg.dassert_lte(1, ins_mask.sum())
    oos_mask = ~ins_mask
    dbg.dassert_lte(1, oos_mask.sum())
    ins = idx[ins_mask]
    oos = idx[oos_mask]
    return [(ins, oos)]


# TODO(Paul): Support train/test/validation or more.
def get_train_test_pct_split(
    idx: pd.Index, train_pct: float
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Split index into train and test sets by percentage.
    """
    dbg.dassert_monotonic_index(idx)
    dbg.dassert_lt(0.0, train_pct)
    dbg.dassert_lt(train_pct, 1.0)
    #
    train_size = int(train_pct * idx.size)
    dbg.dassert_lte(0, train_size)
    train_split = idx[:train_size]
    test_split = idx[train_size:]
    return [(train_split, test_split)]


def get_expanding_window_splits(
    idx: pd.Index, n_splits: int
) -> List[Tuple[pd.Index, pd.Index]]:
    """
    Generate splits with expanding overlapping windows.
    """
    dbg.dassert_monotonic_index(idx)
    dbg.dassert_lte(1, n_splits)
    tscv = sklearn.model_selection.TimeSeriesSplit(n_splits=n_splits)
    locs = list(tscv.split(idx))
    splits = [(idx[loc[0]], idx[loc[1]]) for loc in locs]
    return splits


def truncate_index(idx: pd.Index, min_idx, max_idx) -> pd.Index:
    """
    Return subset of idx with values >= min_idx and < max_idx.
    """
    dbg.dassert_monotonic_index(idx)
    # TODO(*): PartTask667: Consider using bisection to avoid linear scans.
    min_mask = idx >= min_idx
    max_mask = idx < max_idx
    mask = min_mask & max_mask
    dbg.dassert_lte(1, mask.sum())
    return idx[mask]


def combine_indices(idxs: Iterable[pd.Index]) -> pd.Index:
    """
    Combine multiple indices into a single index for cross-validation splits.

    This is computed as the union of all the indices within the largest common
    interval.

    TODO(Paul): Consider supporting multiple behaviors with `mode`.
    """
    for idx in idxs:
        dbg.dassert_monotonic_index(idx)
    # Find the maximum start/end datetime overlap of all source indices.
    max_min = max([idx.min() for idx in idxs])
    _LOG.debug("Latest start datetime of indices=%s", max_min)
    min_max = min([idx.max() for idx in idxs])
    _LOG.debug("Earliest end datetime of indices=%s", min_max)
    truncated_idxs = [truncate_index(idx, max_min, min_max) for idx in idxs]
    # Take the union of truncated indices. Though all indices fall within the
    # datetime range [max_min, min_max), they do not necessarily have the same
    # resolution or all values.
    composite_idx = functools.reduce(lambda x, y: x.union(y), truncated_idxs)
    return composite_idx


def convert_splits_to_string(splits):
    txt = "n_splits=%s\n" % len(splits)
    for train_idxs, test_idxs in splits:
        txt += "train=%s [%s, %s]" % (
            len(train_idxs),
            min(train_idxs),
            max(train_idxs),
        )
        txt += "\n"
        txt += "test=%s [%s, %s]" % (
            len(test_idxs),
            min(test_idxs),
            max(test_idxs),
        )
        txt += "\n"
    return txt


# #############################################################################
# Hypothesis testing
# #############################################################################


def ttest_1samp(
    df: pd.DataFrame,
    popmean: Optional[float] = None,
    nan_policy: Optional[str] = None,
) -> pd.DataFrame:
    """
    Thin wrapper around scipy's ttest.

    WARNING: Passing in df.dropna(how='all') vs df.dropna() (which defaults to
    'any') can yield different results. Safest is to NOT DROP NANs in the input
    and instead use `nan_policy='omit'`.

    :param df: DataFrame with samples along rows, groups along columns.
    :param popmean: assumed population mean for test
    :param nan_policy: `nan_policy` for scipy's ttest_1samp
    :return: DataFrame with t-value and p-value columns, rows like df's columns
    """
    if popmean is None:
        popmean = 0
    if nan_policy is None:
        nan_policy = "omit"
    tvals, pvals = sp.stats.ttest_1samp(
        df, popmean=popmean, nan_policy=nan_policy
    )
    result = pd.DataFrame({TVAL_COL: tvals, PVAL_COL: pvals}, index=df.columns)
    return result


def multipletests(srs: pd.Series, method: Optional[str] = None) -> pd.Series:
    """
    Wrap statsmodel's multipletests.

    Returns results in a series indexed like srs.
    Documentation at
    https://www.statsmodels.org/stable/generated/statsmodels.stats.multitest.multipletests.html

    :param srs: Series with pvalues
    :param method: `method` for scipy's multipletests
    :return: Series of adjusted p-values
    """
    if method is None:
        method = "fdr_bd"
    pvals_corrected = sm.stats.multitest.multipletests(srs, method=method)[1]
    return pd.Series(pvals_corrected, index=srs.index, name=ADJ_PVAL_COL)


def multi_ttest(
    df: pd.DataFrame,
    popmean: Optional[float] = None,
    nan_policy: Optional[str] = None,
    method: Optional[str] = None,
) -> pd.DataFrame:
    """
    Combine ttest and multitest pvalue adjustment.
    """
    ttest = ttest_1samp(df, popmean=popmean, nan_policy=nan_policy)
    ttest[ADJ_PVAL_COL] = multipletests(ttest[PVAL_COL], method=method)
    return ttest
