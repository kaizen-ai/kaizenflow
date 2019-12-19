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


# TODO: Some functions could result in error with drop_na = False. Test
#  behaviour of function with drop_na = False.
def drop_na_inf_if_needed(
    series: pd.Series, drop_na: bool = True, drop_inf: bool = True
) -> pd.Series:
    """
    Remove nans and infs from series if corresponding param is True.
    """
    if drop_inf:
        series = series.replace([np.inf, -np.inf], np.nan).dropna()
    if drop_na:
        series = series.dropna()
    return series


def compute_pct_zero(
    series: pd.Series,
    zero_threshold: float = 1e-9,
    drop_na: bool = True,
    drop_inf: bool = True,
) -> float:
    """
    Count number of zeroes in a given time series.

    :param zero_threshold: floats smaller than this are treated as zeroes.
    """
    if series.empty:
        _LOG.warning("Series is empty")
        pct_zeros = np.nan
    else:
        series = drop_na_inf_if_needed(series, drop_na=drop_na, drop_inf=drop_inf)
        num_rows = series.shape[0]
        num_zeros = (series.dropna().abs() < zero_threshold).sum()
        pct_zeros = 100.0 * num_zeros / num_rows
    return pct_zeros


def compute_pct_nan(series: pd.Series, drop_inf: bool = True) -> float:
    """
    Count number of nans in a given time series.
    """
    if series.empty:
        _LOG.warning("Series is empty")
        pct_nan = np.nan
    else:
        series = drop_na_inf_if_needed(series, drop_na=False, drop_inf=drop_inf)
        num_rows = series.shape[0]
        num_nans = series.isna().sum()
        pct_nan = 100.0 * num_nans / num_rows
    return pct_nan


def compute_pct_inf(series: pd.Series, drop_na: bool = True) -> float:
    """
    Count number of infs in a given time series.
    """
    if series.empty:
        _LOG.warning("Series is empty")
        pct_inf = np.nan
    else:
        series = drop_na_inf_if_needed(series, drop_na=drop_na, drop_inf=False)
        num_rows = series.shape[0]
        num_infs = series.dropna().apply(np.isinf).sum()
        pct_inf = 100.0 * num_infs / num_rows
    return pct_inf


def compute_pct_changes(
    series: pd.Series, drop_na: bool = True, drop_inf: bool = True
) -> float:
    """
    Compute percentage of values in the series that changes at the next timestamp.
    """
    if series.empty:
        _LOG.warning("Series is empty")
        pct_changes = np.nan
    else:
        series = drop_na_inf_if_needed(series, drop_na=drop_na, drop_inf=drop_inf)
        changes = series.dropna().diff()
        changes_count = changes[changes != 0].shape[0]
        pct_changes = changes_count / series.shape[0] * 100
    return pct_changes


def count_num_samples(
    series: pd.Series, drop_na: bool = True, drop_inf: bool = True
) -> int:
    """
    Count number of data points in a given time series.
    """
    series = drop_na_inf_if_needed(series, drop_na=drop_na, drop_inf=drop_inf)
    return series.shape[0]


def count_num_unique_values(
    series: pd.Series, drop_na: bool = True, drop_inf: bool = True
) -> int:
    """
    Count number of unique values in the series.
    """
    series = drop_na_inf_if_needed(series, drop_na=drop_na, drop_inf=drop_inf)
    return len(series.unique())


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
