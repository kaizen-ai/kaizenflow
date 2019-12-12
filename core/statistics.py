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


# TODO(*): move to gen_utils.py as safe_div_nan?
def safe_div(a, b):
    div = a / b if b != 0 else np.nan
    return div


def count_pct_zero(
        series: pd.Series, zero_threshold: float = 1e-9
) -> float:
    """
    Count number of zeroes in a given time series.

    :param zero_threshold: floats smaller than this are treated as zeroes.
    """
    num_rows = series.shape[0]
    num_zeros = (series.dropna().abs() < zero_threshold).sum()
    return 100.0 * safe_div(num_zeros, num_rows)


def count_pct_nan(series: pd.Series) -> float:
    """
    Count number of nans in a given time series.
    """
    num_rows = series.shape[0]
    num_nans = series.isna().sum()
    return 100.0 * safe_div(num_nans, num_rows)


def count_pct_inf(series: pd.Series) -> float:
    """
    Count number of infs in a given time series.
    """
    num_rows = series.shape[0]
    num_infs = series.dropna().apply(np.isinf).sum()
    return 100.0 * safe_div(num_infs, num_rows)


def count_num_samples(series: pd.Series) -> int:
    """
    Count number of data points in a given time series.
    """
    return series.shape[0]


def count_num_unique_values(series: pd.Series) -> int:
    """
    Count number of unique values in the series.
    """
    return len(series.unique())


def count_pct_changes(series: pd.Series) -> float:
    """
    Compute percentage of values in the series that changes at the next timestamp.
    """
    changes = series.dropna().pct_change()
    changes_count = changes[changes != 0].shape[0]
    return safe_div(changes_count, series.shape[0])



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


