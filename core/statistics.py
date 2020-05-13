"""
Import as:

import core.statistics as stats
"""

import functools
import logging
import math
from typing import Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import scipy as sp
import sklearn.model_selection
import statsmodels as sm

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# #############################################################################
# Descriptive statistics
# #############################################################################


# TODO(Paul): Double-check axes in used in calculation.
# Consider exposing `nan_policy`.
def compute_moments(data: Union[pd.Series, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate, mean, standard deviation, skew, and kurtosis.

    :param data: if a dataframe, columns correspond to data sets
    :return: dataframe with columns like `df`'s (or a single column if input
        is a series) and rows with stats
    """
    if isinstance(data, pd.Series):
        data = data.to_frame()
    dbg.dassert_isinstance(data, pd.DataFrame)
    mean = data.mean()
    std = data.std()
    skew = sp.stats.skew(data, nan_policy="omit")
    kurt = sp.stats.kurtosis(data, nan_policy="omit")
    result = pd.DataFrame(
        {"mean": mean, "std": std, "skew": skew, "kurtosis": kurt},
        index=data.columns,
    ).transpose()
    return result


# TODO(*): Move this function out of this library.
def replace_infs_with_nans(
    data: Union[pd.Series, pd.DataFrame],
) -> Union[pd.Series, pd.DataFrame]:
    """
    Replace infs with nans in a copy of `data`.
    """
    return data.replace([np.inf, -np.inf], np.nan)


def compute_frac_zero(
    data: Union[pd.Series, pd.DataFrame],
    atol: float = 0.0,
    axis: Optional[int] = 0,
) -> Union[float, pd.Series]:
    """
    Calculate fraction of zeros in a numerical series or dataframe.

    :param data: numeric series or dataframe
    :param atol: absolute tolerance, as in `np.isclose`
    :param axis: numpy axis for summation
    """
    # Create an ndarray of zeros of the same shape.
    zeros = np.zeros(data.shape)
    # Compare values of `df` to `zeros`.
    is_close_to_zero = np.isclose(data.values, zeros, atol=atol)
    num_zeros = is_close_to_zero.sum(axis=axis)
    return _compute_denominator_and_package(num_zeros, data, axis)


def compute_frac_nan(
    data: Union[pd.Series, pd.DataFrame], axis: Optional[int] = 0
) -> Union[float, pd.Series]:
    """
    Calculate fraction of nans in `data`.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    num_nans = data.isna().values.sum(axis=axis)
    return _compute_denominator_and_package(num_nans, data, axis)


def compute_frac_inf(
    data: Union[pd.Series, pd.DataFrame], axis: Optional[int] = 0
) -> Union[float, pd.Series]:
    """
    Count fraction of infs in a numerical series or dataframe.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    num_infs = np.isinf(data.values).sum(axis=axis)
    return _compute_denominator_and_package(num_infs, data, axis)


# TODO(Paul): Consider exposing `rtol`, `atol`.
def compute_frac_constant(
    data: Union[pd.Series, pd.DataFrame]
) -> Union[float, pd.Series]:
    """
    Compute fraction of values in the series that changes at the next timestamp.

    :param data: numeric series or dataframe
    :param axis: numpy axis for summation
    """
    diffs = data.diff().iloc[1:]
    constant_frac = compute_frac_zero(diffs, axis=0)
    return constant_frac


# TODO(Paul): Refactor to work with dataframes as well. Consider how to handle
#     `axis`, which the pd.Series version of `copy()` does not take.
def count_num_finite_samples(data: pd.Series) -> float:
    """
    Count number of finite data points in a given time series.

    :param data: numeric series or dataframe
    """
    data = data.copy()
    data = replace_infs_with_nans(data)
    return data.count()


# TODO(Paul): Extend to dataframes.
def count_num_unique_values(data: pd.Series) -> int:
    """
    Count number of unique values in the series.
    """
    srs = pd.Series(data=data.unique())
    return count_num_finite_samples(srs)


def _compute_denominator_and_package(
    reduction: Union[float, np.ndarray],
    data: Union[pd.Series, pd.DataFrame],
    axis: Optional[float] = None,
):
    """
    Normalize and package `reduction` according to `axis` and `data` metadata.

    This is a helper function used for several `compute_frac_*` functions:
    - It determines the denominator to use in normalization (for the `frac`
      part)
    - It packages the output so that it has index/column information as
      appropriate

    :param reduction: contains a reduction of `data` along `axis`
    :param data: numeric series or dataframe
    :param axis: indicates row or column or else `None` for ignoring 2d
        structure
    """
    if isinstance(data, pd.Series):
        df = data.to_frame()
    else:
        df = data
    nrows, ncols = df.shape
    # Ensure that there is data available.
    # TODO(Paul): Consider adding a check on the column data type.
    if nrows == 0 or ncols == 0:
        _LOG.warning("No data available!")
        return np.nan
    # Determine the correct denominator based on `axis`.
    if axis is None:
        denom = nrows * ncols
    elif axis == 0:
        denom = nrows
    elif axis == 1:
        denom = ncols
    else:
        raise ValueError("axis=%i", axis)
    normalized = reduction / denom
    # Return float or pd.Series as appropriate based on dimensions and axis.
    if isinstance(normalized, float):
        dbg.dassert(not axis)
        return normalized
    else:
        dbg.dassert_isinstance(normalized, np.ndarray)
        if axis == 0:
            return pd.Series(data=normalized, index=df.columns)
        elif axis == 1:
            return pd.Series(data=normalized, index=df.index)
        else:
            raise ValueError("axis=`%s` but expected to be `0` or `1`!", axis)


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
    data: Union[pd.Series, pd.DataFrame],
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
    :return: DataFrame with t-value and p-value rows, columns like df's columns
    """
    if isinstance(data, pd.Series):
        data = data.to_frame()
    dbg.dassert_isinstance(data, pd.DataFrame)
    if popmean is None:
        popmean = 0
    if nan_policy is None:
        nan_policy = "omit"
    tvals, pvals = sp.stats.ttest_1samp(
        data, popmean=popmean, nan_policy=nan_policy
    )
    result = pd.DataFrame(
        {"tval": tvals, "pval": pvals}, index=data.columns
    ).transpose()
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
    dbg.dassert_isinstance(srs, pd.Series)
    if method is None:
        method = "fdr_bh"
    pvals_corrected = sm.stats.multitest.multipletests(srs, method=method)[1]
    return pd.Series(pvals_corrected, index=srs.index, name="adj_pval")


def multi_ttest(
    df: pd.DataFrame,
    popmean: Optional[float] = None,
    nan_policy: Optional[str] = None,
    method: Optional[str] = None,
) -> pd.DataFrame:
    """
    Combine ttest and multitest pvalue adjustment.
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    ttest = ttest_1samp(df, popmean=popmean, nan_policy=nan_policy).transpose()
    ttest["adj_pval"] = multipletests(ttest["pval"], method=method)
    return ttest.transpose()


def apply_normality_test(
    data: Union[pd.Series, pd.DataFrame], nan_policy: Optional[str] = None
) -> pd.DataFrame:
    """
    Test (indep) null hypotheses that each col is normally distributed.

    An omnibus test of normality that combines skew and kurtosis.

    :return: dataframe with same cols as `df` and two rows:
        1. "statistic"
        2. "pvalue"
    """
    if isinstance(data, pd.Series):
        data = data.to_frame()
    dbg.dassert_isinstance(data, pd.DataFrame)
    if nan_policy is None:
        nan_policy = "omit"
    stats = []
    pvals = []
    for col in data.columns:
        if data[col].dropna().size < 8:
            # The `skewtest` requires at least 8 samples and will raise if it
            # does not receive at least 8.
            stats.append(np.nan)
            pvals.append(np.nan)
            continue
        stat, pval = sp.stats.normaltest(data[col], nan_policy=nan_policy)
        stats.append(stat)
        pvals.append(pval)
    res = pd.DataFrame(
        data=list(zip(stats, pvals)),
        columns=["statistic", "pvalue"],
        index=data.columns,
    )
    return res.transpose()
