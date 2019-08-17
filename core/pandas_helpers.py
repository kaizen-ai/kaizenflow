"""
Package with general pandas helpers.
"""

import collections
import inspect
import logging

import numpy as np
import pandas as pd
import tqdm

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def df_rolling_apply(df, window, func, convert_to_df=True, progress_bar=False):
    """
    Apply function `func` to a rolling window over `df` with `window` columns.
    The implementation from https://stackoverflow.com/questions/38878917
    doesn't scale both in time and memory since it makes copies of the windowed df.
    This implementations uses views and apply `func` directly without making
    copies.

    :param df: dataframe to process
    :param window: number of rows in each window
    :param func: function taking a df and returning a pd.Series with the results
        `func` should not change the passed df
    :param convert_to_df: return a df (potentially multiindex) with the result.
        If False return a OrderDict index to df
    :return: dataframe with the concatenated results, with the same number of
        rows as `df`
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.check_monotonic_df(df)
    # Make sure the window is not larger than the df.
    dbg.dassert_lte(1, window)
    dbg.dassert_lte(window, df.shape[0])
    idx_to_df = collections.OrderedDict()
    is_class = inspect.isclass(func)
    # Store the columns of the results.
    idxs = cols = None
    # Roll the window over the df.
    # Note that numpy / pandas slicing [a:b] corresponds to python slicing
    # [a:b+1].
    is_series = False
    iter_ = range(window, df.shape[0] + 1)
    if progress_bar:
        iter_ = tqdm.tqdm(iter_)
    for i in iter_:
        # Extract the window.
        lower_bound = i - window
        upper_bound = i
        _LOG.debug("slice=[%d:%d]", lower_bound, upper_bound)
        window_df = df.iloc[lower_bound:upper_bound, :]
        ts = window_df.index[-1]
        _LOG.debug("ts=%s", ts)
        # Apply function.
        if is_class:
            df_tmp = func(window_df, ts)
        else:
            df_tmp = func(window_df)
        # Make sure result is well-formed.
        if isinstance(df_tmp, pd.Series):
            is_series = True
            df_tmp = pd.DataFrame(df_tmp).T
        if cols is None:
            idxs = df_tmp.index
            cols = df_tmp.columns
        else:
            if is_series:
                # TODO(gp): The equivalent check for multiindex is more complicated.
                dbg.dassert_eq_all(df_tmp.index, idxs)
                dbg.dassert_eq_all(df_tmp.columns, cols)
        # Accumulate results.
        _LOG.debug("df_tmp=\n%s", df_tmp)
        idx_to_df[ts] = df_tmp
    # Unfortunately the code paths for concatenating pd.Series and multiindex
    # pd.DataFrame are difficult to unify.
    if is_series:
        # Add a number of empty rows to handle when there were not enough rows to
        # build a window.
        idx_to_df_all = collections.OrderedDict()
        empty_df = pd.DataFrame([[np.nan] * len(cols)], columns=cols)
        for j in range(0, window - 1):
            ts = df.index[j]
            idx_to_df_all[ts] = empty_df
        idx_to_df_all.update(idx_to_df)
        # Assemble result into a df.
        res_df = pd.concat(idx_to_df_all.values())
        idx = idx_to_df_all.keys()
        dbg.dassert_eq(res_df.shape[0], len(idx))
        res_df.index = idx
        # The result should have the same length of the original df.
        dbg.dassert_eq(res_df.shape[0], df.shape[0])
    else:
        # Add a number of empty rows to handle when there were not enough rows to
        # build a window.
        idx_to_df_all = collections.OrderedDict()
        empty_df = pd.DataFrame(
            [[np.nan] * len(cols)] * len(idxs), index=idxs, columns=cols)
        for j in range(0, window - 1):
            ts = df.index[j]
            idx_to_df_all[ts] = empty_df
        idx_to_df_all.update(idx_to_df)
        # Assemble result into a df.
        res_df = pd.concat(idx_to_df_all)
    return res_df
