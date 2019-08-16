"""
Package with general pandas helpers.
"""

import collections
import logging

import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def df_rolling_apply(df, window, func):
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
    :return: dataframe with the concatenated results, with the same number of
        rows as `df`
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.check_monotonic_df(df)
    # Make sure the window is not larger than the df.
    dbg.dassert_lte(1, window)
    dbg.dassert_lte(window, df.shape[0])
    df_res = collections.OrderedDict()
    # Store the columns of the results.
    idxs = cols = None
    # Roll the window over the df.
    # Note that numpy / pandas slicing [a:b] corresponds to python slicing
    # [a:b+1].
    is_series = False
    for i in range(window, df.shape[0] + 1):
        # Extract the window.
        lower_bound = i - window
        upper_bound = i
        _LOG.debug("slice=[%d:%d]", lower_bound, upper_bound)
        window_df = df.iloc[lower_bound:upper_bound, :]
        ts = window_df.index[-1]
        _LOG.debug("ts=%s", ts)
        # Apply function.
        df_tmp = func(window_df)
        # Make sure result is well-formed.
        if isinstance(df_tmp, pd.Series):
            is_series = True
            df_tmp = pd.DataFrame(df_tmp).T
        if cols is None:
            idxs = df_tmp.index
            cols = df_tmp.columns
        else:
            dbg.dassert((df_tmp.index == idxs).all)
            dbg.dassert((df_tmp.columns == cols).all)
        # Accumulate results.
        _LOG.debug("df_tmp=\n%s", df_tmp)
        df_res[ts] = df_tmp
    # Unfortunately the code paths for concatenating pd.Series and multiindex
    # pd.DataFrame are difficult to unify.
    if is_series:
        # Add a number of empty rows to handle when there were not enough rows to
        # build a window.
        df_res_tmp = collections.OrderedDict()
        empty_df = pd.DataFrame([[np.nan] * len(cols)], columns=cols)
        for j in range(0, window - 1):
            ts = df.index[j]
            df_res_tmp[ts] = empty_df
        df_res_tmp.update(df_res)
        idx = df_res_tmp.keys()
        _LOG.debug("idx=%s", idx)
        # Assemble result into a df.
        _LOG.debug("df_res_tmp=%s", df_res_tmp)
        res_vals = [x for x in df_res_tmp.values()]
        _LOG.debug("res_vals=%s", res_vals)
        df_res = pd.concat(res_vals)
        dbg.dassert_eq(df_res.shape[0], len(idx))
        df_res.index = idx
        # The result should have the same length of the original df.
        dbg.dassert_eq(df_res.shape[0], df.shape[0])
    else:
        # Add a number of empty rows to handle when there were not enough rows to
        # build a window.
        df_res_nan = collections.OrderedDict()
        empty_df = pd.DataFrame(
            [[np.nan] * len(cols)] * len(idxs), index=idxs, columns=cols)
        for j in range(0, window - 1):
            ts = df.index[j]
            df_res_nan[ts] = empty_df
        df_res_nan = pd.concat(df_res_nan)
        # Assemble result into a df.
        df_res = pd.concat(df_res)
        df_res = pd.concat([df_res_nan, df_res])
    return df_res
