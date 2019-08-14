"""
Package with general pandas helpers.
"""

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
    df_res = []
    # Store the columns of the results.
    cols = None
    # Roll the window over the df.
    # Note that numpy / pandas slicing [a:b] corresponds to python slicing
    # [a:b+1].
    for i in range(window, df.shape[0] + 1):
        # Extract the window.
        lower_bound = i - window
        upper_bound = i
        _LOG.debug("slice=[%d:%d]", lower_bound, upper_bound)
        window_df = df.iloc[lower_bound:upper_bound, :]
        # Apply function.
        df_tmp = func(window_df)
        # Make sure result is well-formed.
        dbg.dassert_isinstance(df_tmp, pd.Series)
        if cols is None:
            cols = df_tmp.index
        else:
            dbg.dassert((df_tmp.index == cols).all)
        # Accumulate results.
        df_res.append(pd.DataFrame(df_tmp).T)
    # Add a number of empty rows to handle when there were not enough rows to
    # build a window.
    empty_row = pd.DataFrame([[np.nan] * len(cols)], columns=cols)
    df_res_tmp = [empty_row] * (window - 1)
    df_res_tmp.extend(df_res)
    # Assemble result into a df.
    df_res = pd.concat(df_res_tmp)
    dbg.dassert_eq(df_res.shape[0], df.shape[0])
    df_res.index = df.index
    return df_res
