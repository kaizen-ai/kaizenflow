"""
Package with general pandas helpers.
"""

import collections
import logging
import types

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import helpers.dbg as dbg
import helpers.printing as pri

_LOG = logging.getLogger(__name__)


# ##############################################################################


def resample_index(index, time=None, **kwargs):
    """
    Resample `index` with options compatible with pd.date_range().
    Implementation inspired by https://stackoverflow.com/questions/37853623

    :param index: The daily-frequency index to resample as pd.DatetimeIndex
    :param time: (hour, time) tuple to align the sampling
    :param **kwargs: parameters (e.g., freq) passed to pd.date_range()

    :return: The resampled index. Use df.loc[resampled_index] to sample.
    """
    dbg.dassert_isinstance(index, pd.DatetimeIndex)
    _LOG.debug("index=%s", index)
    start_date = index.min()
    if time is not None:
        start_date = start_date.replace(hour=time[0], minute=time[1])
    end_date = index.max() + pd.DateOffset(nanoseconds=1)
    _LOG.debug("start_date=%s end_date=%s", start_date, end_date)
    resampled_index = pd.date_range(start_date, end_date, **kwargs)[:-1]
    _LOG.debug("resampled_index=%s", resampled_index)
    index = resampled_index.intersection(index)
    return index


# ##############################################################################


def _build_empty_df(metadata):
    """
    Build an empty dataframe using the data in `metadata`, which is populated
    in the previous calls of the rolling function.
    This is used to generate missing data when applying the rolling function.
    """
    dbg.dassert_is_not(metadata, None)
    cols = metadata["cols"]
    dbg.dassert_lte(1, len(cols))
    idxs = metadata["idxs"]
    dbg.dassert_lte(1, len(idxs))
    if metadata["is_series"]:
        empty_df = pd.DataFrame([[np.nan] * len(cols)], columns=cols)
    else:
        empty_df = pd.DataFrame(
            [[np.nan] * len(cols)] * len(idxs), index=idxs, columns=cols
        )
    return empty_df


def _loop(i, ts, df, func, window, metadata, abort_on_error):
    """
    Apply `func` to a slice of `df` given by `i` and `window`.
    """
    # Extract the window.
    if i <= 1:
        _LOG.debug("i=%s -> return=None", i)
        df_tmp = None
        return df_tmp, metadata
    dbg.dassert_lte(i, df.shape[0])
    dbg.dassert_lt(0, window)
    upper_bound = i + 1
    lower_bound = upper_bound - window
    _LOG.debug(
        "i=%s, window=%s -> slice=[%d:%d]", i, window, lower_bound, upper_bound
    )
    if lower_bound < 0:
        _LOG.debug("lower_bound=%s < 0 -> return=None", i)
        df_tmp = None
        return df_tmp, metadata
    window_df = df.iloc[lower_bound:upper_bound, :]
    if window_df.shape[0] < window:
        _LOG.debug(
            "Not enough samples: window.shape=%s < window=%s -> " "return=None",
            window_df.shape[0],
            window,
        )
        df_tmp = None
        return df_tmp, metadata
    # Apply function.
    # is_class = inspect.isclass(func)
    is_class = not isinstance(func, types.FunctionType)
    try:
        if is_class:
            df_tmp = func(window_df, ts)
        else:
            df_tmp = func(window_df)
    except (RuntimeError, AssertionError) as e:
        _LOG.error("Caught exception at ts=%s", ts)
        if abort_on_error:
            _LOG.error(str(e))
            raise e
        else:
            df_tmp = _build_empty_df(metadata)
    # Make sure result is well-formed.
    is_series = isinstance(df_tmp, pd.Series)
    if is_series:
        df_tmp = pd.DataFrame(df_tmp).T
    if metadata is None:
        metadata = {
            "is_series": is_series,
            "idxs": df_tmp.index,
            "cols": df_tmp.columns,
        }
    else:
        if metadata["is_series"]:
            # TODO(gp): The equivalent check for multiindex is more complicated.
            dbg.dassert_eq_all(df_tmp.index, metadata["idxs"])
            dbg.dassert_eq_all(df_tmp.columns, metadata["cols"])
    return df_tmp, metadata


def df_rolling_apply(
    df,
    window,
    func,
    timestamps=None,
    convert_to_df=True,
    progress_bar=False,
    abort_on_error=True,
):
    """
    Apply function `func` to a rolling window over `df` with `window` columns.
    Timing semantic:
    - a timestamp i is computed based on a data slice [i - window + 1:i]
    - mimics pd.rolling functions

    The implementation from https://stackoverflow.com/questions/38878917
    doesn't scale both in time and memory since it makes copies of the windowed
    df. This implementations uses views and apply `func` directly without
    making copies.

    :param df: dataframe to process
    :param window: number of rows in each window
    :param func: function taking a df and returning a pd.Series with the results
        `func` should not change the passed df
    :param timestamps: pd.Index representing the datetimes to apply `func`
        - `None` implies using all the timestamps in `df`
    :param convert_to_df: return a df (potentially multiindex) with the result.
        If False return a OrderDict index to df
    :return: dataframe with the concatenated results, with the same number of
        rows as `df`
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.dassert_monotonic_index(df)
    # Make sure the window is not larger than the df.
    dbg.dassert_lt(0, window)
    if int(window) != window:
        _LOG.warning("window=%s is not an integer", window)
    window = int(window)
    dbg.dassert_lte(window, df.shape[0])
    idx_to_df = collections.OrderedDict()
    # Store the metadata about the result of `func`.
    metadata = None
    if timestamps is None:
        # Roll the window over the df.
        iter_ = range(0, df.shape[0])
    else:
        dbg.dassert_isinstance(timestamps, pd.Index)
        dbg.dassert_monotonic_index(timestamps)
        idxs = df.index.intersection(timestamps)
        dbg.dassert_lte(1, len(idxs))
        if len(idxs) < len(timestamps):
            _LOG.warning(
                "Some of the requested timestamps are not in df: "
                "missing %s timestamps",
                pri.perc(len(idxs), len(timestamps), invert=True),
            )
        # Find the numerical index of all the timestamps in df.
        idxs_loc = (
            pd.Series(list(range(df.shape[0])), index=df.index)
            .loc[idxs]
            .values.tolist()
        )
        dbg.dassert_eq(len(idxs_loc), len(idxs))
        dbg.dassert_eq_all(df.iloc[idxs_loc].index, idxs)
        iter_ = idxs_loc
    if progress_bar:
        iter_ = tqdm(iter_)
    for i in iter_:
        ts = df.index[i]
        _LOG.debug(pri.frame("i=%s ts=%s"), i, ts)
        df_tmp, metadata = _loop(
            i, ts, df, func, window, metadata, abort_on_error
        )
        idx_to_df[ts] = df_tmp
    # Replace None values with an empty df.
    empty_df = _build_empty_df(metadata)
    for ts, v in idx_to_df.items():
        if v is None:
            idx_to_df[ts] = empty_df
    _LOG.debug("idx_to_df=\n%s", idx_to_df)
    # Unfortunately the code paths for concatenating pd.Series and multiindex
    # pd.DataFrame are difficult to unify.
    if convert_to_df:
        if metadata["is_series"]:
            # Assemble result into a df.
            res_df = pd.concat(idx_to_df.values())
            idx = idx_to_df.keys()
            dbg.dassert_eq(res_df.shape[0], len(idx))
            res_df.index = idx
        else:
            # Assemble result into a df.
            res_df = pd.concat(idx_to_df)
        res = res_df
        if timestamps is not None:
            dbg.dassert_eq_all(res_df.index, idxs)
    else:
        res = idx_to_df
    return res
