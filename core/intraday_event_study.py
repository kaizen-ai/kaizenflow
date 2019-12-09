"""
Import as:

import core.intraday_event_study as esf

TODO(Paul): Rename file to just `event_study.py`

Sketch of flow:

events         grid_data -------------------
  |              |                          |
  |              |                          |
resample ----- `reindex_event_features`     |
  |              |                          |
  |              |                          |
  |            kernel (ema, etc.) -------- merge
  |                                         |
  |                                         |
`build_local_timeseries` -------------------
  |
  |
drop multiindex
  |
  |
linear modeling
  |
  ...


Comments:

-   `grid_data` contains
    -   exactly one response var
    -   zero or more predictors
    -   predictors may include lagged response vars
-   Linear modeling step:
    -   For unpredictable events, this model may not be tradable
    -   In any case, the main purpose of this model is to detect an event
        effect
    -   If predicting returns, project to PnL using kernel

TODO(Paul): Update function docstrings everywhere
"""


import logging
from typing import Any, Dict, Iterable, Optional, Union

import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def reindex_event_features(
    events: pd.DataFrame, grid_data: pd.DataFrame, **kwargs,
) -> pd.DataFrame:
    """

    :param events:
    :param data_idx:
    :param kwargs:
    :return:
    """
    reindexed = events.reindex(index=grid_data.index, **kwargs)
    return reindexed


def _shift_and_select(
    idx: pd.Index,
    grid_data: pd.DataFrame,
    periods: int,
    freq: Optional[Union[pd.DateOffset, pd.Timedelta, str]] = None,
    info: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Shift by `periods` and select `idx`.

    This private helper encapsulates and isolates time-shifting behavior.

    :param idx: reference index (e.g., of datetimes of events)
    :param grid_data: tabular data
    :param periods: as in pandas `shift` functions
    :param freq: as in pandas `shift` functions
    :param info: optional empty dict-like object to be populated with stats
        about the operation performed
    """
    # Shift.
    shifted_grid_data = grid_data.shift(periods, freq)
    # Select.
    intersection = idx.intersection(shifted_grid_data.index)
    dbg.dassert(not intersection.empty)
    # TODO(Paul): Make this configurable
    pct_found = intersection.size / idx.size
    if pct_found < 0.9:
        _LOG.warning("pct_found=%f for periods=%d", pct_found, periods)
    selected = shifted_grid_data.loc[intersection]
    # Maybe add info.
    if info is not None:
        dbg.dassert_isinstance(info, dict)
        dbg.dassert(not info)
        info["indices_with_no_data"] = intersection.difference(idx)
        info["periods"] = periods
        if freq is not None:
            info["freq"] = freq
        info["mode"] = mode
    return selected


def build_local_timeseries(
    events: pd.DataFrame,
    grid_data: pd.DataFrame,
    relative_grid_indices: Iterable[int],
    freq: Optional[Union[pd.DateOffset, pd.Timedelta, str]] = None,
    info: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Construct relative time series of `grid_data` around each event.

    The effect of this function is to grab uniform time slices of `data`
    around each event time indicated in `idx`.

    :param events: reference index (e.g., of datetimes of events)
    :param grid_data: tabular data
    :param relative_grid_indices: time points on grid relative to event time
        of "0", e.g., [-2, -1, 0, 1] denotes t_{-2} < t_{-1} < t_0 < t_1,
        where "t_0" is the event time.
    :param freq: as in pandas `shift`
    :return: multiindexed dataframe
        -   level 0: relative grid indices
        -   level 1: event time (e.g., "t_0") for each event
                Note that the event time needs to be adjusted by
                relative_grid_index grid points in order to obtain the data
                timestamps
        -   cols: same as grid_data cols
    """
    dbg.dassert_isinstance(events, pd.DataFrame)
    dbg.dassert_isinstance(grid_data, pd.DataFrame)
    dbg.dassert_monotonic_index(events.index)
    dbg.dassert_monotonic_index(grid_data.index)
    if not isinstance(relative_grid_indices, list):
        relative_grid_indices = list(relative_grid_indices)
    dbg.dassert(relative_grid_indices)
    relative_grid_indices.sort()
    #
    relative_data = {}
    for idx in relative_grid_indices:
        if info is not None:
            info_for_idx = {}
        else:
            info_for_idx = None
        #
        data_at_idx = _shift_and_select(
            events.index, grid_data, -idx, freq, info_for_idx
        )
        #
        if info is not None:
            info[idx] = info_for_idx
        relative_data[idx] = data_at_idx
    df = pd.concat(relative_data)
    dbg.dassert_monotonic_index(df)
    return df


def regression(x: pd.DataFrame, y: pd.Series):
    """
    Linear regression of y_var on x_vars.

    Constant regression term not included but must be supplied by caller.

    WARNING: Advisable to use sklearn, etc. instead.

    Returns beta_hat along with beta_hat_covar and z-scores for the hypothesis
    that a beta_j = 0.
    """
    nan_filter = ~np.isnan(y)
    nobs = np.count_nonzero(nan_filter)
    _LOG.info("nobs (resp) = %s", nobs)
    x = x[nan_filter]
    y = y[nan_filter]
    y_mean = np.mean(y)
    _LOG.info("y_mean = %f", y_mean)
    tss = (y - y_mean).dot(y - y_mean)
    _LOG.info("tss = %f", tss)
    # Linear regression to estimate \beta
    regress = np.linalg.lstsq(x, y, rcond=None)
    beta_hat = regress[0]
    _LOG.info("beta = %s", np.array2string(beta_hat))
    # Estimate delta covariance
    rss = regress[1]
    _LOG.info("rss = %f", rss)
    _LOG.info("r^2 = %f", 1 - rss / tss)
    xtx_inv = np.linalg.inv((x.transpose().dot(x)))
    sigma_hat_sq = rss / (nobs - x.shape[1])
    _LOG.info("sigma_hat_sq = %s", np.array2string(sigma_hat_sq))
    beta_hat_covar = xtx_inv * sigma_hat_sq
    _LOG.info("beta_hat_covar = %s", np.array2string(beta_hat_covar))
    beta_hat_z_score = beta_hat / np.sqrt(sigma_hat_sq * np.diagonal(xtx_inv))
    _LOG.info("beta_hat_z_score = %s", np.array2string(beta_hat_z_score))
    return beta_hat, beta_hat_covar, beta_hat_z_score
