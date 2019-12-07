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
`stack_data`
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
"""


import logging
from typing import Dict, Iterable, Optional, Union

import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


EVENT_INDICATOR = "event_indicator"


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
    mode: Optional[str] = None,
    info: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Shift by `periods` and select `idx`.

    This private helper encapsulates and isolates time-shifting behavior.

    :param idx: reference index (e.g., of datetimes of events)
    :param grid_data: tabular data
    :param periods: as in pandas `shift` functions
    :param freq: as in pandas `shift` functions
    :param mode:
        -   "shift_data" applies `.shift()` to `data`
        -   "shift_idx" applies `.shift()` to `idx`
    :param info: optional empty dict-like object to be populated with stats
        about the operation performed
    """
    mode = mode or "shift_data"
    # Shift.
    if mode == "shift_data":
        grid_data = grid_data.copy()
        grid_data = grid_data.shift(periods, freq)
    elif mode == "shift_idx":
        idx = idx.copy()
        idx = idx.shift(periods, freq)
    else:
        raise ValueError("Unrecognized mode=`%s`", mode)
    # Select.
    intersection = idx.intersection(grid_data.index)
    dbg.dassert(not intersection.empty)
    # TODO(Paul): Make this configurable
    pct_found = intersection.size / idx.size
    if pct_found < 0.9:
        _LOG.warning("pct_found=%f for periods=%d", pct_found, periods)
    selected = grid_data.loc[intersection]
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
    periods: Iterable[int],
    freq: Optional[Union[pd.DateOffset, pd.Timedelta, str]] = None,
    shift_mode: Optional[str] = None,
    info: Optional[dict] = None,
) -> Dict[int, pd.DataFrame]:
    """
    Compute series over `periods` relative to `events.index`.

    This generates a sort of "panel" time series:
    -   The period indicates the relative time step
    -   (In an event study) the value at each period is a dataframe with
        -   An index of "event times" (given by `idx`)
        -   Columns corresponding to
            -   Precisely one response variable
            -   Zero or more predictors

    The effect of this function is to grab uniform time slices of `data`
    around each event time indicated in `idx`.

    :param events: reference index (e.g., of datetimes of events)
    :param grid_data: tabular data
    :param periods: shift periods
    :param freq: as in pandas `shift` functions
    :param shift_mode:
        -   "shift_data" applies `.shift()` to `data`
        -   "shift_idx" applies `.shift()` to `idx`
    :return: a dict
        -   keyed by period (from [first_period, last_period])
        -   values series / dataframe, obtained from shifts and idx selection
    """
    dbg.dassert_isinstance(events, pd.DataFrame)
    dbg.dassert_isinstance(grid_data, pd.DataFrame)
    dbg.dassert_monotonic_index(events.index)
    dbg.dassert_monotonic_index(grid_data.index)
    if not isinstance(periods, list):
        periods = list(periods)
    dbg.dassert(periods)
    #
    period_to_data = {}
    for period in periods:
        if info is not None:
            period_info = {}
        else:
            period_info = None
        #
        grid_data = _shift_and_select(
            events.index, grid_data, period, freq, shift_mode, period_info
        )
        #
        if info is not None:
            info[period] = period_info
        period_to_data[period] = grid_data
    return period_to_data


def stack_data(
    data: Dict[int, Union[pd.Series, pd.DataFrame]],
) -> Union[pd.Series, pd.DataFrame]:
    """
    Stack dict of data (to prepare for modeling).

    :param data:
    :return:
    """
    stacked = pd.concat(data.values(), axis=0, ignore_index=True)
    return stacked


def regression(x: pd.DataFrame, y: pd.Series):
    """
    Linear regression of y_var on x_vars.

    Constant regression term not included but must be supplied by caller.

    WARNING: Implemented from scratch as an exercise. Better to use a library
        version.

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
