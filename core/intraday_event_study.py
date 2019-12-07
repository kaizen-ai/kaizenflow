"""
Import as:

import core.intraday_event_study as esf

TODO(Paul): Rename file to just `event_study.py`

Sketch of flow:

events         grid_data -------------------
  |              |                          |
  |              |                          |
resample ----- reindex_event_features       |
  |              |                          |
  |              |                          |
  |            kernel (ema, etc.) -------- merge
  |                                         |
  |                                         |
build_local_timeseries ----------------------
  |
  |
stack_data
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
from typing import Any, Callable, Dict, Iterable, Optional, Union

import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


EVENT_INDICATOR = "event_indicator"


def reindex_event_features(
    events: pd.DataFrame,
    grid_data: pd.DataFrame,
    **kwargs,
) -> pd.DataFrame:
    """

    :param events:
    :param data_idx:
    :param kwargs:
    :return:
    """
    reindexed = events.reindex(index=grid_data.index, **kwargs)
    return reindexed


def _shift_and_select(idx: pd.Index, grid_data: pd.DataFrame, periods: int,
                      freq: Optional[Union[pd.DateOffset, pd.Timedelta, str]] = None, mode: Optional[str] = None,
                      info: Optional[dict] = None) -> pd.DataFrame:
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


def build_local_timeseries(events: pd.DataFrame,
                           grid_data: pd.DataFrame, periods: Iterable[int],
                           freq: Optional[Union[pd.DateOffset, pd.Timedelta, str]] = None,
                           shift_mode: Optional[str] = None, info: Optional[dict] = None) -> Dict[int, pd.DataFrame]:
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
    dbg.dassert_monotonic_index(events.index)
    dbg.dassert_monotonic_index(grid_data.index)
    if not isinstance(periods, list):
        periods = list(periods)
    dbg.dassert(periods)
    period_to_data = {}
    for period in periods:
        if info is not None:
            period_info = {}
        else:
            period_info = None
        #
        grid_data = _shift_and_select(events.index, grid_data, period, freq, shift_mode, period_info)
        #
        if info is not None:
            info[period] = period_info
        period_to_data[period] = grid_data
    return period_to_data


def compute_event_indicator(
    periods: Iterable[int],
    func: Callable[[pd.Series], pd.Series],
    func_kwargs: Optional[Any] = None,
    mode: Optional[str] = None,
) -> pd.Series:
    """
    Transform event impulse time series using func.

    :param periods:
    :param mode:
    :param func:
    :param kwargs:
    :return:
    """
    if not isinstance(periods, list):
        periods = list(periods)
    dbg.dassert(periods)
    dbg.dassert_in(0, periods)
    mode = mode or "autoscale"
    func_kwargs = func_kwargs or {}
    #
    impulse = pd.Series(data=0, index=periods, name=EVENT_INDICATOR)
    impulse.loc[0] = 1
    indicator = func(impulse, **func_kwargs)
    #
    if mode == "autoscale":
        t0_val = indicator[0]
        dbg.dassert_ne(t0_val, 0)
        indicator /= t0_val
    elif mode == "no_rescale":
        pass
    else:
        raise ValueError("Invalid mode=`%s`", mode)
    return indicator


def _add_indicator_to_relative_series(
    data_slices: Dict[int, pd.DataFrame],
    func: Callable[[pd.Series], pd.Series],
    mode: Optional[str] = None,
    func_kwargs: Optional[Any] = None,
    col_name: Optional[str] = None,
) -> Dict[int, pd.DataFrame]:
    """

    :param data_slices:
    :return:
    """
    col_name = col_name or EVENT_INDICATOR
    periods = data_slices.keys()
    event_indicator = compute_event_indicator(
        periods, func=func, func_kwargs=func_kwargs, mode=mode,
    )
    new_slices = {}
    for period in periods:
        val = event_indicator[period]
        data = data_slices[period].copy()
        srs = pd.Series(data=val, index=data.index)
        data.insert(loc=0, column=col_name, value=srs)
        new_slices[period] = data
    return new_slices


def _stack_data(
    data: Dict[int, Union[pd.Series, pd.DataFrame]],
) -> Union[pd.Series, pd.DataFrame]:
    """
    Stack dict of data (to prepare for modeling).

    :param data:
    :return:
    """
    stacked = pd.concat(data.values(), axis=0, ignore_index=True)
    return stacked


def generate_aligned_response(x_df, y_df, resp_col_name, num_shifts):
    """
    Align responses of a particular column of y_df with x_df.

    Restricting to the intraday setting greatly simplifies the alignment
    process.

    A drawback of the current implementation is that multiple responses are not
    considered simultaneously. So, for example, studying the cross-sectional
    response of US equities with this implementation would require a separate
    function call for each equity.

    :param x_df: pd.DataFrame of signal, indexed by datetimes aligned with
        response time grid.
    :param y_df: pd.DataFrame of response
    :param resp_col_name: y_df column to subselect
    :param num_shifts: Number of response time shifts to grab both before and
        after the event time
    :param y_resp_cols: Subselect response column(s) by providing name or list
        of names. If default `None` is used, all available columns are kept.

    :return: pre_event_df, post_event_df
    """
    pre_event = []
    post_event = []
    for i in range(-num_shifts, num_shifts + 1, 1):
        # Times go -num_shifts, ..., -1, 0, 1, ..., num_shifts
        # To get the response at time j, we call .shift(-j) on y_df
        resp = x_df.join(y_df.shift(-i))[resp_col_name]
        resp.name = i
        if i < 0:
            pre_event.append(resp)
        else:
            post_event.append(resp)
    pre_event_df = pd.concat(pre_event, axis=1)
    post_event_df = pd.concat(post_event, axis=1)
    return pre_event_df, post_event_df


def tile_x_flatten_y(x_vars, y_var):
    """
    Reshape x_vars, y_var for analysis from event time forward.

    :param x_var: signal at event time
    :param y_var: a single response variable in an nxk form, from event time
        and forward
    """
    y = y_var.values.transpose().flatten()
    # Tile x values to match flattened y
    _LOG.info("regressors: %s", x_vars.columns.values)
    x = np.tile(x_vars.values, (y_var.shape[1], 1))
    return x, y


def regression(x: pd.DataFrame, y: pd.Series):
    """
    Linear regression of y_var on x_vars.

    Constant regression term not included but must be supplied by caller.

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


def estimate_event_effect(x_vars, pre_resp, post_resp):
    r"""
    Consider the regression model

    y_{ij} = \alpha + \delta x_{ij}

    where i indicates events and j time (relative to event time).
    The x_{ij} are taken to be zero for j < 0 and equal to the event-time value
    provided by x_df for all j >= 0.

    This function performs the following procedure:
      1. Estimate \alpha by taking mean of all values in pre_resp
      2. Adjust values in post_resp by subtracting \alpha
      3. Regress adjusted post_resp values against x_df (estimate \delta)
      4. Estimate dispersion of regression coefficients and z-score

    For 4) we follow standard results as in Sec. 3.2 of Elements of Statistical
    Learning.

    We should consider replacing this procedure with a robust Bayesian
    procedure, such as "Bayesian Estimation Supersedes the t Test"
    (Kruschke, 2012). This is demonstrated in PyMC3 at
    https://colcarroll.github.io/pymc3/notebooks/BEST.html

    For all params, rows are datetimes.
    :param x_vars: pd.DataFrame of event signal, with 1 col for each regressor
    :param pre_resp: pd.DataFrame of response pre-event (cols are neg offsets)
    :param post_resp: pd.DataFrame of response post-event (cols are pos
        offsets)
    """
    # Estimate level pre-event
    _LOG.info("Estimating pre-event response level...")
    x_ind = pd.DataFrame(
        index=x_vars.index, data=np.ones(x_vars.shape[0]), columns=["const"]
    )
    x_lvl, y_pre_resp = tile_x_flatten_y(x_ind, pre_resp)
    alpha_hat, alpha_hat_var, alpha_hat_z_score = regression(x_lvl, y_pre_resp)
    _LOG.info("Regressing level-adjusted post-event response against x_vars...")
    # Adjust post-event values by pre-event-estimated level
    x, y_post_resp = tile_x_flatten_y(x_vars, post_resp - alpha_hat[0])
    delta_hat, delta_hat_var, delta_hat_z_score = regression(x, y_post_resp)
    return alpha_hat, alpha_hat_z_score, delta_hat, delta_hat_z_score
