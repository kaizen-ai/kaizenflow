"""
Import as:

import core.intraday_event_study as ies

TODO(Paul): Reorganize if we add additional event study files

We make a distinction between the following different types of events (with
respect to active trading hours):

1.  Intraday events
    a.  We focus on events strictly within active trading hours
    b.  We may further restrict around the open / close, because response
        windows of uniform time are easy to work with
2.  At-the-open
    a.  We focus on the effect on the market of information that has
        accumulated outside of active-trading-hours
    b.  These can be handled together with intraday events provided we treat
        the event time as the market-open
3.  At-the-close
    a.  Distinguished from vanilla intraday in that the timing of the close
        may restrict the response windows of interest
4.  Multi-day
    a.  Of interest if events are sparse on the scale of days; otherwise the
        proper multi-day setting is a continuous one
"""

import logging
from typing import Tuple, Union

import numpy as np
import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


def generate_pre_and_post_event_split(
    x_vars: pd.DataFrame, y_vars: Union[pd.Series, pd.DataFrame],
        num_shifts: int, mode: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    TODO: Implement

    :param x_vars:
    :param y_vars:
    :param num_shifts:
    :param mode:
        `panel`: y_var
    :return:
    """
    pass


# TODO(Paul): Really, we don't want to call these x_vars and y_vars. We just
#     want 1) a series of event times and 2) a DataFrame of data (maybe with
#     both signals and responses as separate columns).
def generate_aligned_response(
    x_vars: pd.DataFrame, y_vars: Union[pd.Series, pd.DataFrame], num_shifts: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Align responses of a y_var with x_vars according to index

    Restricting to the intraday setting greatly simplifies the alignment
    process.

    A drawback of the current implementation is that multiple responses are not
    considered simultaneously. So, for example, studying the cross-sectional
    response of US equities with this implementation would require a separate
    function call for each equity.

    :param x_vars: pd.DataFrame of signal, indexed by datetimes aligned with
        response time grid frequency.
    :param y_vars: pd.DataFrame of response
    :param num_shifts: Number of response time shifts to grab both before and
        after the event time

    :return: pre_event_df, post_event_df
    """
    if isinstance(y_vars, pd.Series):
        y_vars = y_vars.to_frame()
    dbg.dassert_monotonic_index(y_vars)
    # TODO(Paul): Make sure everything in the big dataframe has a monotonic
    #     index and a frequency. 
    # TODO(Paul): Maybe assert `y_var` has a `freq` specified.
    # TODO(Paul): Check offsets of x_vars (though this may be slow...)
    pre_event = []
    post_event = []
    for i in range(-num_shifts, num_shifts + 1, 1):
        # Times go -num_shifts, ..., -1, 0, 1, ..., num_shifts
        # To get the response at time j, we call .shift(-j) on y_vars
        # TODO(Paul): Restrict back to col names?
        resp = x_vars.join(y_vars.shift(-i))
        resp.name = i
        # TODO(Paul): Placing the event time in the "post-event" group matches
        #     how we treat time intervals. OTOH, the event itself (assuming its
        #     time is a knowledge time) doesn't impact ret_0, which suggests
        #     including the event time in the "pre-event" group.
        if i < 0:
            pre_event.append(resp)
        else:
            post_event.append(resp)
    dbg.dassert_lt(0, len(pre_event), "No pre-event response data!")
    dbg.dassert_lt(0, len(post_event), "No post-event response data!")
    # datetime index contains event times, columns indicate relative time
    #     shifts
    pre_event_df = pd.concat(pre_event, axis=1)
    post_event_df = pd.concat(post_event, axis=1)
    return pre_event_df, post_event_df


def tile_x_flatten_y(
    x_vars: pd.DataFrame, y_var: pd.DataFrame
) -> Tuple[np.array, np.array]:
    """
    Reshape x_vars, y_var into equal-length series.

    Values of `x_vars` are repeated in the tiling for each column of `y_var`.
    WARNING: May be non-casual if cols

    :param x_vars: signal at event time
    :param y_var: a single response variable in n x k form
        - `n` (rows) represents number of events
        - `k` (cols) represents time offsets
    :return:
        - `x` tiled `k` times resulting in n x k rows
        - `y` flattened into an (n x k)-length array
    """
    y = y_var.values.transpose().flatten()
    # Tile x values to match flattened y
    _LOG.debug("x_vars=`%s`", x_vars.columns.values)
    x = np.tile(x_vars.values, (y_var.shape[1], 1))
    return x, y


def regression(x, y):
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
    # Estimate level pre-event.
    _LOG.info("Estimating pre-event response level...")
    # Create a constant term to regress against.
    x_ind = pd.DataFrame(
        index=x_vars.index, data=np.ones(x_vars.shape[0]), columns=["const"]
    )
    x_lvl, y_pre_resp = tile_x_flatten_y(x_ind, pre_resp)
    # Regression against const.
    alpha_hat, alpha_hat_var, alpha_hat_z_score = regression(x_lvl, y_pre_resp)
    # Estimate post-event response.
    _LOG.info("Regressing level-adjusted post-event response against x_vars...")
    # Adjust post-event values by pre-event-estimated level
    x, y_post_resp = tile_x_flatten_y(x_vars, post_resp - alpha_hat[0])
    delta_hat, delta_hat_var, delta_hat_z_score = regression(x, y_post_resp)
    return alpha_hat, alpha_hat_z_score, delta_hat, delta_hat_z_score
