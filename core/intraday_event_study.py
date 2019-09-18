import logging

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


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
