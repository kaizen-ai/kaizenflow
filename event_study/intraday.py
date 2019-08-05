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


def non_nan_cnt(v):
    """
    Convenience function for counting the number of non-nan values in a numpy
    array.
    
    """
    return np.count_nonzero(~np.isnan(v))


def estimate_level(v):
    """
    Estimate mean based on samples provided in v.
    
    :param v: numpy array

    :return: estimate of mean with NaN's dropped and variance of mean estimate
    """
    mean = np.nanmean(v)
    var = np.nanvar(v, ddof=1) / float(non_nan_cnt(v))
    return mean, var


def estimate_event_effect(x_vars, pre_resp, post_resp):
    """
    Consider the regression model

    y_{ij} = \alpha + \delta x_{ij}

    where i indicates events and j time (relative to event time).
    The x_{ij} are taken to be zero for j < 0 and equal to the event-time value
    provided by x_df for all j >= 0.

    This function performs the following procedure:
      1. Estimate \alpha by taking mean of all values in pre_resp
      2. Adjust values in post_resp by subtracting \alpha
      3. Regress adjusted post_resp values against x_df (estimate \delta)

    We should consider replacing this procedure with a robust Bayesian
    procedure, such as "Bayesian Estimation Supersedes the t Test"
    (Kruschke, 2012). This is demonstrated in PyMC3 at
    https://colcarroll.github.io/pymc3/notebooks/BEST.html

    For all params, rows are datetimes.
    :param x_vars: pd.DataFrame of event signal, with 1 col for each regressor 
    :param pre_resp: pd.DataFrame of response pre-event (cols are neg offsets)
    :param post_resp: pd.DataFrame of response post-event (cols are pos
        offsets)

    :return: alpha, alpha_var, delta, delta_covar 
    """
    # Estimate level pre-event
    alpha, alpha_var = estimate_level(pre_resp.values)
    # Adjust post-event values by pre-event-estimated level
    y_tilde = post_resp.values.transpose().flatten() - alpha
    nobs = non_nan_cnt(y_tilde)
    # Tile x values to match flattened y
    x = np.tile(x_vars.values, (post_resp.shape[1], 1))
    # Linear regression to estimate \delta
    regress = np.linalg.lstsq(x, np.nan_to_num(y_tilde), rcond=None) 
    delta = regress[0]
    # Estimate delta covariance
    rss = regress[1]
    xtx_inv = np.linalg.inv((x.transpose().dot(x)))
    delta_covar = xtx_inv * rss / (nobs - 1)
    return alpha, alpha_var, delta, delta_covar 
