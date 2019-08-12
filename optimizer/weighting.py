# Portfolio weighting functions

# TODO: Parametrize covariance / mean estimation strategies
#       We want to parametrize the flow so that
#         - We provide returns, a weighting strategy and, if applicable,
#           - a covariance matrix estimation strategy
#           - a mean estimation strategy
#       The output should consist of
#           - weights at every time index (except for burn-in/warm-up period)
#           - log returns


import logging

import optimizer.covariance as optcov
import helpers.dbg as dbg
import numpy as np
import pandas as pd


_LOG = logging.getLogger(__name__)


def equal_weighting(df):
    """
    Equally weight returns in df and generate stream of log rets.
    """
    rets = df.dropna(how='any').mean(axis=1)
    log_rets = np.log(rets + 1)
    return log_rets


def inverse_volatility_weighting(df, com, min_periods):
    """
    Weight returns by inverse volatility (calculated by rolling std).

    Assume df contains % returns.
    """
    # Convert to log returns for the purpose of calculating volatility.
    log_df = np.log(df + 1)
    log_df = log_df.ewm(com=com, min_periods=min_periods, adjust=True,
                        ignore_na=False, axis=0).std()
    inv_vol = 1. / log_df
    total_inv_vol = inv_vol.sum(axis=1) 
    weights = inv_vol.divide(total_inv_vol, axis=0)
    # Shift weights two time periods (1 to enter, 1 to exit)
    weights = weights.shift(2)
    weighted = df.multiply(weights, axis=0)
    rets = weighted.sum(axis=1)
    log_rets = np.log(rets + 1)
    return log_rets, weights


def minimum_variance_weighting(df, com, min_periods):
    """
    Weight returns by inverse covariance (calculating by rolling cov).

    Note that weights may be negative.
    """
    # Convert to log returns for the purpose of calculating covariance. 
    _LOG.info("df num rows = %i", df.shape[0])
    _LOG.info("df num rows with no NaNs = %i", df.dropna(how='any').shape[0])
    log_df = np.log(df + 1)
    cov = log_df.ewm(com=com, min_periods=min_periods, adjust=True,
                     ignore_na=False, axis=0).cov()
    _LOG.info("cov num matrices = %i", cov.shape[0] / cov.shape[1])
    inv_cov = optcov.cov_df_to_inv(cov)
    weights = np.divide(inv_cov.sum(axis=1),
                        inv_cov.sum(axis=1).sum(axis=1, keepdims=True))
    weights_df = pd.DataFrame(data=weights,
                              index=cov.index.get_level_values(0).drop_duplicates(),
                              columns=cov.columns)
    _LOG.info("weights_df num rows = %i", weights_df.shape[0])
    _LOG.info("weights_df num rows with no NaNs = %i",
              weights_df.dropna(how='any').shape[0])
    # Shift weights two time periods (1 to enter, 1 to exit)
    weights_df = weights_df.shift(2)
    rets = df.multiply(weights_df, axis=0).sum(axis=1, skipna=False)
    log_rets = np.log(rets + 1)
    return log_rets, weights_df


def kelly_optimal_weighting(df, com, min_periods):
    """
    Same as Markowitz tangency portfolio, but with optimal leverage.

    See https://epchan.blogspot.com/2014/08/kelly-vs-markowitz-portfolio.html.

    TODO: Decide whether to use ewm_rets rather than rets. Results may be very
    sensitive to choice of com. Portfolio may be highly leveraged.
    """
    cov = optcov.ewm_cov(df, com, min_periods)
    inv_cov = optcov.cov_df_to_inv(cov)
    ewm_rets = df.ewm(com=com, min_periods=min_periods).mean()
    weights = np.einsum('ijk,ik->ij', inv_cov, ewm_rets)
    weights_df = pd.DataFrame(data=weights,
                              index=cov.index.get_level_values(0).drop_duplicates(),
                              columns=cov.columns)
    # Shift weights two time periods (1 to enter, 1 to exit)
    weights_df = weights_df.shift(2)
    rets = df.multiply(weights_df, axis=0).sum(axis=1, skipna=False)
    log_rets = np.log(rets + 1)
    return log_rets, weights_df
