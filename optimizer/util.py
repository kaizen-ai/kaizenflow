# Rolling correlation, covariance, and matrix inversion functions.


import logging

import helpers.dbg as dbg
import numpy as np
import pandas as pd


_LOG = logging.getLogger(__name__)


def rolling_corr(df, com, min_periods):
    """
    df a pd.DataFrame with datetime index and cols for instruments.
    Entries assumed to be returns (p_{t + 1} / p_t - 1).
    """
    _LOG.info('df.shape = %s', df.shape)
    _LOG.info('Rows with nans will be ignored.')
    _LOG.info('Calculating rolling correlation...')
    return df.dropna(how='any').ewm(com=com, min_periods=min_periods).corr().dropna(how='any')


def rolling_cov(df, com, min_periods):
    """
    df a pd.DataFrame with datetime index and cols for instruments.
    Entries assumed to be returns (p_{t + 1} / p_t - 1).
    """
    _LOG.info('df.shape = %s', df.shape)
    _LOG.info('Rows with nans will be ignored.')
    _LOG.info('Calculating rolling covariance...')
    return df.dropna(how='any').ewm(com=com, min_periods=min_periods).cov().dropna(how='any')


def cov_df_to_inv(df):
    """
    Invert cov/corr matrices given as output of ewm cov/corr.
    """
    _LOG.info("columns are %s", str(df.columns.values))
    cov = df.values
    num_rows = cov.shape[0]
    _LOG.info("num rows = %i", num_rows)
    num_cols = cov.shape[1]
    _LOG.info("num cols = %i", num_cols)
    num_mats = int(num_rows / num_cols)
    _LOG.info("num (square) matrices = %i", num_mats)
    mats = np.reshape(cov, [num_mats, num_cols, num_cols])
    _LOG.info("mat.shape = %s", str(mats.shape))
    return np.linalg.inv(mats)


def equal_weighting(df):
    """
    Equally weight returns in df and generate stream of log rets.
    """
    rets = df.dropna(how='any').mean(axis=1)
    log_rets = np.log(rets + 1)
    return log_rets


# TODO: Return weights themselves
def inverse_volatility_weighting(df, com, min_periods):
    """
    Weight returns by inverse volatility (calculated by rolling std).
    """
    inv_vol = 1. / df.dropna(how='any').ewm(com=com, min_periods=min_periods).std().dropna(how='any')
    total = inv_vol.sum(axis=1) 
    weight = inv_vol.divide(total, axis=0)
    # Shift weights
    weight = weight.shift(1)
    weighted = df.dropna(how='any').multiply(weight, axis=0)
    rets = weighted.sum(axis=1)
    log_rets = np.log(rets + 1)
    return log_rets


def minimum_variance_weighting(df, com, min_periods):
    """
    Weight returns by inverse covariance (calculating by rolling cov).
    """
    cov = rolling_cov(df, com, min_periods)
    inv_cov = cov_df_to_inv(cov)
    weights = np.divide(inv_cov.sum(axis=1),
                        inv_cov.sum(axis=1).sum(axis=1, keepdims=True))
    weights_df = pd.DataFrame(data=weights,
                              index=cov.index.get_level_values(0).drop_duplicates(),
                              columns=cov.columns)
    weights_df = weights_df.shift(1)
    rets = df.dropna(how='any').multiply(weights_df, axis=0).sum(axis=1)
    log_rets = np.log(rets + 1)
    return log_rets
