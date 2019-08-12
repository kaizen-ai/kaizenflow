# Rolling correlation, covariance, and matrix inversion functions.


import logging

import helpers.dbg as dbg
import numpy as np
import pandas as pd


_LOG = logging.getLogger(__name__)


# There are many possible ways of defining moving correlation.
# See, for example, the discussion in Section 3.3.13 of Dacorogna, et al. in
# "An Introduction to High-Frequency Finance".

# Notes on ewm corr/cov in Pandas
#
# Discussion: 
# https://pandas.pydata.org/pandas-docs/stable/user_guide/computation.html#exponentially-weighted-windows
#
# Implementation:
# https://github.com/pandas-dev/pandas/blob/v0.25.0/pandas/core/window.py
# https://github.com/pandas-dev/pandas/blob/v0.25.0/pandas/_libs/window.pyx


def ewm_cov(df, com, min_periods, adjust=True, ignore_na=False, axis=0):
    """
    Accepts df of % returns and calculates ewm covariance matrix
    """
    _LOG.info("df num rows = %i", df.shape[0])
    _LOG.info("df num rows with no NaNs = %i", df.dropna(how='any').shape[0])
    # Convert to log returns for the purpose of calculating covariance. 
    log_df = np.log(df + 1)
    cov = log_df.ewm(com=com, min_periods=min_periods, adjust=adjust,
                     ignore_na=ignore_na, axis=axis).cov()
    _LOG.info("cov num matrices = %i", cov.shape[0] / cov.shape[1])
    return cov


def cov_df_to_inv(df):
    """
    Invert cov/corr matrices given as output of ewm cov/corr.
    """
    _LOG.info("Calculating matrix inverses...")
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


# TODO(Paul): Add sklearn covariance/precision estimators and make them rolling
# See https://scikit-learn.org/stable/modules/classes.html#module-sklearn.covariance.
