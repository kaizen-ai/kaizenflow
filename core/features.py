"""
Import as:

import core.features as ftrs
"""

import collections
import logging

import pandas as pd

import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


# TODO(gp): Add also the inverse.
def reindex_to_integers(df):
    """
    Create a new index using integers and move the datetime to a column (e.g.,
    because sklearn wants indices to be integers).
    The operation is performed in-place.
    """
    dbg.dassert_eq(df.index.name, "datetime")
    df.insert(0, "datetime", df.index)
    df.index = range(df.shape[0])
    df.index.name = "ids"
    return df


def get_lagged_feature_names(y_var, delay_lag, num_lags):
    dbg.dassert(
        y_var.endswith("_0"),
        "y_var='%s' is not a valid name to generate lagging variables",
        y_var,
    )
    if delay_lag < 1:
        _LOG.warning(
            "Using anticausal features since delay_lag=%d < 1. This "
            "could be lead to future peeking",
            delay_lag,
        )
    dbg.dassert_lte(1, num_lags)
    #
    x_var_shifts = list(range(1 + delay_lag, 1 + delay_lag + num_lags))
    x_vars = []
    for i in x_var_shifts:
        x_var = y_var.replace("_0", "_%d" % i)
        x_vars.append(x_var)
    dbg.dassert_eq(len(x_vars), len(x_var_shifts))
    return x_var_shifts, x_vars


def compute_lagged_features(df, y_var, delay_lag, num_lags):
    """
    Compute features by adding lags of `y_var` in `df`.
    The operation is performed in-place.

    :return: transformed df and info about the transformation.
    """
    info = collections.OrderedDict()
    dbg.dassert_in(y_var, df.columns)
    _LOG.debug("df.shape=%s", df.shape)
    #
    _LOG.debug("y_var='%s'", y_var)
    info["y_var"] = y_var
    x_var_shifts, x_vars = get_lagged_feature_names(y_var, delay_lag, num_lags)
    _LOG.debug("x_vars=%s", x_vars)
    info["x_vars"] = x_vars
    for i, num_shifts in enumerate(x_var_shifts):
        x_var = x_vars[i]
        _LOG.debug("Computing var=%s", x_var)
        df[x_var] = df[y_var].shift(num_shifts)
    # TODO(gp): Add dropna stats using exp.dropna().
    info["before_df.shape"] = df.shape
    df = df.dropna()
    _LOG.debug("df.shape=%s", df.shape)
    info["after_df.shape"] = df.shape
    return df, info


def compute_lagged_columns(
    df: pd.DataFrame, lag_delay: int, num_lags: int
) -> pd.DataFrame:
    """
    Computes lags of each column in df.
    """
    if lag_delay < 1:
        _LOG.warning(
            "Using anticausal features since lag_delay=%d < 1. This "
            "could be lead to future peeking.",
            lag_delay,
        )
    dbg.dassert_lte(1, num_lags)
    #
    shifts = list(range(1 + lag_delay, 1 + lag_delay + num_lags))
    out_cols = []
    for col in df.columns:
        for num_shifts in shifts:
            out_col = df[col].shift(num_shifts)
            out_col.name += "_%i" % num_shifts
            out_cols.append(out_col)
    return pd.concat(out_cols, axis=1)
