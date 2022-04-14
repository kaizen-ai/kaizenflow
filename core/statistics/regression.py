"""
Import as:

import core.statistics.regression as cstaregr
"""

import logging
from typing import List, Optional, Union

import numpy as np
import pandas as pd
import scipy as sp

import core.statistics.entropy as cstaentr
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_regression_coefficients(
    df: pd.DataFrame,
    x_cols: List[Union[int, str]],
    y_col: Union[int, str],
    sample_weight_col: Optional[Union[int, str]] = None,
) -> pd.DataFrame:
    """
    Regresses `y_col` on each `x_col` independently.

    This function assumes (but does not check) that the x and y variables are
    centered.

    :param df: data dataframe
    :param x_cols: x variable columns
    :param y_col: y variable column
    :param sample_weight_col: optional nonnegative sample observation weights.
        If `None`, then equal weights are used. Weights do not need to be
        normalized.
    :return: dataframe of regression coefficients and related stats
    """
    hdbg.dassert(not df.empty, msg="Dataframe must be nonempty")
    hdbg.dassert_isinstance(x_cols, list)
    hdbg.dassert_is_subset(x_cols, df.columns)
    hdbg.dassert_isinstance(y_col, (int, str))
    hdbg.dassert_in(y_col, df.columns)
    # Sanity check weight column, if available. Set weights uniformly to 1 if
    # not specified.
    if sample_weight_col is not None:
        hdbg.dassert_in(sample_weight_col, df.columns)
        weights = df[sample_weight_col].rename("weight")
    else:
        weights = pd.Series(index=df.index, data=1, name="weight")
    # Ensure that no weights are negative.
    hdbg.dassert(not (weights < 0).any())
    # Ensure that the total weight is positive.
    hdbg.dassert((weights > 0).any())
    # Drop rows with no y value.
    _LOG.debug("y_col=`%s` count=%i", y_col, df[y_col].count())
    df = df.dropna(subset=[y_col])
    # Reindex weights to reflect any dropped y values.
    weights = weights.reindex(df.index)
    # Extract x variables.
    x_vars = df[x_cols]
    x_var_counts = x_vars.count().rename("count")
    # Create a per-`x_col` weight dataframe to reflect possibly different NaN
    # positions. This is used to generate accurate weighted sums and effective
    # sample sizes.
    weight_df = pd.DataFrame(index=x_vars.index, columns=x_cols)
    for col in x_cols:
        weight_df[col] = weights.reindex(x_vars[col].dropna().index)
    weight_sums = weight_df.sum(axis=0)
    # We use the 2-cardinality of the weights. This is equivalent to using
    # Kish's effective sample size.
    x_var_eff_counts = weight_df.apply(
        lambda x: cstaentr.compute_cardinality(x.dropna(), 2)
    ).rename("eff_count")
    # Calculate sign correlation.
    sgn_rho = (
        x_vars.multiply(df[y_col], axis=0)
        .apply(np.sign)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("sgn_rho")
    )
    # Calculate variance assuming x variables are centered at zero.
    x_variance = (
        x_vars.pow(2)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("var")
    )
    # Calculate covariance assuming x variables and y variable are centered.
    covariance = (
        x_vars.multiply(df[y_col], axis=0)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("covar")
    )
    # Calculate y variance assuming variable is centered.
    # NOTE: We calculate only one estimate of the variance of y, using all
    #     available data (regardless of whether a particular `x_col` is NaN or
    #     not). If samples of `x_cols` are not substantially aligned, then this
    #     may be undesirable.
    y_variance = df[y_col].pow(2).multiply(weights).sum() / weights.sum()
    _LOG.debug("y_col=`%s` variance=%f", y_col, y_variance)
    # Calculate correlation from covariances and variances.
    rho = covariance.divide(np.sqrt(x_variance) * np.sqrt(y_variance)).rename(
        "rho"
    )
    # Calculate beta coefficients and associated statistics.
    beta = covariance.divide(x_variance).rename("beta")
    # The `x_var_eff_counts` term makes this invariant with respect to
    # rescalings of the weight column.
    beta_se = np.sqrt(
        y_variance / (x_variance.multiply(x_var_eff_counts))
    ).rename("SE(beta)")
    z_scores = beta.divide(beta_se).rename("beta_z_scored")
    # Calculate two-sided p-values.
    p_val_array = 2 * sp.stats.norm.sf(z_scores.abs())
    p_val = pd.Series(index=z_scores.index, data=p_val_array, name="p_val_2s")
    # Calculate autocovariance-related stats of x variables.
    autocovariance = (
        x_vars.multiply(x_vars.shift(1), axis=0)
        .multiply(weight_df, axis=0)
        .sum(axis=0)
        .divide(weight_sums)
        .rename("autocovar")
    )
    # Normalize autocovariance to get autocorrelation.
    autocorrelation = autocovariance.divide(x_variance).rename("autocorr")
    turn = np.sqrt(2 * (1 - autocorrelation)).rename("turn")
    # Consolidate stats.
    coefficients = [
        x_var_counts,
        x_var_eff_counts,
        sgn_rho,
        x_variance,
        covariance,
        rho,
        beta,
        beta_se,
        z_scores,
        p_val,
        autocovariance,
        autocorrelation,
        turn,
    ]
    return pd.concat(coefficients, axis=1)
