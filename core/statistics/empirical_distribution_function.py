"""
Import as:

import core.statistics.empirical_distribution_function as cstaecdf
"""

import logging

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_empirical_cdf(srs: pd.Series) -> pd.Series:
    """
    Compute the empirical cdf from data.

    :param srs: data series
    :return: series with
      - x values equal to sorted `srs`
      - y values equal to percentage of data leq x value
    """
    hdbg.dassert_isinstance(srs, pd.Series)
    # Sort the series and drop NaNs.
    sorted_srs = srs.sort_values().dropna()
    # The new indexed comes from the sorted values. Rename for clarity.
    new_idx = sorted_srs.values
    # Create the values representing cumulative percentage seen.
    count = sorted_srs.count()
    new_values = range(1, count + 1) / (count + 1)
    ecdf = pd.Series(new_values, new_idx, name="ecdf")
    return ecdf


def compute_empirical_cdf_with_bounds(
    srs: pd.Series, alpha: float
) -> pd.DataFrame:
    """
    Compute the empirical cdf from data.

    The confidence interval is calculated using the
    Dvoretzky–Kiefer–Wolfowitz inequality.

    :param srs: data series
    :param alpha: interval contains true CDF with probability 1 - alpha
    :return: dataframe with the ecdf and lower/upper CI bounds
    """
    hdbg.dassert_lt(0, alpha)
    hdbg.dassert_lt(alpha, 1)
    ecdf = compute_empirical_cdf(srs)
    count = ecdf.count()
    epsilon = np.sqrt(np.log(2 / alpha) / (2 * count))
    lower_CI = (ecdf - epsilon).clip(lower=0.0).rename("lower_CI")
    upper_CI = (ecdf + epsilon).clip(upper=1.0).rename("upper_CI")
    df = pd.concat([ecdf, lower_CI, upper_CI], axis=1)
    return df
