"""
Import as:

import core.statistics.correlation as cstacorr
"""

import logging

import numpy as np
import pandas as pd
import scipy as sp

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_distance_matrix(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute the distance matrix of `df`, treating rows as samples.

    :param df: numeric n x m dataframe (n samples of m dimensions)
    :return: n x n distance matrix
    """
    if isinstance(df, pd.Series):
        df = df.to_frame()
    hdbg.dassert_isinstance(df, pd.DataFrame)
    idx = df.index
    distance_matrix = sp.spatial.distance_matrix(df, df)
    out_df = pd.DataFrame(distance_matrix, idx, idx.to_list())
    return out_df


def compute_doubly_centered_distance_matrix(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute the "doubly centered" distance matrix.

    :param df: as in `compute_distance_matrix()`
    :return: return distance matrix re-centered row-wise and column-wise.
    """
    if isinstance(df, pd.Series):
        df = df.to_frame()
    distance_matrix_df = compute_distance_matrix(df)
    #
    col_mean = distance_matrix_df.mean(axis=0)
    row_mean = distance_matrix_df.mean(axis=1)
    grand_mean = col_mean.mean()
    #
    centered = distance_matrix_df
    centered = centered.subtract(col_mean, axis=0)
    centered = centered.subtract(row_mean, axis=1)
    centered = centered + grand_mean
    return centered


def compute_distance_covariance(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> float:
    """
    Compute the "distance covariance" (Szekely, Rizzo, Bakirov).

    :param df1: numeric n x m_1 dataframe
    :param df2: numeric n x m_2 dataframe
    :return: distance covariance
    """
    if isinstance(df1, pd.Series):
        df1 = df1.to_frame()
    if isinstance(df2, pd.Series):
        df2 = df2.to_frame()
    hdbg.dassert_isinstance(df1, pd.DataFrame)
    hdbg.dassert_isinstance(df2, pd.DataFrame)
    n_samples_1 = df1.shape[0]
    n_samples_2 = df2.shape[0]
    hdbg.dassert_eq(n_samples_1, n_samples_2)
    #
    doubly_centered_distance_matrix_1 = compute_doubly_centered_distance_matrix(
        df1
    )
    doubly_centered_distance_matrix_2 = compute_doubly_centered_distance_matrix(
        df2
    )
    #
    hadamard_product = doubly_centered_distance_matrix_1.multiply(
        doubly_centered_distance_matrix_2
    )
    distance_covariance = hadamard_product.mean().mean()
    return distance_covariance


def compute_distance_correlation(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute the "distance correlation" (Szekely, Rizzo, Bakirov).

    This is a number in [0, 1].

    :param df1: numeric n x m_1 dataframe
    :param df2: numeric n x m_2 dataframe
    :return: distance covariance
    """
    distance_covariance = compute_distance_covariance(df1, df2)
    distance_variance_1 = compute_distance_covariance(df1, df1)
    distance_variance_2 = compute_distance_covariance(df2, df2)
    distance_correlation = distance_covariance / np.sqrt(
        distance_variance_1 * distance_variance_2
    )
    return distance_correlation
