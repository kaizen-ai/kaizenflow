"""
Import as:

import core.signal_processing.cross_sectional as csprcrse
"""

import logging

import pandas as pd
import sklearn

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def gaussian_rank(
    df: pd.DataFrame,
    *,
    n_quantiles=1001,
) -> pd.DataFrame:
    """
    Perform row-wise Gaussian ranking.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    quantile_transformer = sklearn.preprocessing.QuantileTransformer(
        n_quantiles=n_quantiles,
        output_distribution="normal",
    )
    gaussian_ranking = quantile_transformer.fit_transform(df.T.values).T
    df = pd.DataFrame(
        gaussian_ranking,
        df.index,
        df.columns,
    )
    return df
