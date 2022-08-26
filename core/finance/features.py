"""
Import as:

import core.finance.features as cfinfeat
"""

import logging

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_stochastic(
    df: pd.DataFrame,
    high_col: str,
    low_col: str,
    close_col: str,
    apply_log: bool = False,
) -> pd.DataFrame:
    """
    Compute intrabar close price relative to high/low range.

    The feature is scaled so that it lies between -1 and +1.

    :param df: dataframe of high, low, and close values
    :param high_col: name of high-value col
    :param low_col:  name of low-value col
    :param close_col: name of close value col
    :param apply_log: apply `log()` to data prior to calculation iff True
    :return: 1-col dataframe with indicator
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    cols = [high_col, low_col, close_col]
    hdbg.dassert_container_type(cols, container_type=list, elem_type=str)
    hdbg.dassert_is_subset(cols, df.columns)
    #
    hlc = df[cols]
    if apply_log:
        hlc = np.log(hlc)
    high = hlc[high_col]
    low = hlc[low_col]
    close = hlc[close_col]
    stochastic = (2 * close - high - low) / (high - low)
    stochastic.name = "stochastic"
    return stochastic.to_frame()