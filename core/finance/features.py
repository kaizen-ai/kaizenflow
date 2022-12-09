"""
Import as:

import core.finance.features as cfinfeat
"""

import logging

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_midrange(
    df: pd.DataFrame,
    high_col: str,
    low_col: str,
    apply_log: bool = False,
) -> pd.DataFrame:
    """
    Return midrange price.

    :param df: dataframe of high, low, and volume values
    :param high_col: name of high-value col
    :param low_col: name of low-value col
    :param apply_log: apply `log()` to data prior to calculation iff True
    :return: 1-col dataframe
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    cols = [high_col, low_col]
    hdbg.dassert_container_type(cols, container_type=list, elem_type=str)
    hdbg.dassert_is_subset(cols, df.columns)
    #
    hl = df[cols]
    if apply_log:
        hl = np.log(hl)
    high = hl[high_col]
    low = hl[low_col]
    midrange = 0.5 * (high + low)
    if apply_log:
        midrange.name = "log_midrange"
    else:
        midrange.name = "midrange"
    return midrange.to_frame()


def compute_money_transacted(
    df: pd.DataFrame,
    high_col: str,
    low_col: str,
    volume_col: str,
) -> pd.DataFrame:
    """
    Return estimated amount of money transacted in bar.

    :param df: dataframe of high, low, and volume values
    :param high_col: name of high-value col
    :param low_col:  name of low-value col
    :param volume_col: name of volume value col
    :return: 1-col dataframe
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    cols = [high_col, low_col, volume_col]
    hdbg.dassert_container_type(cols, container_type=list, elem_type=str)
    hdbg.dassert_is_subset(cols, df.columns)
    #
    hlv = df[cols]
    high = hlv[high_col]
    low = hlv[low_col]
    volume = hlv[volume_col]
    money = 0.5 * (high + low) * volume
    money.name = "money_transacted"
    return money.to_frame()


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


def normalize_bar(
    df: pd.DataFrame,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
) -> pd.DataFrame:
    """
    Perform standard bar translation/rescaling of price.
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    cols = [open_col, high_col, low_col, close_col, volume_col]
    hdbg.dassert_container_type(cols, container_type=list, elem_type=str)
    hdbg.dassert_is_subset(cols, df.columns)
    hdbg.dassert_lte(0.0, df[volume_col].min())
    #
    translated_hlc = df[[high_col, low_col, close_col]].subtract(
        df[open_col], axis=0
    )
    normalized_hlc = translated_hlc.divide(np.sqrt(df[volume_col]), axis=0)
    normalized_hlc = normalized_hlc.replace([-np.inf, np.inf], np.nan)
    normalized_hlc = normalized_hlc.rename(
        columns={
            high_col: "adj_high",
            low_col: "adj_low",
            close_col: "adj_close",
        },
    )
    return normalized_hlc