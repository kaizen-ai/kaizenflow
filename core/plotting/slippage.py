"""
Import as:

import core.plotting.slippage as cploslip
"""

import logging

import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def plot_slippage_boxplot(
    df: pd.DataFrame,
    grouping: str = "by_time",
    ylabel: str = "",
) -> None:
    """
    Plot boxplots of slippage.

    :param df: time-indexed dataframe with instruments as columns
    :param grouping: x-axis grouping; "by_time" or "by_asset"
    :param ylabel: ylabel label
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_eq(df.columns.nlevels, 1)
    if grouping == "by_time":
        data = df.T
    elif grouping == "by_asset":
        data = df
    else:
        raise ValueError("Unrecognized grouping %s" % grouping)
    rot = 45
    ax = data.boxplot(rot=rot, ylabel=ylabel)
    ax.axhline(0, c="b")
