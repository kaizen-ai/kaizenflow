"""
Import as:

import core.plotting.boxplot as cploboxp
"""

import logging

import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def plot_boxplot(
    df: pd.DataFrame,
    grouping: str = "by_row",
    ylabel: str = "",
) -> None:
    """
    Plot boxplots of slippage.

    :param df: time-indexed dataframe with instruments as columns
    :param grouping: x-axis grouping; "by_row" or "by_col"
    :param ylabel: ylabel label
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_eq(df.columns.nlevels, 1)
    if grouping == "by_row":
        data = df.T
    elif grouping == "by_col":
        data = df
    else:
        raise ValueError("Unrecognized grouping %s" % grouping)
    rot = 45
    ax = data.boxplot(rot=rot, ylabel=ylabel)
    ax.axhline(0, c="b")


def plot_bid_ask_price_levels(
    bids: pd.Series, asks: pd.Series, *, num_levels: int = 50
) -> None:
    """
    Given bids and asks price levels data, plot the bar graph.

    :param bids: pd.Series with index as bid price level and value as
        quantity at that level
    :param asks: pd.Series with index as ask price level and value as
        quantity at that level
    :param num_levels: number of price levels to display
    """
    # Get the data for the specified number of levels
    bid_levels = bids.head(num_levels)
    ask_levels = asks.head(num_levels)
    # Create a DataFrame for the plot
    df = pd.DataFrame({"Bids": bid_levels, "Asks": ask_levels})
    # Create a line plot with steps-mid draw style
    ax = df.plot(drawstyle="steps-mid", figsize=(12, 6), color=["blue", "red"])
    # Fill the area under the step plot
    ax.fill_between(
        bid_levels.index, bid_levels.values, step="mid", color="blue", alpha=0.4
    )
    ax.fill_between(
        ask_levels.index, ask_levels.values, step="mid", color="red", alpha=0.4
    )
    # Set labels and title
    ax.set_xlabel("Price")
    ax.set_ylabel("Quantity")
    ax.set_title("Bids and Asks")
    ax.legend(["Bids", "Asks"])
