"""
Import as:

import core.finance.bid_ask as cfibiask
"""
import logging
from typing import List, Optional

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def process_bid_ask(
    df: pd.DataFrame,
    bid_col: str,
    ask_col: str,
    bid_volume_col: str,
    ask_volume_col: str,
    requested_cols: Optional[List[str]] = None,
    join_output_with_input: bool = False,
) -> pd.DataFrame:
    """
    Process top-of-book bid/ask quotes.

    :param df: dataframe with columns for top-of-book bid/ask info
    :param bid_col: bid price column
    :param ask_col: ask price column
    :param bid_volume_col: column with quoted volume at bid
    :param ask_volume_col: column with quoted volume at ask
    :param requested_cols: the requested output columns; `None` returns all
        available.
    :param join_output_with_input: whether to only return the requested columns
        or to join the requested columns to the input dataframe
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_in(bid_col, df.columns)
    hdbg.dassert_in(ask_col, df.columns)
    hdbg.dassert_in(bid_volume_col, df.columns)
    hdbg.dassert_in(ask_volume_col, df.columns)
    hdbg.dassert(not (df[bid_col] > df[ask_col]).any())
    supported_cols = [
        "mid",
        "geometric_mid",
        "quoted_spread",
        "relative_spread",
        "log_relative_spread",
        "weighted_mid",
        # These imbalances are with respect to shares.
        "order_book_imbalance",
        "centered_order_book_imbalance",
        "log_order_book_imbalance",
        # TODO: use `notional` instead of `value`.
        "bid_value",
        "ask_value",
        "mid_value",
    ]
    requested_cols = requested_cols or supported_cols
    hdbg.dassert_is_subset(
        requested_cols,
        supported_cols,
        "The available columns to request are %s",
        supported_cols,
    )
    hdbg.dassert(requested_cols)
    requested_cols = set(requested_cols)
    results = []
    if "mid" in requested_cols:
        srs = ((df[bid_col] + df[ask_col]) / 2).rename("mid")
        results.append(srs)
    if "geometric_mid" in requested_cols:
        srs = np.sqrt(df[bid_col] * df[ask_col]).rename("geometric_mid")
        results.append(srs)
    if "quoted_spread" in requested_cols:
        srs = (df[ask_col] - df[bid_col]).rename("quoted_spread")
        results.append(srs)
    if "relative_spread" in requested_cols:
        srs = 2 * (df[ask_col] - df[bid_col]) / (df[ask_col] + df[bid_col])
        srs = srs.rename("relative_spread")
        results.append(srs)
    if "log_relative_spread" in requested_cols:
        srs = (np.log(df[ask_col]) - np.log(df[bid_col])).rename(
            "log_relative_spread"
        )
        results.append(srs)
    if "weighted_mid" in requested_cols:
        srs = (
            df[bid_col] * df[ask_volume_col] + df[ask_col] * df[bid_volume_col]
        ) / (df[ask_volume_col] + df[bid_volume_col])
        srs = srs.rename("weighted_mid")
        results.append(srs)
    if "order_book_imbalance" in requested_cols:
        srs = df[bid_volume_col] / (df[bid_volume_col] + df[ask_volume_col])
        srs = srs.rename("order_book_imbalance")
        results.append(srs)
    if "centered_order_book_imbalance" in requested_cols:
        srs = (df[bid_volume_col] - df[ask_volume_col]) / (
            df[bid_volume_col] + df[ask_volume_col]
        )
        srs = srs.rename("centered_order_book_imbalance")
        results.append(srs)
    if "log_order_book_imbalance" in requested_cols:
        srs = np.log(df[bid_volume_col]) - np.log(df[ask_volume_col])
        srs = srs.rename("log_order_book_imbalance")
        results.append(srs)
    if "bid_value" in requested_cols:
        srs = (df[bid_col] * df[bid_volume_col]).rename("bid_value")
        results.append(srs)
    if "ask_value" in requested_cols:
        srs = (df[ask_col] * df[ask_volume_col]).rename("ask_value")
        results.append(srs)
    if "mid_value" in requested_cols:
        srs = (
            df[bid_col] * df[bid_volume_col] + df[ask_col] * df[ask_volume_col]
        ) / 2
        srs = srs.rename("mid_value")
        results.append(srs)
    out_df = pd.concat(results, axis=1)
    # TODO(gp): Maybe factor out this in a `_maybe_join_output_with_input` since
    #  it seems a common idiom.
    if join_output_with_input:
        out_df = out_df.merge(df, left_index=True, right_index=True, how="outer")
        hdbg.dassert(not out_df.columns.has_duplicates)
    return out_df
