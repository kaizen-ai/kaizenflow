"""
Import as:

import oms.broker.ccxt.ccxt_execution_quality as obccexqu
"""
import logging
from typing import Tuple

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest
import oms.broker.ccxt.ccxt_aggregation_functions as obccagfu

_LOG = logging.getLogger(__name__)


# #############################################################################
# Child order handling.
# #############################################################################


def get_limit_order_price(
    orders: pd.DataFrame, *, freq: str = "1s"
) -> pd.DataFrame:
    """
    Get limit order prices from orders.
    """
    # Generate DataFrame of buy limit orders.
    buys = orders[orders["diff_num_shares"] > 0]
    buys = buys.pivot(
        index="creation_timestamp", columns="asset_id", values="limit_price"
    )
    buys = buys.resample(freq, closed="right", label="right").mean()
    # Generate DataFrame of sell limit orders.
    sells = orders[orders["diff_num_shares"] < 0]
    sells = sells.pivot(
        index="creation_timestamp", columns="asset_id", values="limit_price"
    )
    sells = sells.resample(freq, closed="right", label="right").mean()
    #
    col_set = buys.columns.union(sells.columns)
    col_set = col_set.sort_values()
    buys = buys.reindex(columns=col_set)
    sells = sells.reindex(columns=col_set)
    #
    dict_ = {
        "buy_limit_order_price": buys,
        "sell_limit_order_price": sells,
    }
    df = pd.concat(dict_, axis=1)
    return df


def align_ccxt_orders_and_fills(
    child_order_response_df: pd.DataFrame,
    fills_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cols = [
        "order",
        "order_type",
        "time_in_force",
        "post_only",
        "reduce_only",
        "side",
        "order_price",
        "stop_price",
        "order_amount",
        "order_update_timestamp",
        "order_update_datetime",
    ]
    order_responses = child_order_response_df[cols].set_index("order")
    order_fills = obccagfu.aggregate_fills_by_order(fills_df)
    filled_orders = pd.merge(
        order_responses,
        order_fills,
        how="inner",
        left_index=True,
        right_index=True,
    )
    # TODO(Paul): Compute price improvement for filled orders.
    unfilled_orders = order_responses.loc[
        order_responses.index.difference(order_fills.index)
    ]
    return filled_orders, unfilled_orders


# #############################################################################


def compute_filled_order_execution_quality(
    df: pd.DataFrame,
    tick_decimals: int,
) -> pd.DataFrame:
    """
    Compute price improvement and fill rate for filled orders.

    :param df: a `filled_orders` DataFrame, from
        `align_ccxt_orders_and_fills()`
    :param tick_decimals: number of decimals of rounding for order quantities.
        This helps suppress machine precision artifacts.
    :return: DataFrame indexed by order id with the following columns:
      - direction (buy or sell as +1 or -1, respectively)
      - price_improvement_notional (with respect to limit price)
      - price_improvement_bps
      - underfill_quantity
      - underfill_pct
      - underfill_notional_at_limit_price
      - underfill_notional_at_transaction_price
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    required_cols = [
        "amount",
        "order_amount",
        "order_price",
        "price",
        "side",
    ]
    hdbg.dassert_is_subset(required_cols, df.columns)
    #
    srs_list = []
    is_buy = df["side"] == "buy"
    #
    direction = np.round(2 * is_buy.astype(int) - 1).rename("direction")
    srs_list.append(direction)
    #
    price_improvement_notional = (
        direction.multiply(df["order_price"] - df["price"])
        .round(tick_decimals)
        .replace(-0.0, 0.0)
        .rename("price_improvement_notional")
    )
    srs_list.append(price_improvement_notional)
    #
    price_improvement_bps = 1e4 * (
        price_improvement_notional / df["order_price"]
    ).rename("price_improvement_bps")
    srs_list.append(price_improvement_bps)
    #
    underfill_quantity = (df["order_amount"] - df["amount"]).rename(
        "underfill_quantity"
    )
    srs_list.append(underfill_quantity)
    #
    underfill_pct = (underfill_quantity / df["order_amount"]).rename(
        "underfill_pct"
    )
    srs_list.append(underfill_pct)
    #
    underfill_notional_at_limit_price = (
        underfill_quantity * df["order_price"]
    ).rename("underfill_notional_at_limit_price")
    srs_list.append(underfill_notional_at_limit_price)
    #
    underfill_notional_at_transaction_price = (
        underfill_quantity * df["price"]
    ).rename("underfill_notional_at_transaction_price")
    srs_list.append(underfill_notional_at_transaction_price)
    #
    return pd.concat(srs_list, axis=1)


def _check_buy_sell_overlap(fills_df: pd.DataFrame) -> None:
    """
    Check that `buy_count` and `sell_count` values don't overlap in the same
    timestamp index & are non-negative.

    There are 2 valid cases:
      - one is more that zero, another one is zero
      - both of them are zero

    :param fills_df: a fills dataframe aggregated by bar
    """
    for timestamp, sub_df in fills_df.groupby(level="bar_end_datetime"):
        # Check that buy and sell counts don't overlap.
        valid = (sub_df["buy_count"] > 0) ^ (sub_df["sell_count"] > 0) | (
            (sub_df["buy_count"] == 0) & (sub_df["sell_count"] == 0)
        )
        hdbg.dassert(
            valid.all(),
            f"Invalid buy/sell overlap at `{timestamp}`:\n\
            {hunitest.convert_df_to_string(sub_df, index=True)}",
        )
        # Check that buy and sell counts are non-negative.
        no_negative = (sub_df["buy_count"] >= 0) & (sub_df["sell_count"] >= 0)
        hdbg.dassert(
            no_negative.all(),
            f"Negative buy/sell count at `{timestamp}`:\n\
            {hunitest.convert_df_to_string(sub_df, index=True)}",
        )


def convert_bar_fills_to_portfolio_df(
    fills_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create a portfolio dataframe from a fills dataframe.

    :param fills_df: a fills dataframe aggregated by bar
    :param price_df: a bar reference price (e.g., TWAP or VWAP)
    :return: a portfolio-style dataframe, with holdings and trades in both
        shares and notional
    """
    cols = [
        "buy_count",
        "sell_count",
        "cost",
        "amount",
    ]
    hdbg.dassert_is_subset(cols, fills_df.columns)
    hpandas.dassert_time_indexed_df(
        fills_df, allow_empty=False, strictly_increasing=True
    )
    hpandas.dassert_time_indexed_df(
        price_df, allow_empty=False, strictly_increasing=True
    )
    hdbg.dassert_eq(2, fills_df.index.nlevels)
    _check_buy_sell_overlap(fills_df)
    buy = (fills_df["buy_count"] > 0).astype(int)
    sell = (fills_df["sell_count"] > 0).astype(int)
    # Buy = +1, sell = -1.
    side = buy - sell
    # Compute "executed_trades_notional".
    signed_cost = side * fills_df["cost"]
    executed_trades_notional = signed_cost.unstack().fillna(0)
    # Compute "executed_trades_shares".
    signed_amount = side * fills_df["amount"]
    executed_trades_shares = signed_amount.unstack().fillna(0)
    # Compute "holdings_shares" by cumulatively summing executed trades.
    holdings_shares = executed_trades_shares.cumsum()
    # Compute "holdings_notional" by multiplying holdings in shares by
    #   reference price.
    holdings_notional = holdings_shares.multiply(price_df)[
        holdings_shares.columns
    ].fillna(0)
    # Compute PnL.
    pnl = holdings_notional.subtract(
        holdings_notional.shift(1), fill_value=0
    ).subtract(executed_trades_notional, fill_value=0)
    # Construct the portfolio dataframe.
    portfolio_df = pd.concat(
        {
            "holdings_notional": holdings_notional,
            "holdings_shares": holdings_shares,
            "executed_trades_notional": executed_trades_notional,
            "executed_trades_shares": executed_trades_shares,
            "pnl": pnl,
        },
        axis=1,
    )
    return portfolio_df


def convert_parent_orders_to_target_position_df(
    parent_order_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create a target position dataframe from a parent orders dataframe.

    :param parent_order_df: a parent order dataframe (bar-level)
    :param price_df: a bar reference price (e.g., TWAP or VWAP)
    :return: a target position-style dataframe
    """
    cols = [
        "asset_id",
        "curr_num_shares",
        "diff_num_shares",
    ]
    hdbg.dassert_is_subset(cols, parent_order_df.columns)
    hpandas.dassert_time_indexed_df(
        price_df, allow_empty=False, strictly_increasing=True
    )
    price_df_freq = price_df.index.freq
    hdbg.dassert(price_df_freq is not None)
    # Extract holdings and shift index so that end-of-bar holdings are
    # reflected.
    holdings_shares = parent_order_df.pivot(
        index="end_timestamp", columns="asset_id", values="curr_num_shares"
    ).ffill()
    holdings_shares.index.freq = price_df_freq
    holdings_shares.index = holdings_shares.index.shift(-1)
    # Multiply by end-of-bar price to get notional holdings.
    holdings_notional = holdings_shares.multiply(price_df)[
        holdings_shares.columns
    ]
    #
    target_trades_shares = parent_order_df.pivot(
        index="end_timestamp", columns="asset_id", values="diff_num_shares"
    ).fillna(0)
    target_trades_shares.index.freq = price_df_freq
    target_trades_shares.index = target_trades_shares.index.shift(-1)
    #
    target_holdings_shares = holdings_shares.shift(1).add(
        target_trades_shares, fill_value=0
    )
    target_holdings_notional = target_holdings_shares.multiply(price_df)[
        target_trades_shares.columns
    ].fillna(0)
    target_position_df = pd.concat(
        {
            "holdings_notional": holdings_notional,
            "holdings_shares": holdings_shares,
            "price": price_df,
            "target_holdings_notional": target_holdings_notional,
            "target_holdings_shares": target_holdings_shares,
            "target_trades_shares": target_trades_shares,
        },
        axis=1,
    )
    # This can (and should) raise an error if the index cannot be conformed.
    target_position_df.index.freq = price_df_freq
    # Trim last end-of-bar artifact.
    target_position_df = target_position_df.iloc[:-1]
    return target_position_df


# #############################################################################
# Fee summaries
# #############################################################################


def generate_fee_summary(
    fills_df: pd.DataFrame, group_by_col: str
) -> pd.DataFrame:
    """
    Summarize fees according to `group_by_col`.

    :param fills_df: the input DataFrame.
    :param group_by_col: one of
        ["is_buy", "is_maker", "is_positive_realized_pnl"].
    """
    hdbg.dassert_in(
        group_by_col, ["is_buy", "is_maker", "is_positive_realized_pnl"]
    )
    fills_df_copy = fills_df.copy()
    fills_df_copy["is_buy"] = fills_df["side"] == "buy"
    fills_df_copy["is_maker"] = fills_df["takerOrMaker"] == "maker"
    fills_df_copy["is_positive_realized_pnl"] = fills_df["realized_pnl"] > 0
    # Any row may have NaN issue (tests 3,4). Current fix: reindex with below.
    # Handle the non-common parts: `groupby_df`, `positive_realized_pnl` (only
    # when grouping by "takerOrMaker"), and `is_maker` (only when
    # grouping by `realized_pnl`).
    groupby_df = fills_df_copy.groupby(group_by_col)
    # Use common formulas for both cases.
    fill_count = groupby_df[group_by_col].count().rename("fill_count")
    fill_fees_dollars = (
        groupby_df["transaction_cost"].sum().rename("fill_fees_dollars")
    )
    #
    traded_dollar_volume = (
        groupby_df["cost"].sum().rename("traded_volume_dollars")
    )
    #
    fill_fees_bps = (1e4 * fill_fees_dollars / traded_dollar_volume).rename(
        "fill_fees_bps"
    )
    #
    realized_pnl_dollars = (
        groupby_df["realized_pnl"].sum().rename("realized_pnl_dollars")
    )
    #
    realized_pnl_bps = (1e4 * realized_pnl_dollars / traded_dollar_volume).rename(
        "realized_pnl_bps"
    )
    is_buy = groupby_df["is_buy"].sum()
    is_maker = groupby_df["is_maker"].sum()
    is_positive_realized_pnl = groupby_df["is_positive_realized_pnl"].sum()
    #
    fill_count["combined"] = fills_df.shape[0]
    fill_fees_dollars["combined"] = fills_df["transaction_cost"].sum()
    traded_dollar_volume["combined"] = fills_df["cost"].sum()
    fill_fees_bps["combined"] = (
        1e4
        * fills_df["transaction_cost"].sum()
        / traded_dollar_volume["combined"]
    )
    realized_pnl_dollars["combined"] = fills_df["realized_pnl"].sum()
    realized_pnl_bps["combined"] = (
        1e4 * fills_df["realized_pnl"].sum() / traded_dollar_volume["combined"]
    )
    is_buy["combined"] = fills_df_copy["is_buy"].sum()
    is_maker["combined"] = fills_df_copy["is_maker"].sum()
    is_positive_realized_pnl["combined"] = fills_df_copy[
        "is_positive_realized_pnl"
    ].sum()
    #
    srs_list = []
    srs_list.append(fill_count)
    srs_list.append(traded_dollar_volume)
    srs_list.append(fill_fees_dollars)
    srs_list.append(fill_fees_bps)
    srs_list.append(realized_pnl_dollars)
    srs_list.append(realized_pnl_bps)
    srs_list.append(is_buy)
    srs_list.append(is_maker)
    srs_list.append(is_positive_realized_pnl)
    #
    df = pd.concat(srs_list, axis=1).T
    return df
