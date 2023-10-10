"""
Import as:

import core.finance.target_position_df_processing.combine_with_portfolio_df as cftpdpcwpd
"""

import logging

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


def compute_notional_costs(
    portfolio_df: pd.DataFrame,
    target_position_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute notional slippage and underfill costs.

    This is more accurate than slippage computed from `Portfolio` alone,
    because `target_position_df` provides baseline prices even when
    `holdings_shares` is zero (in which case we cannot compute the
    baseline price from `Portfolio`).
    """
    executed_trades_shares = portfolio_df["executed_trades_shares"]
    target_trades_shares = target_position_df["target_trades_shares"]
    underfill_share_count = (
        target_trades_shares.shift(1).abs() - executed_trades_shares.abs()
    )
    # Get baseline price.
    price = target_position_df["price"]
    # Compute underfill opportunity cost with respect to baseline price.
    side = np.sign(target_position_df["target_trades_shares"].shift(2))
    underfill_notional_cost = (
        side * underfill_share_count.shift(1) * price.subtract(price.shift(1))
    )
    # Compute notional slippage.
    executed_trades_notional = portfolio_df["executed_trades_notional"]
    slippage_notional = executed_trades_notional - (
        price * executed_trades_shares
    )
    # Aggregate results.
    cost_df = pd.concat(
        {
            "underfill_notional_cost": underfill_notional_cost,
            "slippage_notional": slippage_notional,
        },
        axis=1,
    )
    return cost_df


def summarize_notional_costs(df: pd.DataFrame, aggregation: str) -> pd.DataFrame:
    """
    Aggregate notional costs by bar or by asset.

    :param df: output of `compute_notional_costs()`
    :param aggregation: "by_bar" or "by_asset"
    :return: dataframe with columns [
        "slippage_notional",
        "underfill_notional_cost",
        "net",
      ]
    """
    if aggregation == "by_bar":
        agg = df.groupby(axis=1, level=0).sum(min_count=1)
    elif aggregation == "by_asset":
        agg = df.sum(min_count=1).unstack(level=-1)
        agg = agg.T
    else:
        raise ValueError("Invalid aggregation=%s", aggregation)
    agg["net"] = agg.sum(axis=1, min_count=1)
    return agg


def apply_costs_to_baseline(
    baseline_portfolio_stats_df: pd.DataFrame,
    portfolio_stats_df: pd.DataFrame,
    portfolio_df: pd.DataFrame,
    target_position_df: pd.DataFrame,
) -> pd.DataFrame:
    srs = []
    # Add notional pnls.
    baseline_pnl = baseline_portfolio_stats_df["pnl"].rename("baseline_pnl")
    srs.append(baseline_pnl)
    pnl = portfolio_stats_df["pnl"].rename("pnl")
    srs.append(pnl)
    # Compute notional costs.
    costs = compute_notional_costs(portfolio_df, target_position_df)
    slippage = costs["slippage_notional"].sum(axis=1).rename("slippage_notional")
    srs.append(slippage)
    underfill_cost = (
        costs["underfill_notional_cost"]
        .sum(axis=1)
        .rename("underfill_notional_cost")
    )
    srs.append(underfill_cost)
    # Adjust baseline pnl by costs.
    baseline_pnl_minus_costs = (baseline_pnl - slippage - underfill_cost).rename(
        "baseline_pnl_minus_costs"
    )
    srs.append(baseline_pnl_minus_costs)
    # Compare adjusted baseline pnl to pnl.
    baseline_pnl_minus_costs_minus_pnl = (baseline_pnl_minus_costs - pnl).rename(
        "baseline_pnl_minus_costs_minus_pnl"
    )
    srs.append(baseline_pnl_minus_costs_minus_pnl)
    # Compare baseline pnl to pnl.
    baseline_pnl_minus_pnl = (baseline_pnl - pnl).rename("baseline_pnl_minus_pnl")
    srs.append(baseline_pnl_minus_pnl)
    df = pd.concat(srs, axis=1)
    return df
