"""
Import as:

import oms.portfolio.compute_slippage_from_portfolio_df as opcsfpodf
"""

import logging

import numpy as np
import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


def compute_share_prices_and_slippage(
    df: pd.DataFrame,
    join_output_with_input: bool = False,
    # TODO(Paul): Add optional benchmark price df.
) -> pd.DataFrame:
    """
    Compare trade prices against benchmark.

    NOTE: baseline prices are not available when holdings_shares is zero, and
        this may lead to artificial NaNs in the calculations.

    :param df: a portfolio dataframe, with the following columns for
        each asset:
        - holdings_notional
        - holdings_shares
        - executed_trades_notional
        - executed_trades_shares
    :return: dataframe with per-asset
        - holdings_price_per_share
        - trade_price_per_share
        - slippage_in_bps
        - is_benchmark_profitable
    """
    hpandas.dassert_time_indexed_df(
        df, allow_empty=False, strictly_increasing=True
    )
    hdbg.dassert_eq(2, df.columns.nlevels)
    cols = [
        "holdings_notional",
        "holdings_shares",
        "executed_trades_notional",
        "executed_trades_shares",
    ]
    hdbg.dassert_is_subset(cols, df.columns.levels[0])
    # Compute price per share of holdings (using holdings reference price).
    # We assume that holdings are computed with a benchmark price (e.g., TWAP).
    holdings_price_per_share = df["holdings_notional"] / df["holdings_shares"]
    # We do not expect negative prices.
    hdbg.dassert_lte(0, holdings_price_per_share.min().min())
    # Compute price per share of trades (using execution reference prices).
    trade_price_per_share = (
        df["executed_trades_notional"] / df["executed_trades_shares"]
    )
    hdbg.dassert_lte(0, trade_price_per_share.min().min())
    # Buy = +1, sell = -1.
    buy = (df["executed_trades_notional"] > 0).astype(int)
    sell = (df["executed_trades_notional"] < 0).astype(int)
    side = buy - sell
    # Compute notional slippage against benchmark.
    slippage_notional_per_share = side * (
        trade_price_per_share - holdings_price_per_share
    )
    slippage_notional = (
        slippage_notional_per_share * df["executed_trades_shares"].abs()
    )
    # Compute slippage in bps.
    slippage_in_bps = 1e4 * slippage_notional_per_share / holdings_price_per_share
    # Determine whether the trade, if closed at t+1, would be profitable if
    # executed at the benchmark price on both legs.
    is_benchmark_profitable = side * np.sign(
        holdings_price_per_share.diff().shift(-1)
    )
    benchmark_return_notional = side * holdings_price_per_share.diff().shift(-1)
    benchmark_return_in_bps = (
        1e4 * side * holdings_price_per_share.pct_change().shift(-1)
    )
    price_df = pd.concat(
        {
            "holdings_price_per_share": holdings_price_per_share,
            "trade_price_per_share": trade_price_per_share,
            "slippage_notional": slippage_notional,
            "slippage_notional_per_share": slippage_notional_per_share,
            "slippage_in_bps": slippage_in_bps,
            "benchmark_return_notional": benchmark_return_notional,
            "benchmark_return_in_bps": benchmark_return_in_bps,
            "is_buy": buy.astype(bool),
            "is_sell": sell.astype(bool),
            "is_benchmark_profitable": is_benchmark_profitable,
        },
        axis=1,
    )
    if join_output_with_input:
        price_df = pd.concat([df, price_df], axis=1)
    return price_df
