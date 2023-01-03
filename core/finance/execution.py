"""
Import as:

import core.finance.execution as cfinexec
"""

import logging

import numpy as np
import pandas as pd

import core.finance.resampling as cfinresa
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


# TODO(gp): -> private
def generate_limit_order_price(
    df: pd.DataFrame,
    bid_col: str,
    ask_col: str,
    buy_reference_price_col: str,
    sell_reference_price_col: str,
    buy_spread_frac_offset: float,
    sell_spread_frac_offset: float,
    subsample_freq: str,
    ffill_limit: int,
) -> pd.DataFrame:
    """
    Generate limit order prices from sub-sampled reference prices and an
    offset.

    E.g., place order 5% from the bid or ask level, repricing every 2 minutes.

    :param df: datetime-indexed dataframe with a reference price column
    :param bid_col: name of column with last bid price for bar
    :param ask_col: like `bid_col` but for ask
    :param buy_reference_price_col: name of df column with reference prices
        for submitting buy limit orders
    :param sell_reference_price_col: like `buy_reference_price_col` but for
        submitting sell limit orders
    :param buy_spread_frac_offset: fraction of quoted spread to add to the
        buy reference price to determine the limit price
    :param sell_spread_frac_offset: fraction of quoted spread to add to the
        sell reference price to determine the limit price
    :param subsample_freq: resampling frequency to use for subsampling, e.g.,
        "15T" for a dataframe of one-minute bars
    :param ffill_limit: number of ffill bars to propagate limit orders, e.g.,
        if we use one-minute bars, take `subsample_freq="15T"`, and let
        `ffill_limit=4`, then the execution window will be 1 + 4 = 5 minutes.
    :return: a dataframe of limit order prices named `{buy,sell}_limit_order_price`
    """
    hpandas.dassert_time_indexed_df(
        df, allow_empty=True, strictly_increasing=True
    )
    required_cols = [
        bid_col,
        ask_col,
        buy_reference_price_col,
        sell_reference_price_col,
    ]
    hdbg.dassert_is_subset(required_cols, df.columns)
    hdbg.dassert_isinstance(buy_spread_frac_offset, float)
    hdbg.dassert_isinstance(sell_spread_frac_offset, float)
    hdbg.dassert_isinstance(subsample_freq, str)
    hdbg.dassert_isinstance(ffill_limit, int)
    # Subsample the reference price col, e.g., take samples every "15T" on a
    # "1T" series.
    buy_subsampled = cfinresa.resample(
        df[buy_reference_price_col], rule=subsample_freq
    ).last()
    sell_subsampled = cfinresa.resample(
        df[sell_reference_price_col], rule=subsample_freq
    ).last()
    quoted_spread = df[ask_col] - df[bid_col]
    # Apply a dollar offset to the subsampled prices.
    buy_subsampled += buy_spread_frac_offset * quoted_spread
    sell_subsampled += sell_spread_frac_offset * quoted_spread
    # TODO(Paul): Add an option to round to a different tick size.
    buy_subsampled = buy_subsampled.round(2).rename("buy_limit_order_price")
    sell_subsampled = sell_subsampled.round(2).rename("sell_limit_order_price")
    subsampled = pd.concat([buy_subsampled, sell_subsampled], axis=1)
    # Reindex to the original frequency (imputing NaNs).
    subsampled = subsampled.reindex(index=df.index)
    # Forward fill the limit prices `ffill_limit` steps.
    subsampled = subsampled.ffill(limit=ffill_limit)
    return subsampled


# TODO(gp): -> private
def estimate_limit_order_execution(
    df: pd.DataFrame,
    bid_col: str,
    ask_col: str,
    buy_limit_price_col: str,
    sell_limit_price_col: str,
) -> pd.Series:
    """
    Estimate passive fills.

    :param df: datetime-indexed dataframe with price data
    :param bid_col: name of column with last bid price for bar
    :param ask_col: like `bid_col` but for ask
    :param buy_limit_price_col: name of column with limit order price; NaN
        represents no order.
    :param sell_limit_price_col: like `buy_limit_price_col` but for selling
    :return: dataframe with two bool columns, `limit_buy_executed` and
        `limit_sell_executed`
    """
    hpandas.dassert_time_indexed_df(
        df, allow_empty=True, strictly_increasing=True
    )
    hdbg.dassert_isinstance(bid_col, str)
    hdbg.dassert_isinstance(ask_col, str)
    # Ensure that all required columns are in the dataframe.
    cols = [bid_col, ask_col, buy_limit_price_col, sell_limit_price_col]
    hdbg.dassert_is_subset(cols, df.columns)
    # TODO(Paul): Add a switch to allow equality.
    # Delay by one bar the limit price columns. The columns are indexed by
    # knowledge time (the limit price to use is known at end-of-bar), and so
    # earliest execution takes place in the next bar.
    buy_limit_executed = (df[ask_col] < df[buy_limit_price_col].shift(1)).rename(
        "limit_buy_executed"
    )
    sell_limit_executed = (
        df[bid_col] > df[sell_limit_price_col].shift(1)
    ).rename("limit_sell_executed")
    limit_executed = pd.concat([buy_limit_executed, sell_limit_executed], axis=1)
    return limit_executed


# TODO(gp): -> private
def generate_limit_orders_and_estimate_execution(
    df: pd.DataFrame,
    bid_col: str,
    ask_col: str,
    buy_reference_price_col: str,
    sell_reference_price_col: str,
    buy_spread_frac_offset: float,
    sell_spread_frac_offset: float,
    subsample_freq: str,
    ffill_limit: int,
) -> pd.DataFrame:
    """
    Generate limit buy/sells and estimate execution.

    :param df: datetime-indexed dataframe with price data
    :param bid_col: bid col for estimating buy limit order execution
    :param ask_col: ask col for estimating sell limit order execution
    :param buy_reference_price_col: reference price col for buy limit orders
    :param buy_spread_frac_offset: amount to add to buy reference price
    :param sell_reference_price_col: reference price col for sell limit orders
    :param sell_spread_frac_offset: amount to add to sell reference price
    :param subsample_freq: as in `generate_limit_order_price()`
    :param ffill_limit: as in `generate_limit_order_price()`
    :return: dataframe with limit order price and execution cols
    """
    limit_order_prices = generate_limit_order_price(
        df,
        bid_col,
        ask_col,
        buy_reference_price_col,
        sell_reference_price_col,
        buy_spread_frac_offset,
        sell_spread_frac_offset,
        subsample_freq,
        ffill_limit,
    )
    executions = estimate_limit_order_execution(
        pd.concat([df, limit_order_prices], axis=1),
        bid_col,
        ask_col,
        "buy_limit_order_price",
        "sell_limit_order_price",
    )
    buy_trades = limit_order_prices["buy_limit_order_price"].loc[
        executions["limit_buy_executed"]
    ]
    sell_trades = limit_order_prices["sell_limit_order_price"].loc[
        executions["limit_sell_executed"]
    ]
    buy_trades = buy_trades.reindex(index=df.index).rename("buy_trade_price")
    sell_trades = sell_trades.reindex(index=df.index).rename("sell_trade_price")
    execution_df = pd.concat(
        [
            limit_order_prices,
            executions,
            buy_trades,
            sell_trades,
        ],
        axis=1,
    )
    return execution_df


def apply_execution_prices_to_trades(
    trade: pd.DataFrame,
    buy_price: pd.DataFrame,
    sell_price: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply buy/sell prices to trades.
    """
    # Sanity-check inputs.
    hpandas.dassert_time_indexed_df(
        trade, allow_empty=False, strictly_increasing=True
    )
    hpandas.dassert_time_indexed_df(
        buy_price, allow_empty=False, strictly_increasing=True
    )
    hpandas.dassert_time_indexed_df(
        sell_price, allow_empty=False, strictly_increasing=True
    )
    hpandas.dassert_axes_equal(buy_price, trade)
    hpandas.dassert_axes_equal(sell_price, trade)
    # Ensure all buys may be filled.
    use_buy_price = trade > 0
    no_buy_price_available = buy_price.isna()
    # TODO(Paul): Factor out trimming a boolean df to rows and cols with
    # `True`.
    no_buy_fills = use_buy_price & no_buy_price_available
    no_buy_fills = no_buy_fills.loc[no_buy_fills.any(axis=1)]
    hdbg.dassert(not no_buy_fills.any().any(), msg=str(no_buy_fills))
    use_buy_price = use_buy_price.replace(True, 1.0).replace(False, np.nan)
    # Ensure all sells may be filled.
    use_sell_price = trade < 0
    no_sell_price_available = sell_price.isna()
    no_sell_fills = use_sell_price & no_sell_price_available
    no_sell_fills = no_sell_fills.loc[no_sell_fills.any(axis=1)]
    hdbg.dassert(not no_sell_fills.any().any(), msg=str(no_sell_fills))
    use_sell_price = use_sell_price.replace(True, 1.0).replace(False, np.nan)
    # Splice buy and sell prices.
    execution_price = buy_price.multiply(use_buy_price).add(
        sell_price.multiply(use_sell_price), fill_value=0.0
    )
    return execution_price
