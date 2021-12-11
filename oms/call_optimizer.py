"""
Import as:

import oms.call_optimizer as ocalopti
"""

import logging

import numpy as np
import pandas as pd

import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_target_positions_in_cash(
    df: pd.DataFrame,
    cash_asset_id: int,
) -> pd.DataFrame:
    """
    Compute target trades from holdings (dollar-valued) and predictions.

    This is a stand-in for optimization. This function does not have access to
    prices and so does not perform any conversions to or from shares. It also
    needs to be told the id associated with cash.

    :param df: a dataframe with current positions (in dollars) and predictions
    :param cash_asset_id: id used to represent cash
    :return: a dataframe with target positions and trades
        (denominated in dollars)
    """
    hdbg.dassert_isinstance(df, pd.DataFrame)
    hdbg.dassert_is_subset(["asset_id", "prediction", "value"], df.columns)
    # TODO(*): Check uniqueness of `asset_id` column.
    hdbg.dassert_not_in("target_position", df.columns)
    hdbg.dassert_not_in("target_trade", df.columns)
    df = df.set_index("asset_id")
    # In this placeholder, we maintain two invariants (approximately):
    #   1. Net wealth is conserved from one step to the next.
    #   2. Leverage is conserved from one step to the next.
    # The second invariant may be restated as conserving gross exposure.
    # TODO(Paul): Make this configurable.
    TARGET_LEVERAGE = 0.1
    _LOG.debug("TARGET_LEVERAGE=%s", TARGET_LEVERAGE)
    predictions = df["prediction"]
    predictions_l1 = predictions.abs().sum()
    _LOG.debug("predictions_l1 =%s", predictions_l1)
    hdbg.dassert(np.isfinite(predictions_l1), "scale_factor=%s", predictions_l1)
    hdbg.dassert_lt(0, predictions_l1)
    # These positions are expressed in dollars.
    current_positions = df["value"]
    net_wealth = current_positions.sum()
    _LOG.debug("net_wealth=%s", net_wealth)
    # Drop cash.
    df.drop(index=cash_asset_id, inplace=True)
    # NOTE: Some of these checks are now redundant.
    hdbg.dassert(np.isfinite(net_wealth), "wealth=%s", net_wealth)
    scale_factor = net_wealth * TARGET_LEVERAGE / predictions_l1
    _LOG.debug("scale_factor=%s", scale_factor)
    target_positions = scale_factor * predictions
    target_positions[cash_asset_id] = current_positions[cash_asset_id]
    target_trades = target_positions - current_positions
    df["target_position"] = target_positions
    df["target_trade"] = target_trades
    return df
