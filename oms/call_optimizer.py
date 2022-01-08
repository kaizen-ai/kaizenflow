"""
Import as:

import oms.call_optimizer as ocalopti
"""

import logging

import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg

_LOG = logging.getLogger(__name__)


def compute_target_positions_in_cash(
    df: pd.DataFrame,
    cash_asset_id: int,
    # config: cconfig.Config,
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
    hdbg.dassert_is_subset(
        ["asset_id", "prediction", "volatility", "value"], df.columns
    )
    hdbg.dassert_not_in("target_position", df.columns)
    hdbg.dassert_not_in("target_trade", df.columns)
    #
    hdbg.dassert(not df["prediction"].isna().any())
    hdbg.dassert(not df["volatility"].isna().any())
    hdbg.dassert(not df["value"].isna().any())
    #
    df = df.set_index("asset_id")
    hdbg.dassert(not df.index.has_duplicates)
    # TODO(Paul): Pass this through a config.
    # target_gmv = config["target_gmv"]
    target_gmv = 100000
    # In this placeholder, we maintain two invariants (approximately):
    #   1. Net wealth is conserved from one step to the next.
    #   2. GMV is conserved from one step to the next.
    # The second invariant may be restated as conserving gross exposure.
    predictions = df["prediction"]
    _LOG.debug("predictions=\n%s", predictions)
    volatility = df["volatility"]
    _LOG.debug("volatility=\n%s", volatility)
    volatility[cash_asset_id] = 1.0
    volatility = volatility.clip(lower=1e-5)
    #
    unscaled_target_positions = predictions.divide(volatility)
    unscaled_target_positions_l1 = unscaled_target_positions.abs().sum()
    _LOG.debug("unscaled_target_positions_l1 =%s", unscaled_target_positions_l1)
    hdbg.dassert_lt(0, unscaled_target_positions_l1)
    scale_factor = target_gmv / unscaled_target_positions_l1
    _LOG.debug("scale_factor=%s", scale_factor)
    # These positions are expressed in dollars.
    current_positions = df["value"]
    net_wealth = current_positions.sum()
    _LOG.debug("net_wealth=%s", net_wealth)
    # Drop cash.
    df.drop(index=cash_asset_id, inplace=True)
    target_positions = scale_factor * unscaled_target_positions
    target_positions[cash_asset_id] = current_positions[cash_asset_id]
    target_trades = target_positions - current_positions
    df["target_position"] = target_positions
    df["target_trade"] = target_trades
    return df
