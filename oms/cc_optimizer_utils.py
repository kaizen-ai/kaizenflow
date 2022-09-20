"""
Import as:

import oms.cc_optimizer_utils as occoputi
"""

import logging
from typing import Any, Dict

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


# TODO(gp): @all add unit tests for these functions
def _apply_prod_limits(
    order: pd.Series, asset_market_info: Dict[int, Any]
) -> pd.Series:
    """
    Enforce that `order` verifies the minimum quantity set by the exchange for
    prod account.

    The function checks that:
    1) the order quantity of the asset is above the minimum required
    2) the total cost of the asset is above the minimum required.
    If either of these conditions is not verified, the order is changed to be above
    the required minimal amount.

    :param order: order to be submitted represented as a row from the forecast
        DataFrame
    :param asset_market_info: market info for the particular asset
    :return: updated order
    """
    hdbg.dassert_isinstance(order, pd.Series)
    _LOG.info("Order before adjustments: %s", order)
    order.name
    # 1) Ensure that the amount of shares is above the minimum required.
    min_amount = asset_market_info["min_amount"]
    diff_num_shares = order["diff_num_shares"]
    if abs(order["diff_num_shares"]) < min_amount:
        if diff_num_shares < 0:
            min_amount = -min_amount
        _LOG.warning(
            "Order: %s\nAmount of asset in order is below minimal value: %s. "
            + "Setting to min amount: %s",
            str(order),
            diff_num_shares,
            min_amount,
        )
        diff_num_shares = min_amount
    # 2) Ensure that the order value is above the minimal cost.
    # Estimate the total value of the order. We use the low price since this is a
    # more conservative estimate of the order value.
    price = order["price"]
    total_cost = price * abs(diff_num_shares)
    min_cost = asset_market_info["min_cost"]
    if total_cost <= min_cost:
        # Set amount based on minimal notional price.
        min_amount = min_cost * 3 / price
        if diff_num_shares < 0:
            min_amount = -min_amount
        _LOG.warning(
            "Order: %s\nAmount of asset in order is below minimal value: %s. "
            + "Setting to following amount based on notional limit: %s",
            str(order),
            diff_num_shares,
            min_amount,
        )
        # Update the number of shares.
        diff_num_shares = min_amount
    # Round the order amount in accordance with exchange rules.
    amount_precision = asset_market_info["amount_precision"]
    diff_num_shares = round(diff_num_shares, amount_precision)
    _LOG.info(
        "Rounding order amount to %s decimal points. Result: %s",
        amount_precision,
        diff_num_shares,
    )
    #
    order["diff_num_shares"] = diff_num_shares
    _LOG.info("Order after adjustments: %s", order)
    return order


def _force_minimal_order(
    order: pd.Series, market_info: Dict[int, Any]
) -> pd.Series:
    """
    Enforce that `order` verifies the minimum quantity set by the exchange for
    testnet account.

    Note that the constraints for testnet are more stringent than for the prod
    account.

    Same interface as `_apply_prod_limits()`.
    """
    _LOG.info("Order before adjustments: %s", order)
    hdbg.dassert_isinstance(order, pd.Series)
    order.name
    #
    required_amount = market_info["min_amount"]
    min_cost = market_info["min_cost"]
    # Get the low price for the asset.
    low_price = order["price"]
    # Verify that the estimated total cost is above 10.
    if low_price * required_amount <= min_cost:
        # Set the amount of asset to above min cost.
        #  Note: the multiplication by 2 is done to give some
        #  buffer so the order does not go below
        #  the minimal amount of asset.
        required_amount = (min_cost / low_price) * 2
    # Round the order amount in accordance with exchange rules.
    amount_precision = market_info["amount_precision"]
    required_amount = round(required_amount, amount_precision)
    _LOG.info(
        "Rounding order amount to %s decimal points. Result: %s",
        amount_precision,
        required_amount,
    )
    # Apply back the sign.
    if order["diff_num_shares"] < 0:
        order["diff_num_shares"] = -required_amount
    else:
        order["diff_num_shares"] = required_amount
    _LOG.info("Order after adjustments: %s", order)
    return order


def apply_cc_limits(
    forecast_df: pd.DataFrame, broker: ombroker.Broker, log_dir: str
) -> pd.DataFrame:
    """
    Apply notional limits for DataFrame of multiple orders.

    :param forecast_df: DataFrame with forecasts, e.g.
        ```
                    curr_num_shares      price   position      wall_clock_timestamp  prediction  volatility  spread  target_position  target_notional_trade  diff_num_shares
        asset_id
        6051632686         2.524753   5.040333  12.725596 2022-09-15 10:35:11-04:00    0.475591    0.004876       0        12.018895              -0.706701        -0.140209
        8717633868              0.0      18.77        0.0 2022-09-15 10:35:11-04:00   -0.134599     0.00312       0       -29.341767             -29.341767        -1.563227
        2540896331              0.0  12.958333        0.0 2022-09-15 10:35:11-04:00    0.103423    0.002859       0         0.000000                    0.0              0.0
        ```
    :param broker: Broker class instance
    :param log_dir: directory to store order transformations
    :return: DataFrame with updated orders
    """
    _LOG.info(
        "Order df before adjustments: forecast_df=%s",
        hpandas.df_to_str(forecast_df, num_rows=None),
    )
    log_timestamp = forecast_df.loc[0, "wall_clock_timestamp"]
    forecast_df.to_csv(
        log_dir + f"forecast_df_before_constraints{log_timestamp}.csv"
    )
    # Add diff_num_shares to calculate notional limit.
    hdbg.dassert_is_subset(
        ["target_notional_trade", "price"], forecast_df.columns
    )
    forecast_df["diff_num_shares"] = (
        forecast_df["target_notional_trade"] / forecast_df["price"]
    )
    #
    stage = broker.stage
    market_info = broker.market_info
    forecast_df_tmp = []
    for idx, row in forecast_df.iterrows():
        if stage in ["preprod", "prod"]:
            row_tmp = _apply_prod_limits(row, market_info[idx])
        elif stage == "local":
            row_tmp = _force_minimal_order(row, market_info[idx])
        else:
            raise ValueError(f"Unknown stage='{stage}'")
        forecast_df_tmp.append(row_tmp)
    forecast_df_tmp = pd.concat(forecast_df_tmp, axis=1).T
    hdbg.dassert_eq(str(forecast_df.shape), str(forecast_df_tmp.shape))
    forecast_df = forecast_df_tmp
    _LOG.info(
        "Order df after adjustments: forecast_df=%s",
        hpandas.df_to_str(forecast_df, num_rows=None),
    )
    forecast_df.to_csv(
        log_dir + f"forecast_df_after_constraints{log_timestamp}.csv"
    )
    return forecast_df
