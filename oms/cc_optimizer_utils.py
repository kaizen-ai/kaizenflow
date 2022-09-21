"""
Import as:

import oms.cc_optimizer_utils as occoputi
"""

import logging

import pandas as pd

import helpers.hdbg as hdbg
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


# TODO(gp): @all add unit tests for these functions.
# TODO(gp): Pass the broker.minimal_order_limits instead of broker so testing is
# easier.
# TODO(gp): Compute the constraints on the df directly.
# TODO(gp): Dump this data before and after in log_dir.
def _apply_prod_limits(order: pd.Series, broker: ombroker.Broker) -> pd.Series:
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
    :return: updated order
    """
    hdbg.dassert_isinstance(order, pd.Series)
    _LOG.debug('Order before adjustments:\n%s', order)
    asset_id = order.name
    market_info = broker.market_info[asset_id]
    # 1) Ensure that the amount of shares is above the minimum required.
    min_amount = market_info["min_amount"]
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
    price = broker.get_low_market_price(asset_id)
    total_cost = price * abs(diff_num_shares)
    min_cost = market_info["min_cost"]
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
    amount_precision = market_info["amount_precision"]
    diff_num_shares = round(diff_num_shares, amount_precision)
    _LOG.info(
        "Rounding order amount to %s decimal points. Result: %s",
        amount_precision,
        diff_num_shares,
    )
    #
    order["diff_num_shares"] = diff_num_shares
    _LOG.debug('Order after adjustments:\n%s', order)
    return order


def _force_minimal_order(order: pd.Series, broker: ombroker.Broker) -> pd.Series:
    """
    Enforce that `order` verifies the minimum quantity set by the exchange for
    testnet account.

    Note that the constraints for testnet are more stringent than for the prod
    account.

    Same interface as `_apply_prod_limits()`.
    """
    _LOG.info("Order before adjustments: %s", order)
    hdbg.dassert_isinstance(order, pd.Series)
    asset_id = order.name
    market_info = broker.market_info[asset_id]
    #
    required_amount = market_info["min_amount"]
    min_cost = market_info["min_cost"]
    # Get the low price for the asset.
    low_price = broker.get_low_market_price(asset_id)
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
    forecast_df: pd.DataFrame, broker: ombroker.Broker
) -> pd.DataFrame:
    """
    Apply notional limits for DataFrame of multiple orders.

    :param forecast_df: DataFrame with forecasts
    :param broker: Broker class instance
    :return: DataFrame with updated orders
    """
    _LOG.info("Order df before adjustments: %s", forecast_df.to_string())
    # Add diff_num_shares to calculate notional limit.
    hdbg.dassert_is_subset(
        ["target_notional_trade", "price"], forecast_df.columns
    )
    forecast_df["diff_num_shares"] = (
        forecast_df["target_notional_trade"] / forecast_df["price"]
    )
    #
    stage = broker.stage
    if stage in ["preprod", "prod"]:
        forecast_df = forecast_df.apply(
            _apply_prod_limits, args=(broker,), axis=1
        )
    elif stage == "local":
        forecast_df = forecast_df.apply(
            _force_minimal_order, args=(broker,), axis=1
        )
    else:
        hdbg.dfatal(f"Unknown mode: {stage}")
    _LOG.info("Order df after adjustments: %s", forecast_df.to_string())
    return forecast_df
