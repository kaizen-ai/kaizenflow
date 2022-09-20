"""
Import as:

import oms.cc_optimizer_utils as occoputi
"""

import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hpandas as hpandas
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)

# TODO(gp): @all add unit tests for these functions
# TODO(gp): Pass the broker.minimal_order_limits instead of broker so testing is
#  easier.
# TODO(gp): Compute the constraints on the df directly.
# TODO(gp): Dump this data before and after in log_dir
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
    asset_limits = broker.minimal_order_limits[asset_id]
    # 1) Ensure that the amount of shares is above the minimum required.
    min_amount = asset_limits["min_amount"]
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
    #low_price = broker.get_low_market_price(asset_id)
    price = order["price"]
    total_cost = price * abs(diff_num_shares)
    min_cost = asset_limits["min_cost"]
    if total_cost <= min_cost:
        # Set amount based on minimal notional price.
        min_amount = round(min_cost * 3 / price, 2)
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
    hdbg.dassert_isinstance(order, pd.Series)
    asset_id = order.name
    asset_limits = broker.minimal_order_limits[asset_id]
    #
    required_amount = asset_limits["min_amount"]
    min_cost = asset_limits["min_cost"]
    # Get the low price for the asset.
    low_price = broker.get_low_market_price(asset_id)
    # Verify that the estimated total cost is above 10.
    if low_price * required_amount <= min_cost:
        # Set the amount of asset to above min cost.
        #  Note: the multiplication by 2 is done to give some
        #  buffer so the order does not go below
        #  the minimal amount of asset.
        required_amount = (min_cost / low_price) * 2
    # Apply back the sign.
    if order["diff_num_shares"] < 0:
        order["diff_num_shares"] = -required_amount
    else:
        order["diff_num_shares"] = required_amount
    return order


def apply_cc_limits(
    forecast_df: pd.DataFrame, broker: ombroker.Broker
) -> pd.DataFrame:
    """
    Apply notional limits for DataFrame of multiple orders.

    :param forecast_df: DataFrame with forecasts
        ```
                    curr_num_shares      price   position      wall_clock_timestamp  prediction  volatility  spread  target_position  target_notional_trade  diff_num_shares
        asset_id
        6051632686         2.524753   5.040333  12.725596 2022-09-15 10:35:11-04:00    0.475591    0.004876       0        12.018895              -0.706701        -0.140209
        8717633868              0.0      18.77        0.0 2022-09-15 10:35:11-04:00   -0.134599     0.00312       0       -29.341767             -29.341767        -1.563227
        2540896331              0.0  12.958333        0.0 2022-09-15 10:35:11-04:00    0.103423    0.002859       0         0.000000                    0.0              0.0
        ```
    :param broker: Broker class instance
    :return: DataFrame with updated orders
    """
    # Add diff_num_shares to calculate notional limit.
    hdbg.dassert_is_subset(
        ["target_notional_trade", "price"], forecast_df.columns
    )
    forecast_df["diff_num_shares"] = (
        forecast_df["target_notional_trade"] / forecast_df["price"]
    )
    #
    _LOG.info("before forecast_df=\n%s", hpandas.df_to_str(forecast_df, num_rows=None))
    stage = broker.stage
    forecast_df_tmp = []
    for idx, row in forecast_df.iterrows():
        if stage in ["preprod", "prod"]:
            row_tmp = _apply_prod_limits(row, broker)
        elif stage == "local":
            row_tmp = _force_minimal_order(row, broker)
        else:
            raise ValueError(f"Unknown stage='{stage}'")
        forecast_df_tmp.append(row_tmp)
    forecast_df_tmp = pd.concat(forecast_df_tmp, axis=1).T
    hdbg.dassert_eq(str(forecast_df.shape), str(forecast_df_tmp.shape))
    forecast_df = forecast_df_tmp
    _LOG.info("after forecast_df=\n%s", hpandas.df_to_str(forecast_df, num_rows=None))
    return forecast_df
