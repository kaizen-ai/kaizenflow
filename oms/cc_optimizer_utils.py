"""
Import as:

import oms.cc_optimizer_utils as occoputi
"""

import glob
import logging
import os
from typing import Any, Dict, Tuple

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import oms.broker as ombroker

_LOG = logging.getLogger(__name__)


def _apply_cc_limits(
    order: pd.Series, asset_market_info: Dict[str, Any], stage: str, round_mode: str
) -> pd.Series:
    hdbg.dassert_isinstance(order, pd.Series)
    _LOG.debug("Order before adjustments: %s", order)
    #
    min_amount = asset_market_info["min_amount"]
    price = order["price"]
    min_cost = asset_market_info["min_cost"]
    final_order_amount = order["target_trades_shares"]
    #
    if stage == "local":
        # Force minimum order amount for testnet.
        min_amount = min_cost * 3 / price
        # Apply back the sign.
        if final_order_amount < 0:
            final_order_amount = -min_amount
    elif stage in ["preprod", "prod"]:
        # 1) Set the target number of shares to 0 if the order's number of
        # shares is below the minimum required.
        if abs(final_order_amount) < min_amount:
            _LOG.warning(
                "Order: %s\nAmount of asset in order = %s is below minimal value = %s. "
                + "Setting the target number of shares to 0.",
                str(order),
                abs(final_order_amount),
                min_amount,
            )
            final_order_amount = 0.0
        # 2) Set the target number of shares to 0 if the order's notional value
        # is below the minimal cost.
        # We estimate the total value of the order using the order's `price`.
        total_cost = price * abs(order["target_trades_shares"])
        if total_cost <= min_cost:
            _LOG.warning(
                "Order: %s\nNotional value of asset in order = %s is below minimal value = %s. "
                + "Setting the target number of shares to 0.",
                str(order),
                total_cost,
                min_cost,
            )
            final_order_amount = 0.0
    else:
        raise ValueError(f"Unsupported stage={stage}")
    # No need to round Nones or zeros.
    if final_order_amount and final_order_amount != 0:
        hdbg.dassert_isinstance(final_order_amount, float)
        # 3) Round the order amount in accordance with exchange rules.
        amount_precision = asset_market_info["amount_precision"]
        rounded_order_amount = round(final_order_amount, amount_precision)
        if round_mode == "round":
            # Round the number.
            final_order_amount = rounded_order_amount
            _LOG.debug(
                "Rounding order amount to %s decimal points. Result: %s",
                amount_precision,
                final_order_amount,
            )
        elif round_mode == "check":
            # Check that the number of digits is the correct one according
            # to exchange rules. 
            hdbg.dassert_eq(final_order_amount, rounded_order_amount)
        else:
            raise ValueError(f"Unsupported round_mode={round_mode}")
    #
    order["target_trades_shares"] = final_order_amount
    _LOG.debug("Order after adjustments: %s", order)
    return order


def apply_cc_limits(
    forecast_df: pd.DataFrame, broker: ombroker.Broker, round_mode: str
) -> pd.DataFrame:
    """
    Apply notional limits for DataFrame of multiple orders.

    Target amount of order shares is set to 0 if its actual values are below
    the notional limits.

    :param forecast_df: DataFrame with forecasts, e.g.
        ```
                    curr_num_shares      price   position      wall_clock_timestamp  prediction  volatility  spread  target_position  target_notional_trade  diff_num_shares
        asset_id
        6051632686         2.524753   5.040333  12.725596 2022-09-15 10:35:11-04:00    0.475591    0.004876       0        12.018895              -0.706701        -0.140209
        8717633868              0.0      18.77        0.0 2022-09-15 10:35:11-04:00   -0.134599     0.00312       0       -29.341767             -29.341767        -1.563227
        2540896331              0.0  12.958333        0.0 2022-09-15 10:35:11-04:00    0.103423    0.002859       0         0.000000                    0.0              0.0
        ```
    :param broker: Broker class instance
    :param round_mode: shares roundning mode
        "round": round the amount of shares according to the exchange rules
        "check": check with an assertion whether target shares are rounded or not
    :return: DataFrame with updated orders
    """
    _LOG.debug(
        "Order df before adjustments: forecast_df=\n%s",
        hpandas.df_to_str(forecast_df, num_rows=None),
    )
    # Add diff_num_shares to calculate notional limit.
    hdbg.dassert_is_subset(
        ["target_trades_notional", "price"], forecast_df.columns
    )
    forecast_df["target_trades_shares"] = (
        forecast_df["target_trades_notional"] / forecast_df["price"]
    )
    #
    stage = broker.stage
    hdbg.dassert_in(stage, ["local", "prod", "preprod"])
    market_info = broker.market_info
    #
    # Save shares before limits application.
    forecast_df["target_trades_shares.before_apply_cc_limits"] = forecast_df[
        "target_trades_shares"
    ]
    forecast_df_tmp = []
    # Apply exchange restrictions to individual orders.
    for idx, row in forecast_df.iterrows():
        row_tmp = _apply_cc_limits(row, market_info[idx], stage, round_mode)
        forecast_df_tmp.append(row_tmp)
    # Combine orders into one dataframe.
    forecast_df_tmp = pd.concat(forecast_df_tmp, axis=1).T
    forecast_df_tmp.index.name = forecast_df.index.name
    hdbg.dassert_eq(str(forecast_df.shape), str(forecast_df_tmp.shape))
    forecast_df = forecast_df_tmp
    _LOG.debug(
        "Order df after adjustments: forecast_df=\n%s",
        hpandas.df_to_str(forecast_df, num_rows=None),
    )
    return forecast_df


# TODO(Grisha): should we remove since we do not the logging anymore?
def read_apply_cc_limits_logs(
    log_dir: str,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Read logs for logs on application of cc limits.

    The function reads orders before and after application of
    constraints, each type combined into a separate dataframe.
    """
    # Get the file names.
    log_pattern = os.path.join(log_dir, "forecast_df*.csv")
    file_names = glob.glob(log_pattern)
    # Read orders before exchange constraints.
    file_names_before = [f for f in file_names if "before_apply_cc_limits" in f]
    forecast_df_before = []
    for file_name in file_names_before:
        df_tmp = pd.read_csv(file_name)
        forecast_df_before.append(df_tmp)
    forecast_df_before = pd.concat(forecast_df_before)
    # Read orders after exchange constraints.
    file_names_after = [f for f in file_names if "after_apply_cc_limits" in f]
    forecast_df_after = []
    for file_name in file_names_after:
        df_tmp = pd.read_csv(file_name)
        forecast_df_after.append(df_tmp)
    forecast_df_after = pd.concat(forecast_df_after)
    return forecast_df_before, forecast_df_after
