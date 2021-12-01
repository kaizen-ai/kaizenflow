"""
Import as:

import oms.place_orders as oplaorde
"""

import copy
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import core.dataflow.price_interface as cdtfprint
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.htqdm as htqdm
import helpers.printing as hprint
import oms.order as omorder
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def mark_to_market(
    current_timestamp: pd.Timestamp,
    predictions: pd.Series,
    portfolio: omportfo.Portfolio,
) -> pd.DataFrame:
    """
    Return price, value of all assets held or for which we have a prediction.

    :return: dataframe with columns `asset_id`, `prediction`, `price`,
        curr_num_shares`, `value`.
    """
    _LOG.debug("# Mark portfolio to market at timestamp=%s", current_timestamp)
    # Get the current holdings.
    asset_id = None
    holdings = portfolio.get_holdings(
        current_timestamp, asset_id, exclude_cash=False
    )
    holdings.set_index("asset_id", drop=True, inplace=True)
    _LOG.debug("holdings=\n%s", hprint.dataframe_to_str(holdings))
    # Prepare `predictions` for the merge.
    predictions = pd.DataFrame(predictions)
    predictions.columns = ["prediction"]
    predictions.index.name = "asset_id"
    _LOG.debug("predictions=\n%s", hprint.dataframe_to_str(predictions))
    # Merge current holdings and predictions.
    merged_df = holdings.merge(
        predictions, left_index=True, right_index=True, how="outer"
    )
    merged_df["curr_num_shares"].fillna(0.0, inplace=True)
    # Move `asset_id` from the index to a column.
    merged_df.reset_index(inplace=True)
    # Do not allow `asset_id` to be represented as a float.
    merged_df["asset_id"] = merged_df["asset_id"].convert_dtypes(
        infer_objects=False,
        convert_string=False,
        convert_boolean=False,
        convert_floating=False,
    )
    _LOG.debug("after merge: merged_df=\n%s", hprint.dataframe_to_str(merged_df))
    # Mark to market.
    merged_df = portfolio.mark_to_market(current_timestamp, merged_df)
    _LOG.debug("merged_df=\n%s", hprint.dataframe_to_str(merged_df))
    columns = ["prediction", "price", "curr_num_shares"]
    hdbg.dassert_is_subset(columns, merged_df.columns)
    merged_df[columns] = merged_df[columns].fillna(0.0)
    return merged_df


def compute_trades(
    df: pd.DataFrame,
    cash_asset_id: int,
) -> pd.DataFrame:
    """
    Compute target trades from holdings (dollar-valued) and predictions.

    This is a stand-in for optimization.

    :param df: a dataframe with current positions and predictions
    :param cash_asset_id: id used to represent cash
    :return:
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


def generate_orders(
    shares: pd.Series,
    order_config: Dict[str, Any],
    initial_order_id: int = 0,
) -> List[omorder.Order]:
    """
    Turns a series of shares to trade into a list of orders.

    :param shares: number of shares to trade, indexed by asset_id
    :param order_config: common parameters used to initialize `Order`
    :param initial_order_id: the starting point for enumerating orders
    :return: a list of nontrivial orders (i.e., no zero-share orders)
    """
    _LOG.debug("# Place orders")
    orders: List[omorder.Order] = []
    order_id = initial_order_id
    for asset_id, shares in shares.iteritems():
        if shares == 0.0:
            # No need to place trades.
            continue
        order = omorder.Order(
            **order_config.to_dict(),
            order_id=order_id,
            asset_id=asset_id,
            num_shares=shares,
        )
        order_id += 1
        _LOG.debug("order=%s", order)
        orders.append(order)
    return orders


def optimize_and_update(
    current_timestamp: pd.Timestamp,
    predictions: pd.Series,
    portfolio: omportfo.Portfolio,
    order_config: Dict[str, Any],
    initial_order_id: int = 0,
) -> int:
    """
    Compute target holdings, generate orders, and update the portfolio.

    :param current_timestamp: timestamp used for valuing holdings, timestamping
        orders
    :param predictions: predictions indexed by `asset_id`
    :param portfolio: portfolio with current holdings
    :param order_config: config for `Order`
    :param initial_order_id: first `order_id` to use
    :return: number of orders generated (and filled)
    """
    hdbg.dassert(np.isfinite(predictions).all(), "predictions=%s", predictions)
    priced_holdings = mark_to_market(current_timestamp, predictions, portfolio)
    df = compute_trades(priced_holdings, portfolio.CASH_ID)
    # Round to nearest integer towards zero.
    # df["diff_num_shares"] = np.fix(df["target_trade"] / df["price"])
    df["diff_num_shares"] = df["target_trade"] / df["price"]
    orders = generate_orders(
        df["diff_num_shares"], order_config, initial_order_id
    )
    next_timestamp = order_config["end_timestamp"]
    portfolio.process_filled_orders(current_timestamp, next_timestamp, orders)
    return len(orders)


# TODO(gp): We should use a RT graph executed once step at a time. For now we just
#  play back the entire thing.
# TODO(gp): -> update_holdings
def place_orders(
    predictions_df: pd.DataFrame,
    execution_mode: str,
    config: Dict[str, Any],
    # TODO(gp): -> initial_order_id
    order_id: int = 0,
) -> omportfo.Portfolio:
    """
    Place orders corresponding to the predictions stored in the given df.

    Orders will be realized over the span of two intervals of time (i.e., two lags).

    - The PnL is realized two intervals of time after the corresponding prediction
    - The columns reported in the df are for the beginning of the interval of time
    - The columns ending with `+1` represent what happens in the next interval
      of time

    :param predictions_df: a dataframe indexed by timestamps with one column for the
        predictions for each asset
    :param execution_mode:
        - `batch`: place the trades for all the predictions (used in historical
           mode)
        - `real_time`: place the trades only for the last prediction as in a
           real-time set-up
    :param config:
        - `price_interface`: the interface to get price data
        - `pred_column`: the column in the df from the DAG containing the predictions
           for all the assets
        - `mark_column`: the column from the PriceInterface to mark holdings to
          market
        - `portfolio`: object used to store positions
        - `locates`: object used to access short locates
    :return: updated portfolio
    """
    # Check the config.
    price_interface = config["price_interface"]
    hdbg.dassert_issubclass(price_interface, cdtfprint.AbstractPriceInterface)
    portfolio = copy.copy(config["portfolio"])
    hdbg.dassert_issubclass(portfolio, omportfo.Portfolio)
    # Check predictions.
    hdbg.dassert_isinstance(predictions_df, pd.DataFrame)
    hpandas.dassert_index_is_datetime(predictions_df)
    hpandas.dassert_strictly_increasing_index(predictions_df)
    if execution_mode == "real_time":
        predictions_df = predictions_df.tail(1)
    _LOG.debug("predictions_df=%s\n%s", str(predictions_df.shape), predictions_df)
    _LOG.debug("predictions_df.index=%s", str(predictions_df.index))
    # Cache some variables used many times.
    offset_5min = pd.DateOffset(minutes=5)
    order_type = config["order_type"]
    #
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    num_rows = len(predictions_df)
    iter_ = enumerate(predictions_df.iterrows())
    for idx, (timestamp, predictions) in tqdm(
        iter_, total=num_rows, file=tqdm_out
    ):
        _LOG.debug("\n%s", hprint.frame("# timestamp=%s" % timestamp))
        _LOG.debug("portfolio=\n%s", portfolio)
        _LOG.debug("predictions=\n%s", predictions)
        hdbg.dassert(
            np.isfinite(predictions).all(), "predictions=%s", predictions
        )
        if idx == len(predictions_df) - 1:
            # For the last timestamp we only need to mark to market, but not post
            # any more orders.
            continue
        # Use current price to convert forecasts in position intents.
        _LOG.debug("# Compute trade allocation")
        # Enter position between now and the next 5 mins.
        timestamp_start = timestamp
        timestamp_end = timestamp + offset_5min
        #
        _LOG.debug("# Place orders")
        order_dict_ = {
            "price_interface": price_interface,
            "type_": order_type,
            "creation_timestamp": timestamp,
            "start_timestamp": timestamp_start,
            "end_timestamp": timestamp_end,
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        num_orders_filled = optimize_and_update(
            timestamp, predictions, portfolio, order_config, order_id
        )
        order_id += num_orders_filled
    return portfolio
