"""
Import as:

import oms.place_orders as oplaorde
"""

import collections
import copy
import datetime
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
    _LOG.debug(
        "Number of non-NaN predictions=`%i` at timestamp=`%s`",
        predictions.count(),
        current_timestamp,
    )
    _LOG.debug(
        "Number of NaN predictions=`%i` at timestamp=`%s`",
        predictions.isna().sum(),
        current_timestamp,
    )
    predictions = pd.DataFrame(predictions)
    predictions.columns = ["prediction"]
    predictions.index.name = "asset_id"
    _LOG.debug("predictions=\n%s", hprint.dataframe_to_str(predictions))
    # Merge current holdings and predictions.
    merged_df = holdings.merge(
        predictions, left_index=True, right_index=True, how="outer"
    )
    _LOG.debug(
        "Number of NaNs in `curr_num_shares` post-merge is `%i` at timestamp=`%s`",
        merged_df["curr_num_shares"].isna().sum(),
        current_timestamp,
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
    columns = ["prediction", "price", "curr_num_shares", "value"]
    hdbg.dassert_is_subset(columns, merged_df.columns)
    _LOG.debug(
        "Number of NaN prices=`%i` at timestamp=`%s`",
        merged_df["price"].isna().sum(),
        current_timestamp,
    )
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


def simulate_order_fills(orders: List[omorder.Order]) -> pd.DataFrame:
    """
    Create a dataframe of filled orders from a list of orders.

    :param orders:
    :return:
    """
    hdbg.dassert(orders)
    orders_rows = []
    for order in orders:
        _LOG.debug("# Processing order=%s", order)
        orders_row: Dict[str, Any] = collections.OrderedDict()
        # Copy content of the order.
        orders_row.update(order.to_dict())
        # Complete fills.
        orders_row["num_shares_filled"] = order.num_shares
        # Account for the executed price of the order.
        execution_price = order.get_execution_price()
        orders_row["execution_price"] = execution_price
        orders_rows.append(pd.Series(orders_row))
    order_df = pd.concat(orders_rows, axis=1).transpose()
    order_df = order_df.convert_dtypes()
    return order_df


def optimize_and_update(
    timestamp: pd.Timestamp,
    predictions: pd.Series,
    portfolio: omportfo.Portfolio,
    order_config: Dict[str, Any],
    orders,
    order_id,
) -> list:
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
    _LOG.debug("# Optimize and update portfolio")
    # Simulate order fills here.
    # Advance the portfolio state with orders up till "now"
    # Edge case: we are already initialized at this timestamp.
    # Get orders.
    if portfolio.get_last_timestamp() < timestamp:
        if len(orders) > 0:
            unfilled_orders = simulate_order_fills(orders)
        else:
            unfilled_orders = None
        portfolio.advance_portfolio_state(timestamp, unfilled_orders)
    if predictions.isna().sum() != 0:
        _LOG.debug(
            "Number of NaN predictions=`%i` at timestamp=`%s`",
            predictions.isna().sum(),
            timestamp,
        )
    priced_holdings = mark_to_market(timestamp, predictions, portfolio)
    df = compute_trades(priced_holdings, portfolio.CASH_ID)
    # Round to nearest integer towards zero.
    # df["diff_num_shares"] = np.fix(df["target_trade"] / df["price"])
    df["diff_num_shares"] = df["target_trade"] / df["price"]
    orders = generate_orders(
        df["diff_num_shares"], order_config, order_id
    )
    return orders


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
    # - Check `price_interface`.
    price_interface = config["price_interface"]
    hdbg.dassert_issubclass(price_interface, cdtfprint.AbstractPriceInterface)
    # - Check `portfolio`.
    portfolio = config["portfolio"]
    hdbg.dassert_issubclass(portfolio, omportfo.Portfolio)
    # Make a copy of `portfolio` to return (rather than modifying in-place).
    portfolio = copy.copy(portfolio)
    # - Check `order_type`
    order_type = config["order_type"]
    # The `Order` class knows the valid order types.
    hdbg.dassert_isinstance(order_type, str)
    # - Check `predictions_df`.
    hdbg.dassert_isinstance(predictions_df, pd.DataFrame)
    hpandas.dassert_index_is_datetime(predictions_df)
    hpandas.dassert_strictly_increasing_index(predictions_df)
    # TODO(Paul): Add a check for ATH start/end.
    ath_start_time = config["ath_start_time"]
    hdbg.dassert_isinstance(ath_start_time, datetime.time)
    trading_start_time = config["trading_start_time"]
    hdbg.dassert_isinstance(trading_start_time, datetime.time)
    hdbg.dassert_lte(ath_start_time, trading_start_time)
    ath_end_time = config["ath_end_time"]
    hdbg.dassert_isinstance(ath_end_time, datetime.time)
    trading_end_time = config["trading_end_time"]
    hdbg.dassert_isinstance(trading_end_time, datetime.time)
    hdbg.dassert_lte(trading_end_time, ath_end_time)
    #
    if execution_mode == "real_time":
        predictions_df = predictions_df.tail(1)
    elif execution_mode == "batch":
        pass
    else:
        raise ValueError("Unrecognized execution mode=`%s`", execution_mode)
    _LOG.debug("predictions_df=%s\n%s", str(predictions_df.shape), predictions_df)
    _LOG.debug("predictions_df.index=%s", str(predictions_df.index))
    # Cache a variable used many times.
    offset_5min = pd.DateOffset(minutes=5)
    #
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    num_rows = len(predictions_df)
    iter_ = enumerate(predictions_df.iterrows())
    orders = []
    for idx, (timestamp, predictions) in tqdm(
        iter_, total=num_rows, file=tqdm_out
    ):
        _LOG.debug("\n%s", hprint.frame("# timestamp=%s" % timestamp))
        time = timestamp.time()
        if time < ath_start_time:
            _LOG.debug(
                "time=`%s` < `ath_start_time=`%s`, skipping...",
                time,
                ath_start_time,
            )
            continue
        if time >= ath_end_time:
            _LOG.debug(
                "time=`%s` > `ath_end_time=`%s`, skipping...",
                time,
                ath_end_time,
            )
            continue
        if idx == len(predictions_df) - 1:
            # For the last timestamp we only need to mark to market, but not post
            # any more orders.
            continue
        # Enter position between now and the next 5 mins.
        timestamp_start = timestamp
        timestamp_end = timestamp + offset_5min
        if time < trading_start_time or time > trading_end_time:
            portfolio.advance_portfolio_state(timestamp_end, None)
            continue
        # Create an config for `Order`. This requires timestamps and so is
        # inside the loop.
        order_dict_ = {
            "price_interface": price_interface,
            "type_": order_type,
            "creation_timestamp": timestamp,
            "start_timestamp": timestamp_start,
            "end_timestamp": timestamp_end,
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        # Optimize, generate orders, and update the portfolio.
        orders = optimize_and_update(
            timestamp,
            predictions,
            portfolio,
            order_config,
            orders,
            order_id,
        )
        order_id += len(orders)
    return portfolio
