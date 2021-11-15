"""
Import as:

import oms.place_orders as oplord
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.dataflow.price_interface as cdtfprint
import helpers.dbg as hdbg
import helpers.hpandas as hhpandas
import helpers.htqdm as hhtqdm
import helpers.printing as hprintin
import oms.order as oord
import oms.portfolio as opor

_LOG = logging.getLogger(__name__)

# Optimizer file interface:
# alpha (predictions)
# - we have it

# holdings
# - dollar value per name
# - cash (-1)

# risk
# - query for Barra

# spread
# - either "our" good_bid - good_ask
# - David's "spread"

# volume
# - P1 for market impact

# Constraint
# do not trade

# config
# - lambda
# - various misc

def _compute_target_positions(
    current_ts: pd.Timestamp,
    predictions: pd.Series,
    portfolio: opor.Portfolio,
) -> pd.DataFrame:
    """
    Compute the target positions using `predictions`.

    :return: a dataframe about the holdings with various information, e.g.,
    ```
         curr_num_shares  price  predictions  target_wealth  target_num_shares  diff_num_shares
    101              NaN    0.0          0.1  333333.333333                inf              NaN
    202              NaN    0.0          0.2  666666.666667                inf              NaN
    ```
    """
    _LOG.debug("# Mark portfolio to market at ts=%s", current_ts)
    # Get the current holdings.
    asset_id = None
    holdings = portfolio.get_holdings(current_ts, asset_id, exclude_cash=True)
    holdings.set_index("asset_id", drop=True, inplace=True)
    _LOG.debug("holdings=\n%s", hprintin.dataframe_to_str(holdings))
    # Merge the predictions to the holdings.
    predictions = pd.DataFrame(predictions)
    predictions.reset_index(inplace=True)
    predictions.columns = ["asset_id", "predictions"]
    _LOG.debug("predictions=\n%s", hprintin.dataframe_to_str(predictions))
    merged_df = holdings.merge(predictions, on="asset_id", how="outer")
    _LOG.debug(
        "after merge: merged_df=\n%s", hprintin.dataframe_to_str(merged_df)
    )
    # Mark to market.
    merged_df = portfolio.mark_holdings_to_market(current_ts, merged_df)
    _LOG.debug("merged_df=\n%s", hprintin.dataframe_to_str(merged_df))
    columns = ["predictions", "price", "curr_num_shares"]
    hdbg.dassert_is_subset(columns, merged_df.columns)
    merged_df[columns] = merged_df[columns].fillna(0.0)
    # Compute the target notional positions.
    # TODO(gp): This is the placeholder for the optimizer. In the next iteration
    #  we will write files with the inputs, call Docker, and read the output.
    scale_factor = predictions["predictions"].abs().sum()
    _LOG.debug("scale_factor=%s", scale_factor)
    hdbg.dassert(np.isfinite(scale_factor), "scale_factor=%s", scale_factor)
    hdbg.dassert_lt(0, scale_factor)
    #
    wealth = portfolio.get_total_wealth(current_ts)
    _LOG.debug("wealth=%s", wealth)
    hdbg.dassert(np.isfinite(wealth), "wealth=%s", wealth)
    merged_df["target_wealth"] = wealth * merged_df["predictions"] / scale_factor
    merged_df["target_num_shares"] = (
        merged_df["target_wealth"] / merged_df["price"]
    )
    # Compute the target share positions.
    # TODO(gp): Round the number of shares to integers.
    merged_df["diff_num_shares"] = (
        merged_df["curr_num_shares"] - merged_df["target_num_shares"]
    )
    _LOG.debug("merged_df=\n%s", hprintin.dataframe_to_str(merged_df))
    return merged_df


# TODO(gp): We should use a RT graph executed once step at a time. For now we just
#  play back the entire thing.
# TODO(gp): -> update_holdings
def place_orders(
    predictions_df: pd.DataFrame,
    execution_mode: str,
    config: Dict[str, Any],
    # TODO(gp): -> initial_order_id
    order_id: int = 0,
) -> pd.DataFrame:
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
    :return: df with information about the placed orders
    """
    # Check the config.
    price_interface = config["price_interface"]
    hdbg.dassert_issubclass(price_interface, cdtfprint.AbstractPriceInterface)
    portfolio = config["portfolio"]
    hdbg.dassert_issubclass(portfolio, opor.Portfolio)
    # Check predictions.
    hdbg.dassert_isinstance(predictions_df, pd.DataFrame)
    hhpandas.dassert_index_is_datetime(predictions_df)
    hhpandas.dassert_strictly_increasing_index(predictions_df)
    if execution_mode == "real_time":
        predictions_df = predictions_df.tail(1)
    _LOG.debug("predictions_df=%s\n%s", str(predictions_df.shape), predictions_df)
    _LOG.debug("predictions_df.index=%s", str(predictions_df.index))
    # Cache some variables used many times.
    offset_5min = pd.DateOffset(minutes=5)
    order_type = config["order_type"]
    #
    tqdm_out = hhtqdm.TqdmToLogger(_LOG, level=logging.INFO)
    num_rows = len(predictions_df)
    iter_ = enumerate(predictions_df.iterrows())
    for idx, (ts, predictions) in tqdm(iter_, total=num_rows, file=tqdm_out):
        _LOG.debug("\n%s", hprintin.frame("# ts=%s" % ts))
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
        ts_start = ts
        ts_end = ts + offset_5min
        #
        df = _compute_target_positions(ts, predictions, portfolio)
        _LOG.debug("# Place orders")
        # Create order.
        orders: List[oord.Order] = []
        for _, row in df.iterrows():
            _LOG.debug("row=\n%s", row)
            asset_id = row["asset_id"]
            diff_num_shares = row["diff_num_shares"]
            if diff_num_shares == 0.0:
                # No need to place trades.
                continue
            order = oord.Order(
                order_id,
                price_interface,
                ts,
                asset_id,
                order_type,
                ts_start,
                ts_end,
                diff_num_shares,
            )
            order_id += 1
            _LOG.debug("order=%s", order)
            orders.append(order)
        # Execute the orders.
        # TODO(gp): We rely on the assumption that orders span only one time step
        #  so we can evaluate an order starting now and ending in the next time step.
        #  A more accurate simulation requires to attach "callbacks" representing
        #  actions to timestamp.
        next_ts = predictions_df.index[idx + 1]
        portfolio.place_orders(ts, next_ts, orders)
    # Update the df with intermediate results.
    # df_5mins[pnl] = df_5mins[wealth].pct_change()
    # return df_5mins
