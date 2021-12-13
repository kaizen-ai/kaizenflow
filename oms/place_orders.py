"""
Import as:

import oms.place_orders as oplaorde
"""

# TODO(Paul): -> process_forecasts.py

import asyncio
import collections
import datetime
import logging
from typing import Any, Dict, List

import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.htqdm as htqdm
import helpers.printing as hprint
import market_data.market_data_interface as mdmadain
import oms.broker as ombroker
import oms.call_optimizer as ocalopti
import oms.order as omorder
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


def _mark_to_market(
    wall_clock_timestamp: pd.Timestamp,
    predictions: pd.Series,
    portfolio: omportfo.Portfolio,
) -> pd.DataFrame:
    """
    Return price, value of all assets in `portfolio` or for which we have a
    prediction.

    :return: dataframe with columns `asset_id`, `prediction`, `price`,
        `curr_num_shares`, `value`.
        - The dataframe is the outer join of all the held assets in `portfolio` and
          `predictions`
    """
    _LOG.debug("# Mark portfolio to market at timestamp=%s", wall_clock_timestamp)
    # Get the current holdings for all the assets including cash.
    asset_id = None
    holdings = portfolio.get_holdings(
        wall_clock_timestamp, asset_id, exclude_cash=False
    )
    holdings.set_index("asset_id", drop=True, inplace=True)
    _LOG.debug("holdings=\n%s", hprint.dataframe_to_str(holdings))
    # Prepare `predictions` for the merge.
    _LOG.debug(
        "Number of non-NaN predictions=`%i` at timestamp=`%s`",
        predictions.count(),
        wall_clock_timestamp,
    )
    _LOG.debug(
        "Number of NaN predictions=`%i` at timestamp=`%s`",
        predictions.isna().sum(),
        wall_clock_timestamp,
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
        wall_clock_timestamp,
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
    merged_df = portfolio.mark_to_market(wall_clock_timestamp, merged_df)
    _LOG.debug("merged_df=\n%s", hprint.dataframe_to_str(merged_df))
    columns = ["prediction", "price", "curr_num_shares", "value"]
    hdbg.dassert_is_subset(columns, merged_df.columns)
    _LOG.debug(
        "Number of NaN prices=`%i` at timestamp=`%s`",
        merged_df["price"].isna().sum(),
        wall_clock_timestamp,
    )
    merged_df[columns] = merged_df[columns].fillna(0.0)
    return merged_df


def _generate_orders(
    shares: pd.Series,
    order_config: Dict[str, Any],
) -> List[omorder.Order]:
    """
    Turn a series of asset_id / shares to trade into a list of orders.

    :param shares: number of shares to trade, indexed by `asset_id`
    :param order_config: common parameters used to initialize `Order`
    :return: a list of nontrivial orders (i.e., no zero-share orders)
    """
    _LOG.debug("# Generate orders")
    orders: List[omorder.Order] = []
    for asset_id, shares_ in shares.iteritems():
        if shares_ == 0.0:
            # No need to place trades.
            continue
        order = omorder.Order(
            asset_id=asset_id,
            num_shares=shares_,
            **order_config.to_dict(),
        )
        _LOG.debug("order=%s", order.order_id)
        orders.append(order)
    return orders


def _compute_target_positions_in_shares(
    wall_clock_timestamp: pd.Timestamp,
    predictions: pd.Series,
    portfolio: omportfo.Portfolio,
) -> pd.DataFrame:
    """
    Compute target holdings, generate orders, and update the portfolio.

    :param wall_clock_timestamp: timestamp used for valuing holdings
    :param predictions: predictions indexed by `asset_id`
    :param portfolio: portfolio with current holdings
    """
    hdbg.dassert_eq(portfolio.get_last_timestamp(), wall_clock_timestamp)
    if predictions.isna().sum() != 0:
        _LOG.debug(
            "Number of NaN predictions=`%i` at timestamp=`%s`",
            predictions.isna().sum(),
            wall_clock_timestamp,
        )
    priced_holdings = _mark_to_market(
        wall_clock_timestamp, predictions, portfolio
    )
    df = ocalopti.compute_target_positions_in_cash(
        priced_holdings, portfolio.CASH_ID
    )
    # Round to nearest integer towards zero.
    # df["diff_num_shares"] = np.fix(df["target_trade"] / df["price"])
    df["diff_num_shares"] = df["target_trade"] / df["price"]
    return df


# TODO(Paul): -> process_forecasts()
async def place_orders(
    prediction_df: pd.DataFrame,
    # volatility_df:
    execution_mode: str,
    config: Dict[str, Any],
    # TODO(Paul): Pass Portfolio from outside we can preserve it across invocations.
    # portfolio: Portfolio,
) -> None:
    """
    Place orders corresponding to the predictions stored in the given df.

    Orders will be realized over the span of two intervals of time (i.e., two lags).

    - The PnL is realized two intervals of time after the corresponding prediction
    - The columns reported in the df are for the beginning of the interval of time
    - The columns ending with `+1` represent what happens in the next interval
      of time

    :param prediction_df: a dataframe indexed by timestamps with one column for the
        predictions for each asset
    :param execution_mode:
        - `batch`: place the trades for all the predictions (used in historical
           mode)
        - `real_time`: place the trades only for the last prediction as in a
           real-time set-up
    :param config:
        - `market_data_interface`: the interface to get price data
        - `pred_column`: the column in the df from the DAG containing the predictions
           for all the assets
        - `mark_column`: the column from the PriceInterface to mark holdings to
          market
        - `portfolio`: object used to store positions
        - `locates`: object used to access short locates
    :return: updated portfolio
    """
    _LOG.info("predictions_df=\n%s", prediction_df)
    # Check the config.
    # - Check `market_data_interface`.
    market_data_interface = config["market_data_interface"]
    hdbg.dassert_issubclass(
        market_data_interface, mdmadain.AbstractMarketDataInterface
    )
    # - Check `portfolio`.
    portfolio = config["portfolio"]
    hdbg.dassert_issubclass(portfolio, omportfo.Portfolio)
    # - Check `broker`.
    broker = config["broker"]
    hdbg.dassert_isinstance(broker, ombroker.Broker)
    # Make a copy of `portfolio` to return (rather than modifying in-place).
    # TODO(Paul): We can't make a copy.
    # portfolio = copy.copy(portfolio)
    # - Check `order_type`
    order_type = config["order_type"]
    # The `Order` class knows the valid order types.
    hdbg.dassert_isinstance(order_type, str)
    # - Check `predictions_df`.
    hdbg.dassert_isinstance(prediction_df, pd.DataFrame)
    hpandas.dassert_index_is_datetime(prediction_df)
    hpandas.dassert_strictly_increasing_index(prediction_df)
    # TODO(Paul): Add a check for ATH start/end.
    ath_start_time = config["ath_start_time"]
    hdbg.dassert_isinstance(ath_start_time, datetime.time)
    #
    trading_start_time = config["trading_start_time"]
    hdbg.dassert_isinstance(trading_start_time, datetime.time)
    hdbg.dassert_lte(ath_start_time, trading_start_time)
    #
    ath_end_time = config["ath_end_time"]
    hdbg.dassert_isinstance(ath_end_time, datetime.time)
    #
    trading_end_time = config["trading_end_time"]
    hdbg.dassert_isinstance(trading_end_time, datetime.time)
    hdbg.dassert_lte(trading_end_time, ath_end_time)
    #
    if execution_mode == "real_time":
        prediction_df = prediction_df.tail(1)
    elif execution_mode == "batch":
        pass
    else:
        raise ValueError(f"Unrecognized execution mode='{execution_mode}'")
    _LOG.debug("predictions_df=%s\n%s", str(prediction_df.shape), prediction_df)
    _LOG.debug("predictions_df.index=%s", str(prediction_df.index))
    # Cache a variable used many times.
    offset_5min = pd.DateOffset(minutes=5)
    #
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    num_rows = len(prediction_df)
    iter_ = enumerate(prediction_df.iterrows())
    for idx, (next_timestamp, predictions) in tqdm(
        iter_, total=num_rows, file=tqdm_out
    ):
        _LOG.debug(
            "\n%s",
            hprint.frame("# idx=%s next_timestamp=%s" % (idx, next_timestamp)),
        )
        # TODO(gp): Synchronize here to avoid clock drift.
        # Wait until get_wall_clock_time() == timestamp.
        # hasyncio.wait_until(timestamp, get_wall_clock_time)
        # wall_clock_timestamp = get_wall_clock_time()
        wall_clock_timestamp = next_timestamp
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        #
        time = wall_clock_timestamp.time()
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
        # if execution_mode == "batch":
        #     if idx == len(predictions_df) - 1:
        #         # For the last timestamp we only need to mark to market, but not post
        #         # any more orders.
        #         continue
        # Enter position between now and the next 5 mins.
        timestamp_start = wall_clock_timestamp
        timestamp_end = wall_clock_timestamp + offset_5min
        # Update `portfolio` based on last fills and market movement.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Updating portfolio state from fills: timestamp=%s"
                % wall_clock_timestamp,
                char1="#",
            ),
        )
        # _update_portfolio(wall_clock_timestamp, portfolio, broker)
        portfolio.update_state()
        # Continue if we are outside of our trading window.
        if time < trading_start_time or time > trading_end_time:
            continue
        # Compute target positions
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Computing target positions: timestamp=%s" % wall_clock_timestamp,
                char1="#",
            ),
        )
        target_positions = _compute_target_positions_in_shares(
            wall_clock_timestamp, predictions, portfolio
        )
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Generating orders: timestamp=%s" % wall_clock_timestamp,
                char1="#",
            ),
        )
        # Create an config for `Order`. This requires timestamps and so is
        # inside the loop.
        order_dict_ = {
            "market_data_interface": market_data_interface,
            "type_": order_type,
            "creation_timestamp": wall_clock_timestamp,
            "start_timestamp": timestamp_start,
            "end_timestamp": timestamp_end,
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        orders = _generate_orders(
            target_positions["diff_num_shares"], order_config
        )
        # Submit orders.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Submitting orders to broker: timestamp=%s"
                % wall_clock_timestamp,
                char1="#",
            ),
        )
        broker.submit_orders(orders)
        # TODO(gp): Remove this once it's synchronized above.
        await asyncio.sleep(60 * 5)
