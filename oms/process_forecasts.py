"""
Import as:

import oms.process_forecasts as oprofore
"""

import asyncio
import datetime
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import helpers.dbg as hdbg
import helpers.hasyncio as hasynci
import helpers.hpandas as hpandas
import helpers.htqdm as htqdm
import helpers.printing as hprint
import oms.call_optimizer as ocalopti
import oms.order as omorder
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


async def process_forecasts(
    prediction_df: pd.DataFrame,
    # volatility_df:
    portfolio: omportfo.AbstractPortfolio,
    execution_mode: str,
    config: Dict[str, Any],
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
        - `pred_column`: the column in the df from the DAG containing the predictions
           for all the assets
        - `mark_column`: the column from the MarketDataInterface to mark holdings to
          market
        - `portfolio`: object used to store positions
        - `locates`: object used to access short locates
    :return: updated portfolio
    """
    _LOG.info("predictions_df=\n%s", prediction_df)
    # Check `predictions_df`.
    hdbg.dassert_isinstance(prediction_df, pd.DataFrame)
    hpandas.dassert_index_is_datetime(prediction_df)
    hpandas.dassert_strictly_increasing_index(prediction_df)
    # Check `portfolio`.
    hdbg.dassert_isinstance(portfolio, omportfo.AbstractPortfolio)
    # Extract the objects from the config.
    def _get_object_from_config(key: str, expected_type: type) -> Any:
        hdbg.dassert_in(key, config)
        obj = config[key]
        hdbg.dassert_issubclass(obj, expected_type)
        return obj

    order_type = _get_object_from_config("order_type", str)
    order_duration = _get_object_from_config("order_duration", int)
    # TODO(Paul): Add a check for ATH start/end.
    ath_start_time = _get_object_from_config("ath_start_time", datetime.time)
    #
    trading_start_time = _get_object_from_config(
        "trading_start_time", datetime.time
    )
    hdbg.dassert_lte(ath_start_time, trading_start_time)
    #
    ath_end_time = _get_object_from_config("ath_end_time", datetime.time)
    trading_end_time = _get_object_from_config("trading_end_time", datetime.time)
    hdbg.dassert_lte(trading_end_time, ath_end_time)
    # We should not have anything left in the config that we didn't extract.
    # hdbg.dassert(not config, "config=%s", str(config))
    #
    if execution_mode == "real_time":
        prediction_df = prediction_df.tail(1)
    elif execution_mode == "batch":
        pass
    else:
        raise ValueError(f"Unrecognized execution mode='{execution_mode}'")
    _LOG.debug("predictions_df=%s\n%s", str(prediction_df.shape), prediction_df)
    _LOG.debug("predictions_df.index=%s", str(prediction_df.index))
    market_data_interface = portfolio.market_data_interface
    get_wall_clock_time = market_data_interface.get_wall_clock_time
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    num_rows = len(prediction_df)
    _LOG.debug("Number of rows in `prediction_df`=%d", num_rows)
    iter_ = enumerate(prediction_df.iterrows())
    # `timestamp` is the time when the forecast is available and in the current
    #  setup is also when the order should begin.
    for idx, (timestamp, predictions) in tqdm(
        iter_, total=num_rows, file=tqdm_out
    ):
        _LOG.debug(
            "\n%s",
            hprint.frame("# idx=%s timestamp=%s" % (idx, timestamp)),
        )
        # Wait until get_wall_clock_time() == timestamp.
        await hasynci.wait_until(timestamp, get_wall_clock_time)
        # Get the wall clock timestamp.
        wall_clock_timestamp = get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        # Get the time of day of the wall clock timestamp.
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
        # Continue if we are outside of our trading window.
        if time < trading_start_time or time > trading_end_time:
            continue
        # if execution_mode == "batch":
        #     if idx == len(predictions_df) - 1:
        #         # For the last timestamp we only need to mark to market, but not post
        #         # any more orders.
        #         continue
        # Wait 1 second to give all open orders sufficient time to close.
        _LOG.debug("Event: awaiting asyncio.sleep()...")
        await asyncio.sleep(1)
        _LOG.debug("Event: awaiting asyncio.sleep() done.")
        # Compute the target positions.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Computing target positions: timestamp=%s" % wall_clock_timestamp,
                char1="#",
            ),
        )
        target_positions = _compute_target_positions_in_shares(
            predictions, portfolio
        )
        # Generate orders from target positions.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Generating orders: timestamp=%s" % wall_clock_timestamp,
                char1="#",
            ),
        )
        # Enter position between now and the next `order_duration` minutes.
        # Create a config for `Order`. This requires timestamps and so is
        # inside the loop.
        timestamp_start = wall_clock_timestamp
        offset_min = pd.DateOffset(minutes=order_duration)
        timestamp_end = wall_clock_timestamp + offset_min
        order_dict_ = {
            "type_": order_type,
            "creation_timestamp": wall_clock_timestamp,
            "start_timestamp": timestamp_start,
            "end_timestamp": timestamp_end,
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        orders = _generate_orders(
            target_positions[["curr_num_shares", "diff_num_shares"]], order_config
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
        if orders:
            broker = portfolio.broker
            _LOG.debug("Event: awaiting broker.submit_orders()...")
            await broker.submit_orders(orders)
            _LOG.debug("Event: awaiting broker.submit_orders() done.")
        else:
            _LOG.debug("No orders to submit to broker.")
        _LOG.debug("portfolio=\n%s" % str(portfolio))
    _LOG.debug("Event: exiting process_forecasts() for loop.")


def _compute_target_positions_in_shares(
    predictions: pd.Series,
    portfolio: omportfo.AbstractPortfolio,
) -> pd.DataFrame:
    """
    Compute target holdings, generate orders, and update the portfolio.

    :param predictions: predictions indexed by `asset_id`
    :param portfolio: portfolio with current holdings
    """
    marked_to_market = portfolio.mark_to_market().set_index("asset_id")
    # If there are predictions for assets not currently in `marked_to_market`,
    # then attempt to price those assets and extend `marked_to_market`
    # (imputing 0's for the holdings).
    unpriced_assets = predictions.index.difference(marked_to_market.index)
    if not unpriced_assets.empty:
        _LOG.debug(
            "Unpriced assets by id=\n%s",
            "\n".join(map(str, unpriced_assets.to_list())),
        )
        prices = portfolio.price_assets(unpriced_assets.values)
        mtm_extension = pd.DataFrame(
            index=unpriced_assets, columns=["price", "curr_num_shares", "value"]
        )
        hdbg.dassert_eq(len(unpriced_assets), len(prices))
        mtm_extension["price"] = prices
        mtm_extension.index.name = "asset_id"
        marked_to_market = pd.concat([marked_to_market, mtm_extension], axis=0)
    marked_to_market.reset_index(inplace=True)
    _LOG.debug(
        "marked_to_market dataframe=\n%s"
        % hprint.dataframe_to_str(marked_to_market)
    )
    # Combine the portfolio `marked_to_market` dataframe with the predictions.
    assets_and_predictions = _merge_predictions(
        marked_to_market, predictions, portfolio
    )
    # Compute the target positions in cash (call the optimizer).
    df = ocalopti.compute_target_positions_in_cash(
        assets_and_predictions, portfolio.CASH_ID
    )
    # Convert the target positions from cash values to target share counts.
    # Round to nearest integer towards zero.
    # df["diff_num_shares"] = np.fix(df["target_trade"] / df["price"])
    df["diff_num_shares"] = df["target_trade"] / df["price"]
    return df


def _merge_predictions(
    marked_to_market: pd.DataFrame,
    predictions: pd.Series,
    portfolio,
) -> pd.DataFrame:
    """
    Merge marked_to_market dataframe with predictions.

    :return: dataframe with columns `asset_id`, `prediction`, `price`,
        `curr_num_shares`, `value`.
        - The dataframe is the outer join of all the held assets in `portfolio` and
          `predictions`
    """
    # Prepare `predictions` for the merge.
    _LOG.debug("Number of non-NaN predictions=`%i`", predictions.count())
    _LOG.debug("Number of NaN predictions=`%i`", predictions.isna().sum())
    # Ensure that `predictions` does not already include the cash id.
    hdbg.dassert_not_in(portfolio.CASH_ID, predictions.index)
    # Set the "prediction" for cash to 1. This is for the optimizer.
    predictions[portfolio.CASH_ID] = 1
    predictions = pd.DataFrame(predictions)
    # Format the predictions dataframe.
    predictions.columns = ["prediction"]
    predictions.index.name = "asset_id"
    predictions = predictions.reset_index()
    _LOG.debug("predictions=\n%s", hprint.dataframe_to_str(predictions))
    # Merge current holdings and predictions.
    merged_df = marked_to_market.merge(predictions, on="asset_id", how="outer")
    _LOG.debug(
        "Number of NaNs in `curr_num_shares` post-merge=`%i`",
        merged_df["curr_num_shares"].isna().sum(),
    )
    merged_df = merged_df.convert_dtypes()
    # Do not allow `asset_id` to be represented as a float.
    merged_df["asset_id"] = merged_df["asset_id"].convert_dtypes(
        infer_objects=False,
        convert_string=False,
        convert_boolean=False,
        convert_floating=False,
    )
    columns = ["prediction", "price", "curr_num_shares", "value"]
    hdbg.dassert_is_subset(columns, merged_df.columns)
    merged_df[columns] = merged_df[columns].fillna(0.0)
    _LOG.debug("After merge: merged_df=\n%s", hprint.dataframe_to_str(merged_df))
    return merged_df


def _generate_orders(
    shares_df: pd.DataFrame,
    order_config: Dict[str, Any],
) -> List[omorder.Order]:
    """
    Turn a series of asset_id / shares to trade into a list of orders.

    :param shares_df: dataframe indexed by `asset_id`. Contains columns
        `curr_num_shares` and `diff_num_shares`. May contain zero rows.
    :param order_config: common parameters used to initialize `Order`
    :return: a list of nontrivial orders (i.e., no zero-share orders)
    """
    _LOG.debug("# Generate orders")
    hdbg.dassert_is_subset(
        ("curr_num_shares", "diff_num_shares"), shares_df.columns
    )
    orders: List[omorder.Order] = []
    for asset_id, shares_row in shares_df.iterrows():
        curr_num_shares = shares_row["curr_num_shares"]
        diff_num_shares = shares_row["diff_num_shares"]
        hdbg.dassert(
            np.isfinite(curr_num_shares).all(),
            "All curr_num_share values must be finite.",
        )
        hdbg.dassert(
            np.isfinite(diff_num_shares).all(),
            "All diff_num_share values must be finite.",
        )
        if diff_num_shares == 0.0:
            # No need to place trades.
            continue
        order = omorder.Order(
            asset_id=asset_id,
            curr_num_shares=curr_num_shares,
            diff_num_shares=diff_num_shares,
            **order_config.to_dict(),
        )
        _LOG.debug("order=%s", order.order_id)
        orders.append(order)
    _LOG.debug("Number of orders generated=%i", len(orders))
    return orders
