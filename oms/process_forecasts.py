"""
Import as:

import oms.process_forecasts as oprofore
"""

import asyncio
import datetime
import logging
from typing import Any, Dict, List

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
    # Extract the objects from the config.
    def _get_object_from_config(key: str, expected_type: type) -> Any:
        hdbg.dassert_in(key, config)
        obj = config[key]
        hdbg.dassert_issubclass(obj, expected_type)
        return obj

    portfolio = _get_object_from_config("portfolio", omportfo.AbstractPortfolio)
    market_data_interface = portfolio.market_data_interface
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
    # Cache a variable used many times.
    offset_min = pd.DateOffset(minutes=order_duration)
    #
    get_wall_clock_time = market_data_interface.get_wall_clock_time
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
        # Wait until get_wall_clock_time() == timestamp.
        await hasynci.wait_until(next_timestamp, get_wall_clock_time)
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
        # if execution_mode == "batch":
        #     if idx == len(predictions_df) - 1:
        #         # For the last timestamp we only need to mark to market, but not post
        #         # any more orders.
        #         continue
        # Enter position between now and the next 5 mins.
        timestamp_start = wall_clock_timestamp
        timestamp_end = wall_clock_timestamp + offset_min
        # Update `portfolio` based on last fills and market movement.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Marking portfolio to market: timestamp=%s"
                % wall_clock_timestamp,
                char1="#",
            ),
        )
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
        # Wait 1 second to give all open orders sufficient time to close.
        await asyncio.sleep(1)
        # Compute the target positions.
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
        if orders:
            broker = portfolio.broker
            await broker.submit_orders(orders)


def _compute_target_positions_in_shares(
    wall_clock_timestamp: pd.Timestamp,
    predictions: pd.Series,
    portfolio: omportfo.AbstractPortfolio,
) -> pd.DataFrame:
    """
    Compute target holdings, generate orders, and update the portfolio.

    :param wall_clock_timestamp: timestamp used for valuing holdings
    :param predictions: predictions indexed by `asset_id`
    :param portfolio: portfolio with current holdings
    """
    marked_to_market = portfolio.mark_to_market().set_index("asset_id")
    # If there are predictions for assets not currently in `marked_to_market`,
    # then attempt to price those assets and extend `marked_to_market`
    # (imputing 0's for the holdings).
    unpriced_assets = predictions.index.difference(marked_to_market.index)
    if not unpriced_assets.empty:
        prices = portfolio.price_assets(
            wall_clock_timestamp, unpriced_assets.values
        )
        mtm_extension = pd.DataFrame(
            index=unpriced_assets, columns=["price", "curr_num_shares", "value"]
        )
        mtm_extension["price"] = prices
        mtm_extension.fillna(0.0)
        mtm_extension.index.name = "asset_id"
        marked_to_market = pd.concat([marked_to_market, mtm_extension], axis=0)
    marked_to_market.reset_index(inplace=True)
    _LOG.debug("marked_to_market=%s", marked_to_market)
    if predictions.isna().sum() != 0:
        _LOG.debug(
            "Number of NaN predictions=`%i` at timestamp=`%s`",
            predictions.isna().sum(),
            wall_clock_timestamp,
        )
    # Combine the portfolio `marked_to_market` dataframe with the predictions.
    assets_and_predictions = _merge_predictions(
        wall_clock_timestamp, marked_to_market, predictions, portfolio
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
    wall_clock_timestamp: pd.Timestamp,
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
    # TODO: after the merge, give cash a prediction of `1`
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
    # TODO: Check for membership first.
    predictions[portfolio.CASH_ID] = 1
    predictions = pd.DataFrame(predictions)
    predictions.columns = ["prediction"]
    predictions.index.name = "asset_id"
    _LOG.debug("predictions=\n%s", hprint.dataframe_to_str(predictions))
    # Merge current holdings and predictions.
    predictions = predictions.reset_index()
    merged_df = marked_to_market.merge(predictions, on="asset_id", how="outer")
    _LOG.debug(
        "Number of NaNs in `curr_num_shares` post-merge is `%i` at timestamp=`%s`",
        merged_df["curr_num_shares"].isna().sum(),
        wall_clock_timestamp,
    )
    merged_df["curr_num_shares"].fillna(0.0, inplace=True)
    merged_df["value"].fillna(0.0, inplace=True)
    # TODO(Paul): Fix this!! Either have the portfolio use a universe, or else
    #  price assets not held but for which we have a prediction.
    merged_df["price"].fillna(100.0, inplace=True)
    merged_df = merged_df.convert_dtypes()
    _LOG.debug("merged_df=%s", merged_df)
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
    _LOG.debug("merged_df=\n%s", hprint.dataframe_to_str(merged_df))
    columns = ["prediction", "price", "curr_num_shares", "value"]
    hdbg.dassert_is_subset(columns, merged_df.columns)
    merged_df[columns] = merged_df[columns].fillna(0.0)
    _LOG.debug("merged_df=\n%s", hprint.dataframe_to_str(merged_df))
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
            asset_id=asset_id, num_shares=shares_, **order_config.to_dict()
        )
        _LOG.debug("order=%s", order.order_id)
        orders.append(order)
    return orders
