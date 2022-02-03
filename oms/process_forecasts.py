"""
Import as:

import oms.process_forecasts as oprofore
"""

import asyncio
import collections
import datetime
import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.htqdm as htqdm
import oms.call_optimizer as ocalopti
import oms.order as omorder
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


async def process_forecasts(
    prediction_df: pd.DataFrame,
    volatility_df: pd.DataFrame,
    portfolio: omportfo.AbstractPortfolio,
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
    :param volatility_df: like `prediction_df`, but for volatility
    :param portfolio: initialized `Portfolio` object
    :param config:
        - `execution_mode`:
            - `batch`: place the trades for all the predictions (used in historical
               mode)
            - `real_time`: place the trades only for the last prediction as in a
        - `log_dir`: directory for logging state
    """
    # Check `predictions_df`.
    hdbg.dassert_isinstance(prediction_df, pd.DataFrame)
    hpandas.dassert_index_is_datetime(prediction_df)
    hpandas.dassert_strictly_increasing_index(prediction_df)
    # Check `volatility_df`.
    hdbg.dassert_isinstance(volatility_df, pd.DataFrame)
    hpandas.dassert_index_is_datetime(volatility_df)
    hpandas.dassert_strictly_increasing_index(volatility_df)
    # Check index compatibility.
    hdbg.dassert(prediction_df.index.equals(volatility_df.index))
    # Check `portfolio`.
    hdbg.dassert_isinstance(portfolio, omportfo.AbstractPortfolio)
    # TODO(*): Make `order_config` a subconfig.
    # Create an `order_config` from `config` elements.
    order_type = _get_object_from_config(config, "order_type", str)
    order_duration = _get_object_from_config(config, "order_duration", int)
    order_config = cconfig.get_config_from_nested_dict(
        {
            "order_type": order_type,
            "order_duration": order_duration,
        }
    )
    # Extract ATH and trading start times from config.
    # TODO(Paul): Add a check for ATH start/end.
    ath_start_time = _get_object_from_config(
        config, "ath_start_time", datetime.time
    )
    trading_start_time = _get_object_from_config(
        config, "trading_start_time", datetime.time
    )
    # Ensure `ath_start_time` <= `trading_start_time`.
    hdbg.dassert_lte(ath_start_time, trading_start_time)
    # Extract end times and sanity-check.
    ath_end_time = _get_object_from_config(config, "ath_end_time", datetime.time)
    trading_end_time = _get_object_from_config(
        config, "trading_end_time", datetime.time
    )
    hdbg.dassert_lte(trading_end_time, ath_end_time)
    # Get executiom mode ("real_time" or "batch").
    execution_mode = _get_object_from_config(config, "execution_mode", str)
    if execution_mode == "real_time":
        prediction_df = prediction_df.tail(1)
    elif execution_mode == "batch":
        pass
    else:
        raise ValueError(f"Unrecognized execution mode='{execution_mode}'")
    # Get log dir.
    log_dir = config.get("log_dir", None)
    # We should not have anything left in the config that we didn't extract.
    # hdbg.dassert(not config, "config=%s", str(config))
    #
    _LOG.debug(
        "predictions_df=%s\n%s",
        str(prediction_df.shape),
        hpandas.df_to_str(prediction_df),
    )
    _LOG.debug("predictions_df.index=%s", str(prediction_df.index))
    num_rows = len(prediction_df)
    _LOG.debug("Number of rows in `prediction_df`=%d", num_rows)
    #
    get_wall_clock_time = portfolio.market_data.get_wall_clock_time
    tqdm_out = htqdm.TqdmToLogger(_LOG, level=logging.INFO)
    iter_ = enumerate(prediction_df.iterrows())
    offset_min = pd.DateOffset(minutes=order_duration)
    # Initialize a `ForecastProcessor` object to perform the heavy lifting.
    forecast_processor = ForecastProcessor(portfolio, order_config, log_dir)
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
        if get_wall_clock_time() > timestamp:
            # E.g., it's 10:21:51, we computed the forecast for [10:20, 10:25]
            # bar. As long as it's before 10:25, we want to place the order. If
            # it's later, either assert or log it as a problem.
            hdbg.dassert_lte(get_wall_clock_time(), timestamp + offset_min)
        else:
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
        volatility = volatility_df.loc[timestamp]
        orders = forecast_processor.generate_orders(predictions, volatility)
        await forecast_processor.submit_orders(orders)
        _LOG.debug("ForecastProcessor=\n%s", str(forecast_processor))
    _LOG.debug("Event: exiting process_forecasts() for loop.")


class ForecastProcessor:
    def __init__(
        self,
        portfolio: omportfo.AbstractPortfolio,
        order_config: cconfig.Config,
        log_dir: Optional[str] = None,
    ) -> None:
        self._portfolio = portfolio
        self._get_wall_clock_time = portfolio.market_data.get_wall_clock_time
        self._order_config = order_config
        # TODO(Paul): process config with checks.
        self._order_type = self._order_config["order_type"]
        self._order_duration = self._order_config["order_duration"]
        self._offset_min = pd.DateOffset(minutes=self._order_duration)
        #
        self._log_dir = log_dir
        #
        self._target_positions = collections.OrderedDict()
        self._orders = collections.OrderedDict()

    def __str__(self) -> str:
        """
        Return the most recent state of the ForecastProcessor as a string.
        """
        act = []
        if self._target_positions:
            last_key = next(reversed(self._target_positions))
            target_positions = self._target_positions[last_key]
            target_positions_str = hpandas.df_to_str(target_positions)
        else:
            target_positions_str = "None"
        act.append("# last target positions=\n%s" % target_positions_str)
        if self._orders:
            last_key = next(reversed(self._orders))
            orders_str = self._orders[last_key]
        else:
            orders_str = "None"
        act.append("# last orders=\n%s" % orders_str)
        act = "\n".join(act)
        return act

    def log_state(self) -> None:
        """
        Log the most recent state of the object.
        """
        hdbg.dassert(self._log_dir, "Must specify `log_dir` to log state.")
        #
        wall_clock_time = self._get_wall_clock_time()
        wall_clock_time_str = wall_clock_time.strftime("%Y%m%d_%H%M%S")
        filename = f"{wall_clock_time_str}.csv"
        #
        if self._target_positions:
            last_key = next(reversed(self._target_positions))
            last_target_positions = self._target_positions[last_key]
            last_target_positions_filename = os.path.join(
                self._log_dir, "target_positions", filename
            )
            hio.create_enclosing_dir(
                last_target_positions_filename, incremental=True
            )
            last_target_positions.to_csv(last_target_positions_filename)
        if self._orders:
            last_key = next(reversed(self._orders))
            last_orders = self._orders[last_key]
            last_orders_filename = os.path.join(self._log_dir, "orders", filename)
            hio.create_enclosing_dir(last_orders_filename, incremental=True)
            hio.to_file(last_orders_filename, last_orders)

    def generate_orders(
        self,
        predictions: pd.Series,
        volatility: pd.Series,
    ) -> List[omorder.Order]:
        """
        Translate returns and volatility forecasts into a list of orders.

        :param predictions: returns forecasts
        :param volatility: volatility forecasts
        :return: a list of orders to execute
        """
        # Convert forecasts into target positions.
        target_positions = self._compute_target_positions_in_shares(
            predictions, volatility
        )
        # Get the wall clock timestamp and internally log `target_positions`.
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        self._sequential_insert(
            wall_clock_timestamp, target_positions, self._target_positions
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
        # Create a config for `Order`.
        timestamp_start = wall_clock_timestamp
        timestamp_end = wall_clock_timestamp + self._offset_min
        order_dict_ = {
            "type_": self._order_type,
            "creation_timestamp": wall_clock_timestamp,
            "start_timestamp": timestamp_start,
            "end_timestamp": timestamp_end,
        }
        order_config = cconfig.get_config_from_nested_dict(order_dict_)
        orders = self._generate_orders(
            target_positions[["curr_num_shares", "diff_num_shares"]], order_config
        )
        # Convert orders to a string representation and internally log.
        orders_as_str = omorder.orders_to_string(orders)
        self._sequential_insert(wall_clock_timestamp, orders_as_str, self._orders)
        return orders

    async def submit_orders(self, orders) -> None:
        """
        Submit `orders` to the broker and confirm receipt.

        :param orders: list of orders to execute
        """
        # Submit orders.
        if orders:
            broker = self._portfolio.broker
            _LOG.debug("Event: awaiting broker.submit_orders()...")
            await broker.submit_orders(orders)
            _LOG.debug("Event: awaiting broker.submit_orders() done.")
        else:
            _LOG.debug("No orders to submit to broker.")
        _LOG.debug("portfolio=\n%s" % str(self._portfolio))
        if self._log_dir:
            self.log_state()
            self._portfolio.log_state(os.path.join(self._log_dir, "portfolio"))

    def _compute_target_positions_in_shares(
        self,
        predictions: pd.Series,
        volatility: pd.Series,
    ) -> pd.DataFrame:
        """
        Compute target holdings in shares.

        :param predictions: predictions indexed by `asset_id`
        :param volatility: volatility forecasts indexed by `asset_id`
        """
        assets_and_predictions = self._prepare_data_for_optimizer(
            predictions, volatility
        )
        # Compute the target positions in cash (call the optimizer).
        df = ocalopti.compute_target_positions_in_cash(
            assets_and_predictions, self._portfolio.CASH_ID
        )
        # Convert the target positions from cash values to target share counts.
        # Round to nearest integer towards zero.
        # df["diff_num_shares"] = np.fix(df["target_trade"] / df["price"])
        df["diff_num_shares"] = df["target_trade"] / df["price"]
        return df

    def _prepare_data_for_optimizer(
        self,
        predictions: pd.Series,
        volatility: pd.Series,
    ) -> pd.DataFrame:
        """
        Clean up data for optimization.

        Cleaning includes ensuring data completeness and NaN handling.

        :param predictions: predictions indexed by `asset_id`
        :param volatility: volatility forecasts indexed by `asset_id`
        """
        hdbg.dassert(
            predictions.index.equals(volatility.index),
            "`predictions` and `volatility` have mismatched indices of asset ids",
        )
        marked_to_market = self._get_extended_marked_to_market_df(predictions)
        # Combine the portfolio `marked_to_market` dataframe with the predictions.
        df_for_optimizer = self._merge_predictions(
            marked_to_market, predictions, volatility
        )
        return df_for_optimizer

    def _get_extended_marked_to_market_df(
        self,
        predictions: pd.Series,
    ) -> pd.DataFrame:
        """
        Get portfolio `mark_to_market()` df and extend to all predictions.

        If the portfolio is initialized with the trading universe, then this
        should be a no-op.

        :param predictions: predictions indexed by `asset_id`
        :return:
        """
        marked_to_market = self._portfolio.mark_to_market().set_index("asset_id")
        # If there are predictions for assets not currently in `marked_to_market`,
        # then attempt to price those assets and extend `marked_to_market`
        # (imputing 0's for the holdings).
        unpriced_assets = predictions.index.difference(marked_to_market.index)
        if not unpriced_assets.empty:
            _LOG.debug(
                "Unpriced assets by id=\n%s",
                "\n".join(map(str, unpriced_assets.to_list())),
            )
            prices = self._portfolio.price_assets(unpriced_assets.values)
            mtm_extension = pd.DataFrame(
                index=unpriced_assets,
                columns=["price", "curr_num_shares", "value"],
            )
            hdbg.dassert_eq(len(unpriced_assets), len(prices))
            mtm_extension["price"] = prices
            mtm_extension.index.name = "asset_id"
            marked_to_market = pd.concat(
                [marked_to_market, mtm_extension], axis=0
            )
        marked_to_market.reset_index(inplace=True)
        _LOG.debug(
            "marked_to_market dataframe=\n%s"
            % hpandas.df_to_str(marked_to_market)
        )
        return marked_to_market

    def _normalize_predictions_srs(
        self, predictions: pd.Series, index: pd.DatetimeIndex
    ) -> pd.DataFrame:
        """
        Normalize predictions with `index`, NaN-filling, and df conversion.
        series.

        :param predictions:
        :param index:
        :return:
        """
        _LOG.debug("Number of predictions=%i", predictions.size)
        _LOG.debug("Number of non-NaN predictions=%i", predictions.count())
        _LOG.debug("Number of NaN predictions=%i", predictions.isna().sum())
        # Ensure that `predictions` does not already include the cash id.
        hdbg.dassert_not_in(self._portfolio.CASH_ID, predictions.index)
        # Ensure that `index` includes `predictions.index`.
        hdbg.dassert(predictions.index.difference(index).empty)
        # Extend `predictions` to `index`.
        predictions = predictions.reindex(index)
        # Set the "prediction" for cash to 1. This is for the optimizer.
        predictions[self._portfolio.CASH_ID] = 1
        # Impute zero for NaNs.
        predictions = predictions.fillna(0.0)
        # Convert to a dataframe.
        predictions = pd.DataFrame(predictions)
        # Format the predictions dataframe.
        predictions.columns = ["prediction"]
        predictions.index.name = "asset_id"
        predictions = predictions.reset_index()
        _LOG.debug("predictions=\n%s", hpandas.df_to_str(predictions))
        return predictions

    def _normalize_volatility_srs(
        self, volatility: pd.Series, index: pd.DatetimeIndex
    ) -> pd.DataFrame:
        """
        Normalize predictions with `index`, NaN-filling, and df conversion.

        :param volatility:
        :param index:
        :return:
        """
        # TODO(Paul): Factor out common part with predictions normalization.
        _LOG.debug("Number of volatility forecasts=%i", volatility.size)
        _LOG.debug(
            "Number of non-NaN volatility forecasts=%i", volatility.count()
        )
        _LOG.debug(
            "Number of NaN volatility forecasts=%i", volatility.isna().sum()
        )
        # Ensure that `predictions` does not already include the cash id.
        hdbg.dassert_not_in(self._portfolio.CASH_ID, volatility.index)
        # Ensure that `index` includes `volatility.index`.
        hdbg.dassert(volatility.index.difference(index).empty)
        # Extend `volatility` to `index`.
        volatility = volatility.reindex(index)
        # Compute average volatility.
        mean_volatility = volatility.mean()
        if not np.isfinite(mean_volatility):
            mean_volatility = 1.0
        # Set the "volatility" for cash to 1. This is for the optimizer.
        volatility[self._portfolio.CASH_ID] = 0
        # Impute mean volatility.
        volatility = volatility.fillna(mean_volatility)
        # Convert to a dataframe.
        volatility = pd.DataFrame(volatility)
        # Format the predictions dataframe.
        volatility.columns = ["volatility"]
        volatility.index.name = "asset_id"
        volatility = volatility.reset_index()
        _LOG.debug("volatility=\n%s", hpandas.df_to_str(volatility))
        return volatility

    def _merge_predictions(
        self,
        marked_to_market: pd.DataFrame,
        predictions: pd.Series,
        volatility: pd.Series,
    ) -> pd.DataFrame:
        """
        Merge marked_to_market dataframe with predictions and volatility.

        :return: dataframe with columns `asset_id`, `prediction`, `price`,
            `curr_num_shares`, `value`.
            - The dataframe is the outer join of all the held assets in `portfolio` and
              `predictions`
        """
        # `predictions` and `volatility` should have exactly the same index.
        hdbg.dassert(predictions.index.equals(volatility.index))
        # The portfolio may have grandfathered holdings for which there is no
        # prediction.
        idx = predictions.index.union(
            marked_to_market.set_index("asset_id").index
        )
        predictions = self._normalize_predictions_srs(predictions, idx)
        volatility = self._normalize_volatility_srs(volatility, idx)
        # Merge current holdings and predictions.
        merged_df = marked_to_market.merge(
            predictions, on="asset_id", how="outer"
        )
        merged_df = merged_df.merge(
            volatility,
            on="asset_id",
            how="outer",
        )
        _LOG.debug(
            "Number of NaNs in `curr_num_shares` post-merge=`%i`",
            merged_df["curr_num_shares"].isna().sum(),
        )
        merged_df = merged_df.convert_dtypes()
        merged_df = merged_df.fillna(0.0)
        _LOG.debug("After merge: merged_df=\n%s", hpandas.df_to_str(merged_df))
        return merged_df

    @staticmethod
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
                np.isfinite(curr_num_shares),
                "The curr_num_share value must be finite.",
            )
            if not np.isfinite(diff_num_shares):
                _LOG.debug(
                    "`diff_num_shares`=%f for `asset_id`=%i",
                    diff_num_shares,
                    asset_id,
                )
                diff_num_shares = 0.0
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

    # TODO(Paul): This is the same as the corresponding method in `Portfolio`.
    @staticmethod
    def _sequential_insert(
        key: pd.Timestamp,
        obj: Any,
        odict: Dict[pd.Timestamp, Any],
    ) -> None:
        """
        Insert `(key, obj)` in `odict` ensuring that keys are in increasing
        order.

        Assume that `odict` is a dict maintaining the insertion order of
        the keys.
        """
        hdbg.dassert_isinstance(key, pd.Timestamp)
        hdbg.dassert_isinstance(odict, collections.OrderedDict)
        # Ensure that timestamps are inserted in increasing order.
        if odict:
            last_key = next(reversed(odict))
            hdbg.dassert_lt(last_key, key)
        # TODO(Paul): If `obj` is a series or dataframe, ensure that the index is
        #  unique.
        odict[key] = obj


# Extract the objects from the config.
def _get_object_from_config(
    config: cconfig.Config, key: str, expected_type: type
) -> Any:
    hdbg.dassert_in(key, config)
    obj = config[key]
    hdbg.dassert_issubclass(obj, expected_type)
    return obj
