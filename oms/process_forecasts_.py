"""
Import as:

import oms.process_forecasts_ as oprofore
"""

import asyncio
import datetime
import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import core.finance as cofinanc
import core.key_sorted_ordered_dict as cksoordi
import helpers.hasyncio as hasynci
import helpers.hdbg as hdbg
import helpers.hdict as hdict
import helpers.hio as hio
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.htqdm as htqdm
import helpers.hwall_clock_time as hwacltim
import oms.call_optimizer as ocalopti
import oms.order as omorder
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


# `ProcessForecastsNode`
# - Adapts `process_forecasts()` to a DataFlow node
# `process_forecasts()`
# - Contains the loop processing the predictions
# - Instantiates `ForecastProcessor` to do the actual work every bar


# - `wake_up_timestamp`:
#   - used by `DagRunner`
#   - when the system starts processing the DagRunner loop
#   - E.g., the system can wake up at 9:00am to warm up some functions
#
# - `time_out_in_secs`:
#   - used by `DagRunner`
#   - when the system stops running the DagRunner loop
#
# - `ath_start_time`, `ath_end_time`:
#   - used by `process_forecasts()`
#   - describe when the market is open
#   - right now it's used but in a redundant way with `trading_start_time`
#   - we want to snapshot the Portfolio during all the ATH
#
# - `trading_start_time`, `trading_end_time`
#   - used by `process_forecasts()`
#   - we use it to filter the forecasts reaching the Optimizer when the system is
#     producing forecasts
#
# Invariant: `ath_{start,end}_time` and `trading_{start,end}_time` are inclusive
# so that we can use the usual times like 9:30 and 16:00 as boundaries of ATH
# interval.


async def process_forecasts(
    prediction_df: pd.DataFrame,
    volatility_df: pd.DataFrame,
    portfolio: omportfo.Portfolio,
    # TODO(gp): It should be a dict -> config_dict, process_forecasts_dict. Do
    #  we need to use a bit of stuttering to make the name unique in the config?
    config: Dict[str, Any],
    # TODO(gp): Should we keep all the dfs close together in the interface?
    *,
    spread_df: Optional[pd.DataFrame],
    restrictions_df: Optional[pd.DataFrame],
) -> None:
    """
    Place orders corresponding to the predictions stored in the passed df.

    Orders will be realized over the span of two intervals of time (i.e., two lags).

    - The PnL is realized two intervals of time after the corresponding prediction
    - The columns reported in the df are for the beginning of the interval of time
    - The columns ending with `+1` represent what happens in the next interval
      of time

    :param prediction_df: a dataframe indexed by timestamps with one column for the
        predictions for each asset
    :param volatility_df: like `prediction_df`, but for volatility
    :param spread_df: like `prediction_df`, but for the bid-ask spread
    :param portfolio: initialized `Portfolio` object
    :param config: the required params are:
          ```
          {
            "order_dict": dict,
            "optimizer_dict": dict,
            "ath_start_time": Optional[datetime.time],
            "trading_start_time": Optional[datetime.time],
            "ath_end_time": Optional[datetime.time],
            "trading_end_time": Optional[datetime.time],
            "execution_mode": str ["real_time", "batch"],
            "remove_weekends": Optional[bool],
            # - Force liquidating the holdings in the bar that corresponds to
            #   trading_end_time, which must be not None.
            "liquidate_at_trading_end_time": bool
            "log_dir": Optional[str],
          }
          ```
        - `execution_mode`:
            - `batch`: place the trades for all the predictions (used in historical
               mode)
            - `real_time`: place the trades only for the last prediction in the df
              (used in real-time mode)
        - `log_dir`: directory for logging state
    """
    # TODO(gp): Move all this in a _validate method
    # Check `predictions_df`.
    hpandas.dassert_time_indexed_df(
        prediction_df, allow_empty=True, strictly_increasing=True
    )
    # Check `volatility_df`.
    hpandas.dassert_time_indexed_df(
        volatility_df, allow_empty=True, strictly_increasing=True
    )
    # Check `spread_df`.
    if spread_df is None:
        _LOG.info("spread_df is `None`; imputing 0.0 spread")
        spread_df = pd.DataFrame(0.0, prediction_df.index, prediction_df.columns)
    hpandas.dassert_time_indexed_df(
        spread_df, allow_empty=True, strictly_increasing=True
    )
    # Check index/column compatibility among the dfs.
    hpandas.dassert_axes_equal(prediction_df, volatility_df)
    hpandas.dassert_axes_equal(prediction_df, spread_df)
    # Check `portfolio`.
    hdbg.dassert_isinstance(portfolio, omportfo.Portfolio)
    hdbg.dassert_isinstance(config, dict)
    # Check `restrictions`.
    if restrictions_df is None:
        _LOG.info("restrictions_df is `None`; no restrictions will be enforced")
    # See https://stackoverflow.com/questions/21706609/where-is-the-nonetype-located
    NoneType = type(None)
    # Create an `order_dict` from `config` elements.
    order_dict = hdict.typed_get(
        config, "order_config", expected_type=(dict, NoneType)
    )
    _validate_order_dict(order_dict)
    optimizer_dict = hdict.typed_get(
        config, "optimizer_config", expected_type=(dict, NoneType)
    )
    _validate_optimizer_dict(optimizer_dict)
    # Extract ATH and trading start times from config.
    ath_start_time = hdict.typed_get(
        config, "ath_start_time", expected_type=(datetime.time, NoneType)
    )
    trading_start_time = hdict.typed_get(
        config, "trading_start_time", expected_type=(datetime.time, NoneType)
    )
    # Extract ATH and trading end times from config.
    ath_end_time = hdict.typed_get(
        config, "ath_end_time", expected_type=(datetime.time, NoneType)
    )
    trading_end_time = hdict.typed_get(
        config, "trading_end_time", expected_type=(datetime.time, NoneType)
    )
    liquidate_at_trading_end_time = hdict.typed_get(
        config,
        "liquidate_at_trading_end_time",
        expected_type=bool,
    )
    # Sanity check trading time.
    _validate_trading_time(
        ath_start_time,
        ath_end_time,
        trading_start_time,
        trading_end_time,
        liquidate_at_trading_end_time,
    )
    # Get execution mode ("real_time" or "batch").
    execution_mode = hdict.typed_get(config, "execution_mode", expected_type=str)
    if execution_mode == "real_time":
        prediction_df = prediction_df.tail(1)
    elif execution_mode == "batch":
        pass
    else:
        raise ValueError(f"Unrecognized execution mode='{execution_mode}'")
    # TODO(Paul): Pass in a trading calendar explicitly instead of simply
    #   filtering out weekends.
    if "remove_weekends" in config and config["remove_weekends"]:
        prediction_df = cofinanc.remove_weekends(prediction_df)
        volatility_df = cofinanc.remove_weekends(volatility_df)
        spread_df = cofinanc.remove_weekends(spread_df)
    # Get log dir.
    log_dir = config.get("log_dir", None)
    _LOG.info("log_dir=%s", log_dir)
    # We should not have anything left in the config that we didn't extract.
    # hdbg.dassert(not config, "config=%s", str(config))
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
    offset_min = pd.DateOffset(minutes=order_dict["order_duration_in_mins"])
    # Initialize a `ForecastProcessor` object to perform the heavy lifting.
    forecast_processor = ForecastProcessor(
        portfolio,
        order_dict,
        optimizer_dict,
        restrictions_df,
        log_dir=log_dir,
    )
    if execution_mode == "batch":
        # In "batch" mode this function is in charge of advancing the bar.
        hwacltim.reset_current_bar_timestamp()
    # `timestamp` is the time when the forecast is available and in the current
    #  setup is also when the order should begin.
    for idx, (timestamp, predictions) in tqdm(
        iter_, total=num_rows, file=tqdm_out
    ):
        if execution_mode == "batch":
            # Update the global state tracking the current bar.
            # The loop in `RealTimeDagRunner` sets the bar based on align_on_grid.
            # If we are running `run_process_forecasts.py`
            hwacltim.set_current_bar_timestamp(timestamp)
        #
        current_bar_timestamp = hwacltim.get_current_bar_timestamp()
        hdbg.dassert_is_not(current_bar_timestamp, None)
        current_bar_time = current_bar_timestamp.time()
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "# idx=%s timestamp=%s current_bar_time=%s"
                % (idx, timestamp, current_bar_time)
            ),
        )
        # Wait until get_wall_clock_time() == timestamp.
        if get_wall_clock_time() > timestamp:
            # E.g., it's 10:21:51, we computed the forecast for [10:20, 10:25]
            # bar. As long as it's before 10:25, we want to place the order. If
            # it's later, either assert or log it as a problem.
            hdbg.dassert_lte(get_wall_clock_time(), timestamp + offset_min)
        else:
            _LOG.debug("async_wait_until")
            await hasynci.async_wait_until(timestamp, get_wall_clock_time)
        # Get the wall clock timestamp.
        wall_clock_timestamp = get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        # if execution_mode == "batch":
        #     if idx == len(predictions_df) - 1:
        #         # For the last timestamp we only need to mark to market, but not
        #         # post any more orders.
        #         continue
        # Wait 1 second to give all open orders sufficient time to close.
        _LOG.debug("Event: awaiting asyncio.sleep()...")
        await asyncio.sleep(1)
        _LOG.debug("Event: awaiting asyncio.sleep() done.")
        skip_generating_orders = _skip_generating_orders(
            current_bar_time,
            ath_start_time,
            ath_end_time,
            trading_start_time,
            trading_end_time,
        )
        # Compute the target positions.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Computing target positions: timestamp=%s current_bar_time=%s"
                % (wall_clock_timestamp, current_bar_time),
                char1="#",
            ),
        )
        if skip_generating_orders:
            _LOG.warning("Skipping generating orders")
            # Keep logging the state and marking to market even if we don't submit
            # orders.
            # `forecast_processor.submit_orders()` takes care of logging
            # `forecast_processor.generate_orders()` takes care of marking to market
            # the portfolio.
            mark_to_market = True
            forecast_processor.log_state(mark_to_market)
            continue
        # TODO(gp): Use an FSM to make the code more reliable.
        # Compute whether to liquidate the holdings or not.
        liquidate_holdings = False
        if liquidate_at_trading_end_time:
            hdbg.dassert_is_not(trading_end_time, None)
            _LOG.debug(
                "liquidate_at_trading_trading_trading_trading_trading_trading_trading_trading_end_time: "
                + hprint.to_str("trading_end_time current_bar_time")
            )
            if current_bar_time >= trading_end_time:
                _LOG.info(
                    "Liquidating holdings: "
                    + hprint.to_str("trading_end_time current_bar_time")
                )
                liquidate_holdings = True
        # Generate orders.
        volatility = volatility_df.loc[timestamp]
        spread = spread_df.loc[timestamp]
        orders = forecast_processor.generate_orders(
            predictions,
            volatility,
            spread,
            liquidate_holdings,
        )
        # Submit orders.
        await forecast_processor.submit_orders(orders)
        _LOG.debug("ForecastProcessor=\n%s", str(forecast_processor))
    _LOG.debug("Event: exiting process_forecasts() for loop.")


# TODO(Grisha): @Dan Move to `helpers/hdbg.py`
def dassert_all_defined_or_all_None(
    vals: List[Any],
    msg: Optional[str] = None,
    *args: Any,
    only_warning: bool = False,
) -> None:
    """
    Check that all the values in a list are either all defined or all None.
    """
    all_defined_cond = all(val is not None for val in vals)
    all_none_cond = all(val is None for val in vals)
    cond = all_defined_cond or all_none_cond
    if not cond:
        txt = f"Some values in list are defined and some are None: '{vals}'"
        hdbg._dfatal(txt, msg, *args, only_warning=only_warning)


def _validate_trading_time(
    ath_start_time: Optional[datetime.time],
    ath_end_time: Optional[datetime.time],
    trading_start_time: Optional[datetime.time],
    trading_end_time: Optional[datetime.time],
    liquidate_at_trading_end_time: bool,
) -> None:
    """
    Check that trading hours are specified correctly.
    """
    dassert_all_defined_or_all_None(
        [ath_start_time, ath_end_time, trading_start_time, trading_end_time]
    )
    # TODO(gp): Not sure about this. For equities it's possible that we
    #  specify ath start/end time, but then we leave trading start/end time
    #  equal to None.
    if ath_start_time is not None:
        hdbg.dassert_lte(ath_start_time, ath_end_time)
        hdbg.dassert_lte(trading_start_time, trading_end_time)
        hdbg.dassert_lte(ath_start_time, trading_start_time)
        hdbg.dassert_lte(trading_end_time, ath_end_time)
    #
    if liquidate_at_trading_end_time:
        hdbg.dassert_is_not(trading_end_time, None)


def _skip_generating_orders(
    time: datetime.time,
    ath_start_time: Optional[datetime.time],
    ath_end_time: Optional[datetime.time],
    trading_start_time: Optional[datetime.time],
    trading_end_time: Optional[datetime.time],
) -> bool:
    """
    Determine whether to skip a bar processing or not.
    """
    _LOG.debug(
        hprint.to_str(
            "time ath_start_time ath_end_time trading_start_time trading_end_time"
        )
    )
    # TODO(gp): Not sure about this.
    # Sanity check: all values are defined together or are all `None`.
    trading_time_list = [
        ath_start_time,
        ath_end_time,
        trading_start_time,
        trading_end_time,
    ]
    all_defined_cond = all(val is not None for val in trading_time_list)
    #
    skip_bar_cond = False
    if all_defined_cond:
        # Filter based on ATH.
        if time < ath_start_time:
            _LOG.debug(
                "time=%s < ath_start_time=%s, skipping bar ...",
                time,
                ath_start_time,
            )
            skip_bar_cond = True
        if time > ath_end_time:
            _LOG.debug(
                "time=%s > ath_end_time=%s, skipping bar ...",
                time,
                ath_end_time,
            )
            skip_bar_cond = True
        # Filter based on trading times.
        if time < trading_start_time:
            _LOG.debug(
                "time=%s < trading_start_time=%s, skipping bar ...",
                time,
                trading_start_time,
            )
            skip_bar_cond = True
        if time > trading_end_time:
            _LOG.debug(
                "time=%s > trading_end_time=%s, skipping bar ...",
                time,
                trading_end_time,
            )
            skip_bar_cond = True
    _LOG.debug(hprint.to_str("skip_bar_cond"))
    return skip_bar_cond


# #############################################################################
# ForecastProcessor
# #############################################################################


# TODO(gp): -> _ForecastProcessor
class ForecastProcessor:
    """
    Take forecasts for the most recent bar and submit orders.

    - Retrieve the Portfolio holdings
    - Perform optimization on the forecasts
    - Generate orders
    - Submit orders
    """

    def __init__(
        self,
        portfolio: omportfo.Portfolio,
        # TODO(gp): -> order_dict?
        order_dict: cconfig.Config,
        # TODO(gp): -> optimizer_dict
        optimizer_dict: cconfig.Config,
        # TODO(gp): -> restrictions_df like the process_forecast
        restrictions: Optional[pd.DataFrame],
        *,
        log_dir: Optional[str] = None,
    ) -> None:
        """
        Build the object.

        :param order_dict: config for placing the orders, e.g.,
            ```
            order_type: price@twap
            order_duration_in_mins: 5
            ```
        :param optimizer_dict: config for the optimizer, e.g.,
            ```
            backend: pomo
            params:
              style: cross_sectional
              kwargs:
                bulk_frac_to_remove: 0.0
                bulk_fill_method: zero
                target_gmv: 100000.0
            ```
        :param log_dir: directory to log different stages of computation
            - Saved by `ForecastProcessor`
                - `orders`
                - `portfolio`
                - `target_positions`
        """
        self._portfolio = portfolio
        self._get_wall_clock_time = portfolio.market_data.get_wall_clock_time
        # Process order config.
        _validate_order_dict(order_dict)
        self._order_dict = order_dict
        self._offset_min = pd.DateOffset(
            minutes=order_dict["order_duration_in_mins"]
        )
        # Process optimizer config.
        _validate_optimizer_dict(optimizer_dict)
        self._optimizer_dict = optimizer_dict
        #
        self._restrictions = restrictions
        #
        self._log_dir = log_dir
        # Store the target positions.
        self._target_positions = cksoordi.KeySortedOrderedDict(pd.Timestamp)
        self._orders = cksoordi.KeySortedOrderedDict(pd.Timestamp)

    def __str__(self) -> str:
        """
        Return the most recent state of the ForecastProcessor as a string.
        """
        act = []
        act.append("# last target positions=")
        if self._target_positions:
            _, target_positions = self._target_positions.peek()
            target_positions_str = hpandas.df_to_str(target_positions)
        else:
            target_positions_str = "None"
        act.append(target_positions_str)
        #
        act.append("# last orders=")
        if self._orders:
            _, orders_str = self._orders.peek()
        else:
            orders_str = "None"
        act.append(orders_str)
        #
        act = "\n".join(act)
        return act

    @staticmethod
    def read_logged_target_positions(
        log_dir: str,
        *,
        tz: str = "America/New_York",
    ) -> pd.DataFrame:
        """
        Parse logged `target_position` dataframes.

        :return a dataframe indexed by datetimes and with two column levels
        """
        name = "target_positions"
        files = ForecastProcessor._get_files(log_dir, name)
        dfs = []
        for path in tqdm(files, desc=f"Loading `{name}` files..."):
            df = pd.read_csv(
                path, index_col=0, parse_dates=["wall_clock_timestamp"]
            )
            # Change the index from `asset_id` to the timestamp.
            df = df.reset_index().set_index("wall_clock_timestamp")
            hpandas.dassert_series_type_is(df["asset_id"], np.int64)
            if not isinstance(df.index, pd.DatetimeIndex):
                _LOG.info("Skipping file_name=%s", path)
                continue
            df.index = df.index.tz_convert(tz)
            # Pivot to multiple column levels.
            df = df.pivot(columns="asset_id")
            dfs.append(df)
        df = pd.concat(dfs)
        return df

    @staticmethod
    def read_logged_orders(
        log_dir: str,
    ) -> pd.DataFrame:
        """
        Parse logged orders and return as a dataframe indexed by order id.

        NOTE: Parsing logged orders takes significantly longer than reading
        logged target positions.
        """
        name = "orders"
        files = ForecastProcessor._get_files(log_dir, name)
        dfs = []
        for path in tqdm(files, desc=f"Loading `{name}` files..."):
            lines = hio.from_file(path)
            lines = lines.split("\n")
            for line in lines:
                if not line:
                    continue
                order = omorder.Order.from_string(line)
                order = order.to_dict()
                order = pd.Series(order).to_frame().T
                dfs.append(order)
        df = pd.concat(dfs)
        df = df.set_index("order_id")
        return df

    def generate_orders(
        self,
        predictions: pd.Series,
        volatility: pd.Series,
        spread: pd.Series,
        liquidate_holdings: bool,
    ) -> List[omorder.Order]:
        """
        Translate returns and volatility forecasts into a list of orders.

        :param predictions: returns forecasts
        :param volatility: volatility forecasts
        :param spread: spread forecasts
        :param liquidate_holdings: force liquidating all the current holdings
        :return: a list of orders to execute
        """
        _LOG.debug(hprint.to_str("liquidate_holdings"))
        # Convert forecasts into target positions.
        target_positions = self._compute_target_positions_in_shares(
            predictions, volatility, spread, liquidate_holdings
        )
        # Get the wall clock timestamp and internally log `target_positions`.
        wall_clock_timestamp = self._get_wall_clock_time()
        _LOG.debug("wall_clock_timestamp=%s", wall_clock_timestamp)
        self._target_positions[wall_clock_timestamp] = target_positions
        # Generate orders from target positions.
        _LOG.debug(
            "\n%s",
            hprint.frame(
                "Generating orders: timestamp=%s" % wall_clock_timestamp,
                char1="#",
            ),
        )
        # Enter position between now and the next `order_duration_in_mins` minutes.
        # Create a config for `Order`.
        timestamp_start = wall_clock_timestamp
        timestamp_end = wall_clock_timestamp + self._offset_min
        order_dict = {
            "type_": self._order_dict["order_type"],
            "creation_timestamp": wall_clock_timestamp,
            "start_timestamp": timestamp_start,
            "end_timestamp": timestamp_end,
        }
        orders = self._generate_orders(
            target_positions[["curr_num_shares", "diff_num_shares"]], order_dict
        )
        # Convert orders to a string representation and internally log.
        orders_as_str = omorder.orders_to_string(orders)
        self._orders[wall_clock_timestamp] = orders_as_str
        return orders

    # TODO(gp): mark_to_market -> mark_to_market_portfolio
    def log_state(self, mark_to_market: bool):
        _LOG.debug("log_state")
        if mark_to_market:
            self._portfolio.mark_to_market()
        # Log the state of Portfolio.
        if self._log_dir:
            self._log_state()
            self._portfolio.log_state(os.path.join(self._log_dir, "portfolio"))

    async def submit_orders(self, orders: List[omorder.Order]) -> None:
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
        # Log the entire state after submitting the orders.
        # We don't need to mark to market since `generate_orders()` takes care of
        # marking to market the Portfolio.
        mark_to_market = False
        self.log_state(mark_to_market)

    # /////////////////////////////////////////////////////////////////////////////

    @staticmethod
    def _get_files(log_dir: str, name: str) -> List[str]:
        # Find all files in the requested dir.
        dir_name = os.path.join(log_dir, name)
        pattern = "*"
        only_files = True
        use_relative_paths = True
        files = hio.listdir(dir_name, pattern, only_files, use_relative_paths)
        files.sort()
        # Add enclosing dir.
        files = [os.path.join(dir_name, file_name) for file_name in files]
        return files

    # TODO(gp): -> _log_internal_state?
    def _log_state(self) -> None:
        """
        Log the most recent state of the object.
        """
        hdbg.dassert(self._log_dir, "Must specify `log_dir` to log state.")
        #
        wall_clock_time = self._get_wall_clock_time()
        wall_clock_time_str = wall_clock_time.strftime("%Y%m%d_%H%M%S")
        filename = f"{wall_clock_time_str}.csv"
        # Log the target position.
        if self._target_positions:
            last_key, last_target_positions = self._target_positions.peek()
            last_target_positions_filename = os.path.join(
                self._log_dir, "target_positions", filename
            )
            hio.create_enclosing_dir(
                last_target_positions_filename, incremental=True
            )
            last_target_positions.to_csv(last_target_positions_filename)
        # Log the orders.
        if self._orders:
            last_key, last_orders = self._orders.peek()
            last_orders_filename = os.path.join(self._log_dir, "orders", filename)
            hio.create_enclosing_dir(last_orders_filename, incremental=True)
            hio.to_file(last_orders_filename, last_orders)

    def _compute_target_positions_in_shares(
        self,
        predictions: pd.Series,
        volatility: pd.Series,
        spread: pd.Series,
        liquidate_holdings: bool,
    ) -> pd.DataFrame:
        """
        Compute target holdings in shares.

        :param predictions: predictions indexed by `asset_id`
        :param volatility: volatility forecasts indexed by `asset_id`
        :param spread: spread forecasts indexed by `asset_id`
        :param liquidate_holdings: force liquidating all the current holdings
        :return: the df with target_positions including `diff_num_shares`
        """
        assets_and_predictions = self._prepare_data_for_optimizer(
            predictions, volatility, spread
        )
        hdbg.dassert_not_in(
            self._portfolio.CASH_ID, assets_and_predictions["asset_id"].to_list()
        )
        # Compute the target positions in cash (call the optimizer).
        # TODO(Paul): Align with ForecastEvaluator and update callers.
        # compute_target_positions_func
        # compute_target_positions_kwargs
        backend = self._optimizer_dict["backend"]
        if backend == "pomo":
            style = self._optimizer_dict["params"]["style"]
            kwargs = self._optimizer_dict["params"]["kwargs"]
            df = ocalopti.compute_target_positions_in_cash(
                assets_and_predictions,
                style=style,
                **kwargs,
            )
        elif backend == "batch_optimizer":
            import optimizer.single_period_optimization as osipeopt

            spo = osipeopt.SinglePeriodOptimizer(
                self._optimizer_dict,
                assets_and_predictions,
                restrictions=self._restrictions,
            )
            df = spo.optimize()
            _LOG.debug("df=\n%s", hpandas.df_to_str(df))
            df = df.merge(
                assets_and_predictions.set_index("asset_id")[
                    ["price", "curr_num_shares"]
                ],
                how="outer",
                left_index=True,
                right_index=True,
            )
        elif backend == "dind_optimizer":
            # Call docker optimizer stub.
            raise NotImplementedError
        elif backend == "service_optimizer":
            raise NotImplementedError
        else:
            raise ValueError("Unsupported `backend`=%s", backend)
        #
        if liquidate_holdings:
            diff_num_shares = -df["curr_num_shares"]
            _LOG.info(
                "Liquidating holdings: diff_num_shares=\n%s",
                hpandas.df_to_str(diff_num_shares),
            )
        else:
            # Convert the target positions from cash values to target share counts.
            # Round to nearest integer towards zero.
            # df["diff_num_shares"] = np.fix(df["target_trade"] / df["price"])
            diff_num_shares = df["target_notional_trade"] / df["price"]
        # Make sure the diff_num_shares are well-formed.
        diff_num_shares.replace([-np.inf, np.inf], np.nan, inplace=True)
        diff_num_shares = diff_num_shares.fillna(0)
        #
        df["diff_num_shares"] = diff_num_shares
        df["spread"] = assets_and_predictions.set_index("asset_id")["spread"]
        _LOG.debug("df=\n%s", hpandas.df_to_str(df))
        return df

    def _prepare_data_for_optimizer(
        self,
        predictions: pd.Series,
        volatility: pd.Series,
        spread: pd.Series,
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
            marked_to_market, predictions, volatility, spread
        )
        cash_id_filter = df_for_optimizer["asset_id"] == self._portfolio.CASH_ID
        df_for_optimizer.rename(columns={"value": "position"}, inplace=True)
        return df_for_optimizer[~cash_id_filter].reset_index(drop=True)

    def _get_extended_marked_to_market_df(
        self,
        predictions: pd.Series,
    ) -> pd.DataFrame:
        """
        Get portfolio `mark_to_market()` df and extend to all predictions.

        If the portfolio is initialized with the trading universe, then this
        should be a no-op.

        :param predictions: predictions indexed by `asset_id`
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

    def _normalize_series(
        self,
        series: pd.Series,
        index: pd.DatetimeIndex,
        imputation: str,
        name: str,
    ) -> pd.DataFrame:
        """
        Normalize series with `index`, NaN-filling, and df conversion.
        """
        hdbg.dassert_isinstance(series, pd.Series)
        _LOG.debug("Number of values=%i", series.size)
        _LOG.debug("Number of non-NaN values=%i", series.count())
        _LOG.debug("Number of NaN values=%i", series.isna().sum())
        # Ensure that `series` does not include the cash id.
        hdbg.dassert_not_in(self._portfolio.CASH_ID, series.index)
        # Ensure that `index` includes `series.index`.
        hdbg.dassert(series.index.difference(index).empty)
        # Extend `predictions` to `index`.
        series = series.reindex(index)
        # Set the "prediction" for cash to 1. This is for the optimizer.
        series[self._portfolio.CASH_ID] = 1
        # Impute zero for NaNs.
        if imputation == "zero":
            series = series.fillna(0.0)
        elif imputation == "mean":
            series_mean = series.mean()
            series = series.fillna(series_mean)
        else:
            raise ValueError("Invalid imputation mode")
        # Convert to a dataframe.
        df = pd.DataFrame(series)
        # Format the predictions dataframe.
        df.columns = [name]
        df.index.name = "asset_id"
        df = df.reset_index()
        _LOG.debug("df=\n%s", hpandas.df_to_str(df))
        return df

    def _merge_predictions(
        self,
        marked_to_market: pd.DataFrame,
        predictions: pd.Series,
        volatility: pd.Series,
        spread: pd.Series,
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
        predictions = self._normalize_series(
            predictions, idx, "zero", "prediction"
        )
        volatility = self._normalize_series(volatility, idx, "mean", "volatility")
        spread = self._normalize_series(spread, idx, "mean", "spread")
        # Merge current holdings and predictions.
        merged_df = marked_to_market.merge(
            predictions, on="asset_id", how="outer"
        )
        merged_df = merged_df.merge(
            volatility,
            on="asset_id",
            how="outer",
        )
        merged_df = merged_df.merge(
            spread,
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

    def _generate_orders(
        self,
        shares_df: pd.DataFrame,
        order_dict: Dict[str, Any],
    ) -> List[omorder.Order]:
        """
        Turn a series of asset_id / shares to trade into a list of orders.

        :param shares_df: dataframe indexed by `asset_id`. Contains columns
            `curr_num_shares` and `diff_num_shares`. May contain zero rows.
        :param order_dict: common parameters used to initialize `Order`
        :return: a list of nontrivial orders (i.e., no zero-share orders)
        """
        _LOG.debug("# Generate orders")
        hdbg.dassert_isinstance(order_dict, dict)
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
            diff_num_shares = self._enforce_restrictions(
                asset_id, curr_num_shares, diff_num_shares
            )
            if diff_num_shares == 0.0:
                # No need to place trades.
                continue
            order = omorder.Order(
                asset_id=asset_id,
                curr_num_shares=curr_num_shares,
                diff_num_shares=diff_num_shares,
                **order_dict,
            )
            _LOG.debug("order=%s", order.order_id)
            orders.append(order)
        _LOG.debug("Number of orders generated=%i", len(orders))
        return orders

    def _enforce_restrictions(
        self,
        asset_id: int,
        curr_num_shares: float,
        diff_num_shares: float,
    ) -> float:
        if self._restrictions is None:
            return diff_num_shares
        filter_ = self._restrictions["asset_id"] == asset_id
        restrictions = self._restrictions[filter_]
        if restrictions.empty:
            return diff_num_shares
        # Enforce "is_buy_restricted".
        if (
            restrictions.loc["is_buy_restricted"]
            and curr_num_shares >= 0
            and diff_num_shares > 0
        ):
            diff_num_shares = 0.0
        # Enforce "is_buy_cover_restricted".
        if (
            restrictions.loc["is_buy_cover_restricted"]
            and curr_num_shares < 0
            and diff_num_shares > 0
        ):
            diff_num_shares = 0.0
        # Enforce "is_sell_short_restricted".
        if (
            restrictions.loc["is_sell_short_restricted"]
            and curr_num_shares <= 0
            and diff_num_shares < 0
        ):
            diff_num_shares = 0.0
        # Enforce "is_sell_long_restricted".
        if (
            restrictions.loc["is_sell_long_restricted"]
            and curr_num_shares > 0
            and diff_num_shares < 0
        ):
            diff_num_shares = 0.0
        _LOG.warning("Enforcing restriction for asset_id=%i", asset_id)
        return diff_num_shares


# #############################################################################


def _validate_order_dict(order_dict: Dict[str, Any]) -> None:
    hdbg.dassert_isinstance(order_dict, dict)
    _ = hdict.typed_get(order_dict, "order_type", expected_type=str)
    _ = hdict.typed_get(order_dict, "order_duration_in_mins", expected_type=int)


def _validate_optimizer_dict(optimizer_dict: Dict[str, Any]) -> None:
    hdbg.dassert_isinstance(optimizer_dict, dict)
    _ = hdict.typed_get(optimizer_dict, "backend", expected_type=str)
    # target_gmv = hdict.typed_get(
    #     optimizer_dict, "target_gmv", expected_type=(float, int)
    # )