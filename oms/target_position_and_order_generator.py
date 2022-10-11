"""
Import as:

import oms.target_position_and_order_generator as omstpog
"""

import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from tqdm.autonotebook import tqdm

import core.config as cconfig
import core.key_sorted_ordered_dict as cksoordi
import helpers.hdbg as hdbg
import helpers.hdict as hdict
import helpers.hio as hio
import helpers.hobject as hobject
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import oms.call_optimizer as ocalopti
import oms.cc_optimizer_utils as occoputi
import oms.order as omorder
import oms.portfolio as omportfo

_LOG = logging.getLogger(__name__)


class TargetPositionAndOrderGenerator(hobject.PrintableMixin):
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
        share_quantization: str,
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
        #
        self._share_quantization = share_quantization

    def __str__(self) -> str:
        """
        Return the most recent state of the ForecastProcessor as a string.
        """
        txt = []
        # <...target_position_and_order_generator at 0x>
        txt.append(hprint.to_object_repr(self))
        # TODO(Paul): Make this more like `Portfolio` logging.
        txt.append("# last target positions=")
        if self._target_positions:
            _, target_positions = self._target_positions.peek()
            target_positions_str = hpandas.df_to_str(target_positions)
        else:
            target_positions_str = "None"
        txt.append(target_positions_str)
        #
        txt.append("# last orders=")
        if self._orders:
            _, orders_str = self._orders.peek()
        else:
            orders_str = "None"
        txt.append(orders_str)
        #
        act = "\n".join(txt)
        return act

    @staticmethod
    def load_target_positions(
        log_dir: str,
        *,
        tz: str = "America/New_York",
        rename_col_map: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Parse logged `target_position` dataframes.

        :return a dataframe indexed by datetimes and with two column levels
        """
        name = "target_positions"
        files = TargetPositionAndOrderGenerator._get_files(log_dir, name)
        dfs = []
        for path in tqdm(files, desc=f"Loading `{name}` files..."):
            df = pd.read_csv(
                path, index_col=0, parse_dates=["wall_clock_timestamp"]
            )
            # Change the index from `asset_id` to the timestamp.
            df = df.reset_index().set_index("wall_clock_timestamp")
            # TODO(Dan): Research why column names are being incorrect sometimes
            #  and save the data with the proper names.
            if rename_col_map:
                df = df.rename(columns=rename_col_map)
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
    def load_orders(
        log_dir: str,
    ) -> pd.DataFrame:
        """
        Parse logged orders and return as a dataframe indexed by order id.

        NOTE: Parsing logged orders takes significantly longer than reading
        logged target positions.
        """
        name = "orders"
        files = TargetPositionAndOrderGenerator._get_files(log_dir, name)
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

    def compute_target_positions_and_generate_orders(
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
        target_positions = self._compute_target_holdings_shares(
            predictions, volatility, spread, liquidate_holdings
        )
        orders = self._generate_orders_wrapper(target_positions)
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
            _, last_target_positions = self._target_positions.peek()
            last_target_positions_filename = os.path.join(
                self._log_dir, "target_positions", filename
            )
            hio.create_enclosing_dir(
                last_target_positions_filename, incremental=True
            )
            last_target_positions.to_csv(last_target_positions_filename)
        # Log the orders.
        if self._orders:
            _, last_orders = self._orders.peek()
            last_orders_filename = os.path.join(self._log_dir, "orders", filename)
            hio.create_enclosing_dir(last_orders_filename, incremental=True)
            hio.to_file(last_orders_filename, last_orders)

    def _compute_target_holdings_shares(
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
        if backend == "cc_pomo":
            market_info = self._portfolio.broker.market_info
            market_info_keys = list(market_info.keys())
            _LOG.debug("market_info keys=%s", market_info_keys)
            asset_ids_to_decimals = {
                key: market_info[key]["amount_precision"]
                for key in market_info_keys
            }
        else:
            asset_ids_to_decimals = None
        if backend == "pomo" or "cc_pomo":
            style = self._optimizer_dict["params"]["style"]
            kwargs = self._optimizer_dict["params"]["kwargs"]
            df = ocalopti.compute_target_holdings_and_trades_notional(
                assets_and_predictions,
                style=style,
                **kwargs,
            )
            df = ocalopti.convert_target_holdings_and_trades_to_shares_and_adjust_notional(
                df,
                quantization=self._share_quantization,
                asset_id_to_decimals=asset_ids_to_decimals,
            )
            if backend == "cc_pomo":
                # Verify that all orders are above the notional limit.
                #  Note: orders that are below the minimal amount of asset
                #  for the exchange are modified to go slightly above the limit.
                df = occoputi.apply_cc_limits(df, self._portfolio.broker, self._log_dir)
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
                    ["price", "holdings_shares"]
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
        if liquidate_holdings:
            target_trades_shares = -df["holdings_shares"]
            target_trades_notional = -df["holdings_notional"]
            _LOG.info(
                "Liquidating holdings: target_trades_shares=\n%s",
                hpandas.df_to_str(target_trades_shares),
            )
            _LOG.info(
                "Liquidating holdings: target_trades_notional=\n%s",
                hpandas.df_to_str(target_trades_notional),
            )
            df["target_trades_shares"] = target_trades_shares
            df["target_trades_notional"] = target_trades_notional
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
        hpandas.dassert_indices_equal(predictions, volatility, allow_series=True)
        marked_to_market = self._get_extended_marked_to_market_df(predictions)
        # Combine the portfolio `marked_to_market` dataframe with the predictions.
        df_for_optimizer = self._merge_predictions(
            marked_to_market, predictions, volatility, spread
        )
        cash_id_filter = df_for_optimizer["asset_id"] == self._portfolio.CASH_ID
        df_for_optimizer.rename(
            columns={"value": "holdings_notional"}, inplace=True
        )
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
        marked_to_market.rename(
            columns={
                "curr_num_shares": "holdings_shares",
                "value": "holdings_notional",
            },
            inplace=True,
        )
        # TODO(Paul): Rename columns here for now.
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
        hpandas.dassert_indices_equal(predictions, volatility, allow_series=True)
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
            "Number of NaNs in `holdings_shares` post-merge=`%i`",
            merged_df["holdings_shares"].isna().sum(),
        )
        merged_df = merged_df.convert_dtypes()
        merged_df = merged_df.fillna(0.0)
        _LOG.debug("After merge: merged_df=\n%s", hpandas.df_to_str(merged_df))
        return merged_df

    def _generate_orders_wrapper(
        self, target_positions: pd.DataFrame
    ) -> List[omorder.Order]:
        """
        Convert target positions into orders.
        """
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
            target_positions[["holdings_shares", "target_trades_shares"]],
            order_dict,
        )
        # Convert orders to a string representation and internally log.
        orders_as_str = omorder.orders_to_string(orders)
        self._orders[wall_clock_timestamp] = orders_as_str
        return orders

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
            ("holdings_shares", "target_trades_shares"), shares_df.columns
        )
        orders: List[omorder.Order] = []
        for asset_id, shares_row in shares_df.iterrows():
            curr_num_shares = shares_row["holdings_shares"]
            diff_num_shares = shares_row["target_trades_shares"]
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


def _validate_order_dict(order_dict: Dict[str, Any]) -> None:
    hdbg.dassert_isinstance(order_dict, dict)
    _ = hdict.typed_get(order_dict, "order_type", expected_type=str)
    _ = hdict.typed_get(order_dict, "order_duration_in_mins", expected_type=int)


def _validate_optimizer_dict(optimizer_dict: Dict[str, Any]) -> None:
    hdbg.dassert_isinstance(optimizer_dict, dict)
    _ = hdict.typed_get(optimizer_dict, "backend", expected_type=str)