"""
Import as:

import dataflow.model.forecast_evaluator_from_prices as dtfmfefrpr
"""
import collections
import logging
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd

import core.finance as cofinanc
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hpandas as hpandas

_LOG = logging.getLogger(__name__)


class ForecastEvaluatorFromPrices:
    """
    Evaluate returns/volatility forecasts.
    """

    def __init__(
        self,
        price_col: str,
        volatility_col: str,
        prediction_col: str,
        spread_col: Optional[str] = None,
        buy_price_col: Optional[str] = None,
        sell_price_col: Optional[str] = None,
    ) -> None:
        """
        Initialize column names.

        Note:
        - the `price_col` is unadjusted price (no adjustment for splits,
          dividends, or volatility); it is used for marking to market and,
          unless buy/sell prices columns are also supplied, execution
          simulation
        - the `volatility_col` is used for position sizing
        - the `prediction_col` is a prediction of vol-adjusted returns
          (presumably with volatility given by `volatility_col`)

        :param price_col: price per share
        :param volatility_col: volatility used for adjustment of forward returns
        :param prediction_col: prediction of volatility-adjusted returns, two
            steps ahead
        """
        # Initialize dataframe columns.
        hdbg.dassert_isinstance(price_col, str)
        self._price_col = price_col
        hdbg.dassert_isinstance(volatility_col, str)
        self._volatility_col = volatility_col
        hdbg.dassert_isinstance(prediction_col, str)
        self._prediction_col = prediction_col
        # Process optional columns.
        if spread_col is not None:
            hdbg.dassert_isinstance(spread_col, str)
            _LOG.debug("Initialized with spread_col=%s", spread_col)
        self._spread_col = spread_col
        if buy_price_col is not None:
            hdbg.dassert_isinstance(buy_price_col, str)
            # If `buy_price_col` is not `None`, then `sell_price_col` must also
            # not be `None`.
            hdbg.dassert_isinstance(sell_price_col, str)
            _LOG.debug("Initialized with buy_price_col=%s", buy_price_col)
        self._buy_price_col = buy_price_col
        if sell_price_col is not None:
            hdbg.dassert_isinstance(sell_price_col, str)
            # If `sell_price_col` is not `None`, then `buy_price_col` must also
            # not be `None`.
            hdbg.dassert_isinstance(buy_price_col, str)
            _LOG.debug("Initialized with sell_price_col=%s", sell_price_col)
        self._sell_price_col = sell_price_col

    @staticmethod
    def read_portfolio(
        log_dir: str,
        *,
        file_name: Optional[str] = None,
        tz: str = "America/New_York",
        cast_asset_ids_to_int: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Read and process logged portfolio.

        :param log_dir: directory for reading log files of portfolio state
        :param file_name: if `None`, find and use the latest
        :param tz: timezone to apply to timestamps (this information is lost in
            the logging/reading round trip)
        :param cast_asset_ids_to_int: cast asset ids from integers-as-strings
            to integers (the data type is lost in the logging/reading round
            trip)
        """
        if file_name is None:
            dir_name = os.path.join(log_dir, "price")
            pattern = "*"
            only_files = True
            use_relative_paths = True
            files = hio.listdir(dir_name, pattern, only_files, use_relative_paths)
            files.sort()
            file_name = files[-1]
            _LOG.info("`file_name`=%s", file_name)
        price = ForecastEvaluatorFromPrices._read_df(
            log_dir, "price", file_name, tz
        )
        volatility = ForecastEvaluatorFromPrices._read_df(
            log_dir, "volatility", file_name, tz
        )
        predictions = ForecastEvaluatorFromPrices._read_df(
            log_dir, "prediction", file_name, tz
        )
        holdings = ForecastEvaluatorFromPrices._read_df(
            log_dir, "holdings", file_name, tz
        )
        positions = ForecastEvaluatorFromPrices._read_df(
            log_dir, "position", file_name, tz
        )
        flows = ForecastEvaluatorFromPrices._read_df(
            log_dir, "flow", file_name, tz
        )
        pnl = ForecastEvaluatorFromPrices._read_df(log_dir, "pnl", file_name, tz)
        if cast_asset_ids_to_int:
            for df in [
                price,
                volatility,
                predictions,
                holdings,
                positions,
                flows,
                pnl,
            ]:
                ForecastEvaluatorFromPrices._cast_cols_to_int(df)
        dfs = {
            "price": price,
            "volatility": volatility,
            "prediction": predictions,
            "holdings": holdings,
            "position": positions,
            "flow": flows,
            "pnl": pnl,
        }
        portfolio_df = ForecastEvaluatorFromPrices._build_multiindex_df(dfs)
        statistics_df = ForecastEvaluatorFromPrices._read_df(
            log_dir, "statistics", file_name, tz
        )
        return portfolio_df, statistics_df

    def to_str(
        self,
        df: pd.DataFrame,
        **kwargs,
    ) -> str:
        """
        Return the state of the Portfolio as a string.

        :param df: as in `compute_portfolio`
        :param kwargs: forwarded to `compute_portfolio()`
        :return: portfolio state (rounded) as a string
        """
        holdings, positions, flows, pnl, stats = self.compute_portfolio(
            df,
            **kwargs,
        )
        act = []
        round_precision = 6
        precision = 2
        act.append(
            "# holdings=\n%s"
            % hpandas.df_to_str(
                holdings.round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# holdings marked to market=\n%s"
            % hpandas.df_to_str(
                positions.round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# flows=\n%s"
            % hpandas.df_to_str(
                flows.round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# pnl=\n%s"
            % hpandas.df_to_str(
                pnl.round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append(
            "# statistics=\n%s"
            % hpandas.df_to_str(
                stats.round(round_precision), num_rows=None, precision=precision
            )
        )
        act = "\n".join(act)
        return act

    def log_portfolio(
        self,
        df: pd.DataFrame,
        log_dir: str,
        **kwargs,
    ) -> str:
        """
        Log portfolio state to the file system.

        The dir structure of the data output is:
        ```
        - flow
        - holdings
        - pnl
        - position
        - prediction
        - price
        - statistics
        - volatility
        ```

        :param df: as in `compute_portfolio()`
        :param log_dir: directory for writing log files of portfolio state
        :param kwargs: forwarded to `compute_portfolio()`
        :return: name of log files with timestamp
        """
        hdbg.dassert(log_dir, "Must specify `log_dir` to log portfolio.")
        holdings, position, flow, pnl, statistics = self.compute_portfolio(
            df,
            **kwargs,
        )
        last_timestamp = df.index[-1]
        hdbg.dassert_isinstance(last_timestamp, pd.Timestamp)
        last_timestamp_str = last_timestamp.strftime("%Y%m%d_%H%M%S")
        file_name = f"{last_timestamp_str}.csv"
        #
        ForecastEvaluatorFromPrices._write_df(
            df[self._price_col], log_dir, "price", file_name
        )
        ForecastEvaluatorFromPrices._write_df(
            df[self._volatility_col], log_dir, "volatility", file_name
        )
        ForecastEvaluatorFromPrices._write_df(
            df[self._prediction_col], log_dir, "prediction", file_name
        )
        ForecastEvaluatorFromPrices._write_df(
            holdings, log_dir, "holdings", file_name
        )
        ForecastEvaluatorFromPrices._write_df(
            position, log_dir, "position", file_name
        )
        ForecastEvaluatorFromPrices._write_df(flow, log_dir, "flow", file_name)
        ForecastEvaluatorFromPrices._write_df(pnl, log_dir, "pnl", file_name)
        ForecastEvaluatorFromPrices._write_df(
            statistics, log_dir, "statistics", file_name
        )
        return file_name

    def compute_portfolio(
        self,
        df: pd.DataFrame,
        *,
        style: str = "cross_sectional",
        quantization: str = "no_quantization",
        liquidate_at_end_of_day: bool = True,
        adjust_for_splits: bool = False,
        reindex_like_input: bool = False,
        burn_in_bars: int = 3,
        burn_in_days: int = 0,
        compute_extended_stats: bool = False,
        **kwargs,
    ) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
    ]:
        """
        Compute target positions, PnL, and portfolio stats.

        :param df: multiindexed dataframe with predictions, price, volatility
        :param reindex_like_input: output dataframes to have the same input as
            `df` (e.g., including any weekends or values outside of the
            `start_time`-`end_time` range)
        :param style: belongs to
            - "cross_sectional": cross-sectionally normalize predictions,
              possibly remove a portion of the bulk of the distribution,
              and allocate a target GMV
            - "longitudinal": normalize and threshold predictions
              longitudinally, allocating an equal dollar risk to each name
              independently
        :param quantization: indicate whether to round to nearest share, lot
        :param liquidate_at_end_of_day: force holdings to zero at the last
            trade if true (otherwise hold overnight)
        :param adjust_for_splits: account for stock splits in considering
            overnight holdings
        :param reindex_like_input: if `False`, only return dataframes indexed
            by the inferred "active index" of datetimes
        :param burn_in_bars: number of leading bars to trim (to remove warm-up
            artifacts)
        :param burn_in_days: number of leading days to trim (to remove warm-up
            artifacts). Applied independently of `burn_in_bars`.
        :param compute_extended_stats: compute additional stats beyond the
            five "core" stats.
        :param kwargs: forwarded to either
            `compute_target_positions_cross_sectionally()` or
            `compute_target_positions_longitudinally()` depending upon the
            value of `style`
        :return: (holdings, position, flow, pnl, stats)
        """
        self._validate_df(df)
        # Record index in case we reindex the results.
        if reindex_like_input:
            idx = df.index
        else:
            idx = None
        # Trim to indices with prices and beginning of forecast availability.
        df = self._apply_trimming(df)
        # Compute target positions (in dollars).
        target_positions = self._compute_target_positions(
            df,
            style,
            **kwargs,
        )
        # Compute holdings (in shares).
        # TODO(Paul): Expose these two parameters.
        ffill_limit = 4
        holdings = self._compute_holdings(
            df,
            target_positions,
            quantization,
            liquidate_at_end_of_day,
            adjust_for_splits,
            ffill_limit,
        )
        # Compute cash inflows/outflows from trades.
        initialize_beginning_of_day_trades_to_zero = True
        flows = self._compute_flows(
            df,
            holdings,
            initialize_beginning_of_day_trades_to_zero,
            ffill_limit,
        )
        # Compute positions (in dollars).
        positions = self._compute_positions(df, holdings)
        # Compute PnL.
        pnl = self._compute_pnl(df, positions, flows)
        # Compute statistics.
        stats = self._compute_stats(
            df, positions, flows, pnl, compute_extended_stats
        )
        # Apply burn-in and reindex like input.
        return self._apply_burn_in_and_reindex(
            df,
            holdings,
            positions,
            flows,
            pnl,
            stats,
            burn_in_bars,
            burn_in_days,
            idx,
        )

    def annotate_forecasts(
        self,
        df: pd.DataFrame,
        **kwargs,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Wraps `compute_portfolio()`, returns a single multiindexed dataframe.

        :param df: as in `compute_portfolio()`
        :param kwargs: forwarded to `compute_portfolio()`
        :return: multiindexed dataframe with level-0 columns
            "returns", "volatility", "prediction", "position", "pnl"
        """
        holdings, position, flow, pnl, statistics_df = self.compute_portfolio(
            df,
            **kwargs,
        )
        dfs = {
            "price": df[self._price_col],
            "volatility": df[self._volatility_col],
            "prediction": df[self._prediction_col],
            "holdings": holdings,
            "position": position,
            "flow": flow,
            "pnl": pnl,
        }
        if self._spread_col is not None:
            dfs["spread"] = df[self._spread_col]
        portfolio_df = ForecastEvaluatorFromPrices._build_multiindex_df(dfs)
        return portfolio_df, statistics_df

    def get_cols(self) -> List[str]:
        """
        Return the names of the price, volatility, and prediction columns.
        """
        cols = [
            self._price_col,
            self._volatility_col,
            self._prediction_col,
        ]
        optional_cols = [
            self._spread_col,
            self._buy_price_col,
            self._sell_price_col,
        ]
        for col in optional_cols:
            if col is not None:
                cols = cols + [col]
        return cols

    def compute_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute data counts by column grouped by time.
        """
        self._validate_df(df)

        def _compute_counts(df: pd.DataFrame, col: str) -> pd.DataFrame:
            return df[col].groupby(lambda x: x.time()).count()

        count_to_col = {
            "price_count": self._price_col,
            "volatility_count": self._price_col,
            "prediction_count": self._price_col,
            "spread_count": self._spread_col,
            "buy_price_count": self._buy_price_col,
            "sell_price_count": self._sell_price_col,
        }
        dfs = collections.OrderedDict()
        for key, value in count_to_col.items():
            if value is not None:
                dfs[key] = _compute_counts(df, value)
        count_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        return count_df

    @staticmethod
    def _build_multiindex_df(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        portfolio_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        return portfolio_df

    @staticmethod
    def _cast_cols_to_int(
        df: pd.DataFrame,
    ) -> None:
        # If integers are converted to floats and then strings, then upon
        # being read they must be cast to floats before being cast to ints.
        df.columns = df.columns.astype("float64").astype("int64")

    @staticmethod
    def _write_df(
        df: pd.DataFrame,
        log_dir: str,
        name: str,
        file_name: str,
    ) -> None:
        path = os.path.join(log_dir, name, file_name)
        hio.create_enclosing_dir(path, incremental=True)
        df.to_csv(path)

    @staticmethod
    def _read_df(
        log_dir: str,
        name: str,
        file_name: str,
        tz: str,
    ) -> pd.DataFrame:
        path = os.path.join(log_dir, name, file_name)
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = df.index.tz_convert(tz)
        return df

    @staticmethod
    def _get_df(df: pd.DataFrame, col: str) -> pd.DataFrame:
        hdbg.dassert_in(col, df.columns)
        return df[col]

    def _validate_target_position_df(
        self, target_positions: pd.DataFrame, predictions: pd.DataFrame
    ) -> None:
        hpandas.dassert_time_indexed_df(
            target_positions, allow_empty=True, strictly_increasing=True
        )
        hdbg.dassert_eq(target_positions.columns.nlevels, 1)
        hpandas.dassert_axes_equal(target_positions, predictions)

    def _validate_df(self, df: pd.DataFrame) -> None:
        hpandas.dassert_time_indexed_df(
            df, allow_empty=True, strictly_increasing=True
        )
        hdbg.dassert_eq(df.columns.nlevels, 2)
        hdbg.dassert_is_subset(
            [self._price_col, self._volatility_col, self._prediction_col],
            df.columns.levels[0].to_list(),
        )

    def _apply_trimming(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Trim `df` according to ATH, weekends, missing data.

        :param df: as in `compute_portfolio()`
        :return: `df` trimmed down to
          - required and possibly optional columns
          - "active" bars (bars where at least one instrument has an end-of-bar
            price)
          - first index with both a returns prediction and a volatility
        """
        # Restrict to required columns.
        cols = [self._price_col, self._volatility_col, self._prediction_col]
        optional_cols = [
            self._spread_col,
            self._buy_price_col,
            self._sell_price_col,
        ]
        for col in optional_cols:
            if col is not None:
                cols += [col]
        df = df[cols]
        active_index = cofinanc.infer_active_bars(df[self._price_col])
        # Drop rows with no prices (this is an approximate way to handle weekends,
        # market holidays, and shortened trading sessions).
        df = df.reindex(index=active_index)
        # Drop indices with prices that precede any returns prediction or
        # volatility computation.
        first_valid_prediction_index = df[
            self._prediction_col
        ].first_valid_index()
        hdbg.dassert_is_not(first_valid_prediction_index, None)
        #
        first_valid_volatility_index = df[
            self._volatility_col
        ].first_valid_index()
        hdbg.dassert_is_not(first_valid_volatility_index, None)
        #
        first_valid_index = max(
            first_valid_prediction_index, first_valid_volatility_index
        )
        df = df.loc[first_valid_index:]
        _LOG.debug("trimmed df=\n%s", hpandas.df_to_str(df))
        return df

    def _compute_target_positions(
        self,
        df: pd.DataFrame,
        style: str,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Compute target positions using returns and volatility predictions.

        :param df: as in `compute_portfolio()`
        :param style: "cross-sectional" or "longitudinal"
        :param kwargs: parameters to forward (depending upon `style`)
        :return: end-of-bar indexed target positions to trade into over the
            next bar
        """
        # Extract prediction and volatility dataframes.
        prediction_df = ForecastEvaluatorFromPrices._get_df(
            df, self._prediction_col
        )
        _LOG.debug("prediction_df=\n%s", hpandas.df_to_str(prediction_df))
        volatility_df = ForecastEvaluatorFromPrices._get_df(
            df, self._volatility_col
        )
        _LOG.debug("volatility_df=\n%s", hpandas.df_to_str(volatility_df))
        spread_df = None
        if self._spread_col is not None:
            spread_df = ForecastEvaluatorFromPrices._get_df(df, self._spread_col)
        # The values of`target_positions` represent cash values.
        if style == "cross_sectional":
            target_positions = (
                cofinanc.compute_target_positions_cross_sectionally(
                    prediction_df,
                    volatility_df,
                    **kwargs,
                )
            )
        elif style == "longitudinal":
            target_positions = cofinanc.compute_target_positions_longitudinally(
                prediction_df,
                volatility_df,
                spread=spread_df,
                **kwargs,
            )
        else:
            raise ValueError("Unsupported `style`=%s", style)
        self._validate_target_position_df(target_positions, prediction_df)
        return target_positions

    def _compute_holdings(
        self,
        df: pd.DataFrame,
        target_positions: pd.DataFrame,
        quantization: str,
        liquidate_at_end_of_day: bool,
        adjust_for_splits: bool,
        ffill_limit: int,
    ) -> pd.DataFrame:
        """
        Convert next-bar [dollar] positions to end-of-bar [share] holdings.

        :param df: as in `compute_portfolio()`
        :param target_positions: from `_compute_target_positions()`
        :param quantization: as in `compute_portfolio()`
        :param liquidate_at_end_of_day: as in `compute_portfolio()`
        :param adjust_for_splits: as in `compute_portfolio()`
        :param ffill_limit: as in `compute_portfolio()`
        :return: end-of-bar indexed holdings (holdings held at the end of the
            bar)
        """
        mark_to_market_price = ForecastEvaluatorFromPrices._get_df(
            df, self._price_col
        )
        # Compute target (next bar) holdings based on prices available at
        # decision time.
        target_holdings = target_positions.divide(mark_to_market_price)
        # Quantize holdings (e.g., nearest share).
        target_holdings = cofinanc.quantize_holdings(
            target_holdings, quantization
        )
        # Adjust holdings for end-of-day and splits. Convert from next-bar
        # desired holdings to end-of-bar realized (assuming perfect fills)
        # holdings.
        ideal_holdings = cofinanc.adjust_holdings_for_overnight(
            target_holdings.shift(1),
            mark_to_market_price,
            liquidate_at_end_of_day,
            adjust_for_splits,
            ffill_limit,
        )
        # If buy/sell prices (possibly with NaNs, indicating no fills) are
        # available, adjust the holdings for underfills (carry positions over).
        if self._buy_price_col is not None:
            buy_price = ForecastEvaluatorFromPrices._get_df(
                df, self._buy_price_col
            )
            sell_price = ForecastEvaluatorFromPrices._get_df(
                df, self._sell_price_col
            )
            holdings = cofinanc.adjust_holdings_for_underfills(
                ideal_holdings,
                mark_to_market_price,
                buy_price,
                sell_price,
            )
        else:
            holdings = ideal_holdings
        return holdings

    def _compute_flows(
        self,
        df: pd.DataFrame,
        holdings: pd.DataFrame,
        initialize_beginning_of_day_trades_to_zero: bool,
        ffill_limit: int,
    ) -> pd.DataFrame:
        """
        Compute trade inflows and outflows.

        :param df: as in `compute_portfolio()`
        :param holdings: from `_compute_holdings()`
        :param initialize_beginning_of_day_trades_to_zero: whether to force
            beginning-of-day trades to be zero. Set to `True` to take into
            account the impact of corporate actions on overnight holdings.
        :return: end-of-bar indexed cash inflows/outflows. A "buy" is considered
            a cash outflow (cash goes out to purchase shares, and so is
            negative) and conversely a "sell" is considered a cash inflow
            (shares are sold for cash, and so the flow is positive).
        """
        # Compute trades as the difference in (share) holdings.
        trades = holdings.subtract(holdings.shift(1), fill_value=0)
        _LOG.debug("`difference in holdings`=\n%s", hpandas.df_to_str(trades))
        # In equity markets with corporate actions, the previous end-of-day
        # share counts may differ from the beginning-of-day share counts even
        # though no trades have taken place. This can be remedied by resetting
        # the beginning-of-day trades to zero.
        if initialize_beginning_of_day_trades_to_zero:
            mark_to_market_price_df = ForecastEvaluatorFromPrices._get_df(
                df, self._price_col
            )
            bod_timestamps = cofinanc.retrieve_beginning_of_day_timestamps(
                mark_to_market_price_df
            )
            # Set overnight trades to zero.
            trades.loc[bod_timestamps["timestamp"]] *= 0
        execution_price_df = self._compute_execution_prices(
            df, holdings, ffill_limit
        )
        flows = -trades.multiply(execution_price_df.ffill(limit=ffill_limit))
        return flows

    def _compute_execution_prices(
        self,
        df: pd.DataFrame,
        holdings: pd.DataFrame,
        ffill_limit: int,
    ) -> pd.DataFrame:
        """
        Compute execution prices from buy/sell price columns if available.

        :param df: as in `compute_portfolio()`
        :param holdings: from `_compute_holdings()`
        :return: end-of-bar indexed dataframe of prices to use to value cash
            outflows/inflows for the buying/selling of shares
        """
        mark_to_market_price_df = ForecastEvaluatorFromPrices._get_df(
            df, self._price_col
        ).ffill(limit=ffill_limit)
        # Use separate buy/sell prices if available.
        if self._buy_price_col is not None and self._sell_price_col is not None:
            buy_price_df = ForecastEvaluatorFromPrices._get_df(
                df, self._buy_price_col
            )
            sell_price_df = ForecastEvaluatorFromPrices._get_df(
                df, self._sell_price_col
            )
            # Generate a signal indicating when we should buy or sell.
            trade_signal = holdings.diff()
            # Market in last bar.
            buy_price_df = cofinanc.replace_end_of_day_values(
                buy_price_df, mark_to_market_price_df
            )
            sell_price_df = cofinanc.replace_end_of_day_values(
                sell_price_df, mark_to_market_price_df
            )
            execution_price_df = cofinanc.apply_execution_prices_to_trades(
                trade_signal,
                buy_price_df,
                sell_price_df,
            )
        else:
            execution_price_df = mark_to_market_price_df
        return execution_price_df

    def _compute_positions(
        self,
        df: pd.DataFrame,
        holdings: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compute positions in dollars from share holdings.

        :param df: as in `compute_portfolio()`
        :param holdings: from `_compute_holdings()`
        :return: end-of-bar indexed dataframe of holdings valued using
            `self.price_col` of `df`
        """
        mark_to_market_price = ForecastEvaluatorFromPrices._get_df(
            df, self._price_col
        )
        positions = holdings.multiply(mark_to_market_price)
        return positions

    def _compute_pnl(
        self,
        df: pd.DataFrame,
        positions: pd.DataFrame,
        flows: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compute PnL from dollar positions and dollar inflows/outflows.

        :param df: as in `compute_portfolio()`
        :param positions: from `_compute_positions()`
        :param flows: from `_compute_flows()`
        :return: end-of-bar indexed dataframe of per-instrument dollar PnL
        """
        _ = df
        pnl = positions.subtract(positions.shift(1), fill_value=0).add(
            flows, fill_value=0
        )
        return pnl

    def _compute_stats(
        self,
        df: pd.DataFrame,
        positions: pd.DataFrame,
        flows: pd.DataFrame,
        pnl: pd.DataFrame,
        compute_extended_stats: bool,
    ) -> pd.DataFrame:
        """
        Compute PnL from dollar positions and dollar inflows/outflows.

        :param df: as in `compute_portfolio()`
        :param positions: from `_compute_positions()`
        :param flows: from `_compute_flows()`
        :param pnl: from `_compute_pnl()`
        :param compute_extended_stats: forwarded to
            `cofinanc.compute_bar_metrics()`
        :return: per-bar metrics from `cofinanc.compute_bar_metrics()`
        """
        spread_df = None
        if self._spread_col is not None:
            spread_df = ForecastEvaluatorFromPrices._get_df(df, self._spread_col)
        stats = cofinanc.compute_bar_metrics(
            positions, flows, pnl, spread_df, compute_extended_stats
        )
        return stats

    def _apply_burn_in_and_reindex(
        self,
        df: pd.DataFrame,
        holdings: pd.DataFrame,
        positions: pd.DataFrame,
        flows: pd.DataFrame,
        pnl: pd.DataFrame,
        stats: pd.DataFrame,
        burn_in_bars: int,
        burn_in_days: int,
        input_idx: Optional[None],
    ) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
    ]:
        # Remove initial bars.
        if burn_in_bars > 0:
            holdings = holdings.iloc[burn_in_bars:]
            positions = positions.iloc[burn_in_bars:]
            flows = flows.iloc[burn_in_bars:]
            pnl = pnl.iloc[burn_in_bars:]
            stats = stats.iloc[burn_in_bars:]
        if burn_in_days > 0:
            # TODO(Paul): Consider making this more efficient (and less
            # awkward).
            date_idx = df.groupby(lambda x: x.date()).count().index
            hdbg.dassert_lt(burn_in_days, date_idx.size)
            first_date = pd.Timestamp(date_idx[burn_in_days], tz=df.index.tz)
            _LOG.info("Initial date after burn-in=%s", first_date)
            holdings = holdings.loc[first_date:]
            positions = positions.loc[first_date:]
            flows = flows.loc[first_date:]
            pnl = pnl.loc[first_date:]
            stats = stats.loc[first_date:]
        # Possibly reindex dataframes.
        if input_idx is not None:
            holdings = holdings.reindex(input_idx)
            positions = positions.reindex(input_idx)
            flows = flows.reindex(input_idx)
            pnl = pnl.reindex(input_idx)
            stats = stats.reindex(input_idx)
        return holdings, positions, flows, pnl, stats
