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
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


# #############################################################################
# ForecastEvaluatorFromPrices
# #############################################################################


class ForecastEvaluatorFromPrices:
    """
    Evaluate returns/volatility forecasts.
    """

    def __init__(
        self,
        price_col: str,
        volatility_col: str,
        prediction_col: str,
        *,
        spread_col: Optional[str] = None,
        buy_price_col: Optional[str] = None,
        sell_price_col: Optional[str] = None,
    ) -> None:
        """
        Construct object.

        :param price_col: price per share
            - the `price_col` is unadjusted price (no adjustment for splits,
              dividends, or volatility); it is used for marking to market and,
              unless buy/sell prices columns are also supplied, execution
              simulation
        :param volatility_col: volatility used for adjustment of forward returns
            - the `volatility_col` is used for position sizing
        :param prediction_col: prediction of volatility-adjusted returns, two
            steps ahead
            - the `prediction_col` is a prediction of vol-adjusted returns
              (presumably with volatility given by `volatility_col`)
        """
        _LOG.debug(
            hprint.to_str(
                "price_col volatility_col prediction_col spread_col buy_price_col"
                " sell_price_col"
            )
        )
        # Initialize dataframe columns.
        hdbg.dassert_isinstance(price_col, str)
        self._price_col = price_col
        #
        hdbg.dassert_isinstance(volatility_col, str)
        self._volatility_col = volatility_col
        #
        hdbg.dassert_isinstance(prediction_col, str)
        self._prediction_col = prediction_col
        # Process optional columns.
        if spread_col is not None:
            hdbg.dassert_isinstance(spread_col, str)
            _LOG.debug("Initialized with spread_col=%s", spread_col)
        self._spread_col = spread_col
        #
        if buy_price_col is not None:
            hdbg.dassert_isinstance(buy_price_col, str)
            # If `buy_price_col` is not `None`, then `sell_price_col` must also
            # not be `None`.
            hdbg.dassert_isinstance(sell_price_col, str)
            _LOG.debug("Initialized with buy_price_col=%s", buy_price_col)
        self._buy_price_col = buy_price_col
        #
        if sell_price_col is not None:
            hdbg.dassert_isinstance(sell_price_col, str)
            # If `sell_price_col` is not `None`, then `buy_price_col` must also
            # not be `None`.
            hdbg.dassert_isinstance(buy_price_col, str)
            _LOG.debug("Initialized with sell_price_col=%s", sell_price_col)
        self._sell_price_col = sell_price_col

    @staticmethod
    def load_portfolio(
        log_dir: str,
        *,
        file_name: Optional[str] = None,
        tz: str = "America/New_York",
        cast_asset_ids_to_int: bool = True,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load and process saved portfolio.

        :param log_dir: directory for loading log files of portfolio state
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
        holdings_shares = ForecastEvaluatorFromPrices._read_df(
            log_dir, "holdings_shares", file_name, tz
        )
        holdings_notional = ForecastEvaluatorFromPrices._read_df(
            log_dir, "holdings_notional", file_name, tz
        )
        executed_trades_shares = ForecastEvaluatorFromPrices._read_df(
            log_dir, "executed_trades_shares", file_name, tz
        )
        executed_trades_notional = ForecastEvaluatorFromPrices._read_df(
            log_dir, "executed_trades_notional", file_name, tz
        )
        pnl = ForecastEvaluatorFromPrices._read_df(log_dir, "pnl", file_name, tz)
        if cast_asset_ids_to_int:
            for df in [
                price,
                volatility,
                predictions,
                holdings_shares,
                holdings_notional,
                executed_trades_shares,
                executed_trades_notional,
                pnl,
            ]:
                ForecastEvaluatorFromPrices._cast_cols_to_int(df)
        dfs = {
            "price": price,
            "volatility": volatility,
            "prediction": predictions,
            "holdings_shares": holdings_shares,
            "holdings_notional": holdings_notional,
            "executed_trades_shares": executed_trades_shares,
            "executed_trades_notional": executed_trades_notional,
            "pnl": pnl,
        }
        portfolio_df = ForecastEvaluatorFromPrices._build_multiindex_df(dfs)
        #
        statistics_df = ForecastEvaluatorFromPrices._read_df(
            log_dir, "statistics", file_name, tz
        )
        return portfolio_df, statistics_df

    def save_portfolio(
        self,
        df: pd.DataFrame,
        log_dir: str,
        **kwargs,
    ) -> str:
        """
        Save portfolio state to the file system.

        The dir structure of the data output is:
        ```
        - holdings_shares
        - holdings_notional
        - executed_trades_shares
        - executed_trades_notional
        - pnl
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
        derived_dfs = self.compute_portfolio(
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
            derived_dfs["holdings_shares"], log_dir, "holdings_shares", file_name
        )
        ForecastEvaluatorFromPrices._write_df(
            derived_dfs["holdings_notional"],
            log_dir,
            "holdings_notional",
            file_name,
        )
        ForecastEvaluatorFromPrices._write_df(
            derived_dfs["executed_trades_shares"],
            log_dir,
            "executed_trades_shares",
            file_name,
        )
        ForecastEvaluatorFromPrices._write_df(
            derived_dfs["executed_trades_notional"],
            log_dir,
            "executed_trades_notional",
            file_name,
        )
        ForecastEvaluatorFromPrices._write_df(
            derived_dfs["pnl"], log_dir, "pnl", file_name
        )
        ForecastEvaluatorFromPrices._write_df(
            derived_dfs["stats"], log_dir, "statistics", file_name
        )
        return file_name

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
        dfs = self.compute_portfolio(
            df,
            **kwargs,
        )
        #
        act = []
        round_precision = 6
        precision = 2
        act.append("# holdings_shares=")
        act.append(
            hpandas.df_to_str(
                dfs["holdings_shares"].round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append("# holdings_notional=")
        act.append(
            hpandas.df_to_str(
                dfs["holdings_notional"].round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append("# executed_trades_shares=")
        act.append(
            hpandas.df_to_str(
                dfs["executed_trades_shares"].round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append("# executed_trades_notional=")
        act.append(
            hpandas.df_to_str(
                dfs["executed_trades_notional"].round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append("# pnl=")
        act.append(
            hpandas.df_to_str(
                dfs["pnl"].round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act.append("# statistics=")
        act.append(
            hpandas.df_to_str(
                dfs["stats"].round(round_precision),
                num_rows=None,
                precision=precision,
            )
        )
        act = "\n".join(act)
        return act

    # //////////////////////////////////////////////////////////////////////////////

    def compute_portfolio(
        self,
        df: pd.DataFrame,
        *,
        style: str = "cross_sectional",
        quantization: str = "no_quantization",
        liquidate_at_end_of_day: bool = True,
        initialize_beginning_of_day_trades_to_zero: bool = True,
        adjust_for_splits: bool = False,
        reindex_like_input: bool = False,
        burn_in_bars: int = 3,
        burn_in_days: int = 0,
        compute_extended_stats: bool = False,
        asset_id_to_share_decimals: Optional[Dict[int, int]] = None,
        **kwargs,
    ) -> Dict[str, pd.DataFrame]:
        """
        Compute target positions, PnL, and portfolio stats.

        :param df: multiindexed dataframe with predictions, price, volatility
        :param style: belongs to
            - "cross_sectional": cross-sectionally normalize predictions,
              possibly remove a portion of the bulk of the distribution,
              and allocate a target GMV
            - "longitudinal": normalize and threshold predictions
              longitudinally, allocating an equal dollar risk to each name
              independently
        :param quantization: indicate whether to round to nearest share / lot
        :param liquidate_at_end_of_day: force holdings to zero at the last
            trade if true (otherwise hold overnight)
        :param adjust_for_splits: account for stock splits in considering
            overnight holdings
        :param reindex_like_input: output dataframes to have the same input as
            `df` (e.g., including any weekends or values outside of the
            `start_time`-`end_time` range). If `False`, only return dataframes
            indexed by the inferred "active index" of datetimes
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
        :return: dictionary of portfolio dataframes, with keys
            ["holdings_shares", "holdings_notional", "executed_trades_shares",
             "executed_trades_notional", "pnl", "stats"]
        """
        _LOG.debug("df=\n%s", hpandas.df_to_str(df, print_shape_info=True))
        self._validate_df(df)
        # Record index in case we reindex the results.
        if reindex_like_input:
            idx = df.index
        else:
            idx = None
        # Trim to indices with prices and beginning of forecast availability.
        df = self._apply_trimming(df)
        # Compute target positions (in dollars).
        target_holdings_notional = self._compute_target_holdings_notional(
            df,
            style,
            **kwargs,
        )
        # Compute holdings (in shares).
        # TODO(Paul): Expose these two parameters.
        ffill_limit = 4
        holdings_shares = self._compute_holdings_shares(
            df,
            target_holdings_notional,
            quantization,
            liquidate_at_end_of_day,
            adjust_for_splits,
            ffill_limit,
            asset_id_to_share_decimals,
        )
        # Compute cash inflows/outflows from trades.
        executed_trades_shares = self._compute_executed_trades_shares(
            df,
            holdings_shares,
            initialize_beginning_of_day_trades_to_zero,
        )
        executed_trades_notional = self._compute_executed_trades_notional(
            df,
            executed_trades_shares,
            ffill_limit,
        )
        # Compute notional positions.
        holdings_notional = self._compute_holdings_notional(df, holdings_shares)
        # Compute PnL.
        pnl = self._compute_pnl(df, holdings_notional, executed_trades_notional)
        # Compute statistics.
        stats = self._compute_stats(
            df,
            holdings_notional,
            executed_trades_notional,
            pnl,
            compute_extended_stats,
        )
        #
        derived_dfs = {
            "holdings_shares": holdings_shares,
            "holdings_notional": holdings_notional,
            "executed_trades_shares": executed_trades_shares,
            "executed_trades_notional": executed_trades_notional,
            "pnl": pnl,
            "stats": stats,
        }
        # Apply burn-in and reindex like input.
        return self._apply_burn_in_and_reindex(
            df,
            derived_dfs,
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
        :return: multiindexed portfolio dataframe, stats dataframe
        """
        derived_dfs = self.compute_portfolio(
            df,
            **kwargs,
        )
        dfs = {
            "price": df[self._price_col],
            "volatility": df[self._volatility_col],
            "prediction": df[self._prediction_col],
            "holdings_shares": derived_dfs["holdings_shares"],
            "holdings_notional": derived_dfs["holdings_notional"],
            "executed_trades_shares": derived_dfs["executed_trades_shares"],
            "executed_trades_notional": derived_dfs["executed_trades_notional"],
            "pnl": derived_dfs["pnl"],
        }
        if self._spread_col is not None:
            dfs["spread"] = df[self._spread_col]
        portfolio_df = ForecastEvaluatorFromPrices._build_multiindex_df(dfs)
        return portfolio_df, derived_dfs["stats"]

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

    # /////////////////////////////////////////////////////////////////////////////

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
        :return: `df` trimmed down to:
          - required and possibly optional columns
          - "active" bars (bars where at least one instrument has an end-of-bar
            price)
          - first index with both a returns prediction and a volatility
        """
        _LOG.debug("df.shape=%s", str(df.shape))
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
        _LOG.debug("cols=%s", cols)
        active_index = cofinanc.infer_active_bars(df[self._price_col])
        # Drop rows with no prices (this is an approximate way to handle weekends,
        # market holidays, and shortened trading sessions).
        df = df.reindex(index=active_index)
        _LOG.debug("after active_index: df.shape=%s", df.shape)
        # Drop indices with prices that precede any returns prediction or
        # volatility computation.
        first_valid_prediction_index = df[
            self._prediction_col
        ].first_valid_index()
        hdbg.dassert_is_not(first_valid_prediction_index, None)
        _LOG.debug(hprint.to_str("first_valid_prediction_index"))
        #
        first_valid_volatility_index = df[
            self._volatility_col
        ].first_valid_index()
        hdbg.dassert_is_not(first_valid_volatility_index, None)
        _LOG.debug(hprint.to_str("first_valid_volatility_index"))
        #
        first_valid_index = max(
            first_valid_prediction_index, first_valid_volatility_index
        )
        df = df.loc[first_valid_index:]
        _LOG.debug("df.shape=%s", str(df.shape))
        _LOG.debug("trimmed df=\n%s", hpandas.df_to_str(df))
        return df

    def _compute_target_holdings_notional(
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
        :return: end-of-bar indexed target notional positions to trade into
            over the next bar
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
            target_notional_positions = (
                cofinanc.compute_target_positions_cross_sectionally(
                    prediction_df,
                    volatility_df,
                    **kwargs,
                )
            )
        elif style == "longitudinal":
            target_notional_positions = (
                cofinanc.compute_target_positions_longitudinally(
                    prediction_df,
                    volatility_df,
                    spread=spread_df,
                    **kwargs,
                )
            )
        else:
            raise ValueError("Unsupported `style`=%s", style)
        self._validate_target_position_df(
            target_notional_positions, prediction_df
        )
        return target_notional_positions

    def _compute_holdings_shares(
        self,
        df: pd.DataFrame,
        target_notional_positions: pd.DataFrame,
        quantization: str,
        liquidate_at_end_of_day: bool,
        adjust_for_splits: bool,
        ffill_limit: int,
        asset_id_to_share_decimals: Optional[Dict[int, int]],
    ) -> pd.DataFrame:
        """
        Convert next-bar [dollar] positions to end-of-bar [share] holdings.

        :param df: as in `compute_portfolio()`
        :param target_notional_positions: from `_compute_target_holdings_notional()`
        :param quantization: as in `compute_portfolio()`
        :param liquidate_at_end_of_day: as in `compute_portfolio()`
        :param adjust_for_splits: as in `compute_portfolio()`
        :param ffill_limit: as in `compute_portfolio()`
        :return: end-of-bar indexed holdings in shares (holdings held at the
            end of the bar)
        """
        mark_to_market_price = ForecastEvaluatorFromPrices._get_df(
            df, self._price_col
        )
        # Compute target (next bar) holdings based on prices available at
        # decision time.
        target_holdings_shares = target_notional_positions.divide(
            mark_to_market_price
        )
        # Quantize holdings (e.g., nearest share).
        target_holdings_shares = cofinanc.quantize_holdings(
            target_holdings_shares, quantization, asset_id_to_share_decimals
        )
        # Adjust holdings for end-of-day and splits. Convert from next-bar
        # desired holdings to end-of-bar realized (assuming perfect fills)
        # holdings.
        ideal_holdings_shares = cofinanc.adjust_holdings_for_overnight(
            target_holdings_shares.shift(1),
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
            holdings_shares = cofinanc.adjust_holdings_for_underfills(
                ideal_holdings_shares,
                mark_to_market_price,
                buy_price,
                sell_price,
            )
        else:
            holdings_shares = ideal_holdings_shares
        return holdings_shares

    def _compute_executed_trades_shares(
        self,
        df: pd.DataFrame,
        holdings_shares: pd.DataFrame,
        initialize_beginning_of_day_trades_to_zero: bool,
    ) -> pd.DataFrame:
        """
        Compute trade inflows and outflows.

        :param df: as in `compute_portfolio()`
        :param holdings_shares: from `_compute_holdings_shares()`
        :param initialize_beginning_of_day_trades_to_zero: whether to force
            beginning-of-day trades to be zero. Set to `True` to take into
            account the impact of corporate actions on overnight holdings.
        :return: end-of-bar indexed trades in shares
        """
        # Compute trades as the difference in (share) holdings.
        executed_trades_shares = holdings_shares.subtract(
            holdings_shares.shift(1), fill_value=0
        )
        _LOG.debug(
            "`executed_trades_shares pre-adjusted`=\n%s",
            hpandas.df_to_str(executed_trades_shares),
        )
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
            executed_trades_shares.loc[bod_timestamps["timestamp"]] *= 0
        _LOG.debug(
            "`executed_trades_shares adjusted`=\n%s",
            hpandas.df_to_str(executed_trades_shares),
        )
        return executed_trades_shares

    def _compute_executed_trades_notional(
        self,
        df: pd.DataFrame,
        executed_trades_shares: pd.DataFrame,
        ffill_limit: int,
    ) -> pd.DataFrame:
        """
        Compute trade inflows and outflows.

        :param df: as in `compute_portfolio()`
        :param executed_trades_shares:
        :return: end-of-bar indexed notional trades
        """
        execution_price_df = self._compute_execution_prices(
            df, executed_trades_shares, ffill_limit
        )
        executed_trades_notional = executed_trades_shares.multiply(
            execution_price_df.ffill(limit=ffill_limit)
        )
        return executed_trades_notional

    def _compute_execution_prices(
        self,
        df: pd.DataFrame,
        executed_trades_shares: pd.DataFrame,
        ffill_limit: int,
    ) -> pd.DataFrame:
        """
        Compute execution prices from buy/sell price columns if available.

        :param df: as in `compute_portfolio()`
        :param executed_trades_shares:
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
            # Market in last bar.
            buy_price_df = cofinanc.replace_end_of_day_values(
                buy_price_df, mark_to_market_price_df
            )
            sell_price_df = cofinanc.replace_end_of_day_values(
                sell_price_df, mark_to_market_price_df
            )
            execution_price_df = cofinanc.apply_execution_prices_to_trades(
                executed_trades_shares,
                buy_price_df,
                sell_price_df,
            )
        else:
            execution_price_df = mark_to_market_price_df
        return execution_price_df

    def _compute_holdings_notional(
        self,
        df: pd.DataFrame,
        holdings_shares: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compute positions in dollars from share holdings.

        :param df: as in `compute_portfolio()`
        :param holdings_shares: from `_compute_holdings_shares()`
        :return: end-of-bar indexed dataframe of holdings valued using
            `self.price_col` of `df`
        """
        mark_to_market_price = ForecastEvaluatorFromPrices._get_df(
            df, self._price_col
        )
        holdings_notional = holdings_shares.multiply(mark_to_market_price)
        return holdings_notional

    def _compute_pnl(
        self,
        df: pd.DataFrame,
        holdings_notional: pd.DataFrame,
        executed_trades_notional: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compute PnL from dollar positions and dollar inflows/outflows.

        :param df: as in `compute_portfolio()`
        :param holdings_notional: from `_compute_holdings_notional()`
        :param executed_trades_notional: from `_compute_executed_trades_notional()`
        :return: end-of-bar indexed dataframe of per-instrument dollar PnL
        """
        _ = df
        pnl = holdings_notional.subtract(
            holdings_notional.shift(1), fill_value=0
        ).subtract(executed_trades_notional, fill_value=0)
        return pnl

    def _compute_stats(
        self,
        df: pd.DataFrame,
        holdings_notional: pd.DataFrame,
        executed_trades_notional: pd.DataFrame,
        pnl: pd.DataFrame,
        compute_extended_stats: bool,
    ) -> pd.DataFrame:
        """
        Compute PnL from dollar positions and dollar inflows/outflows.

        :param df: as in `compute_portfolio()`
        :param holdings_notional: from `_compute_holdings_notional()`
        :param executed_trades_notional: from `_compute_flows()`
        :param pnl: from `_compute_pnl()`
        :param compute_extended_stats: forwarded to
            `cofinanc.compute_bar_metrics()`
        :return: per-bar metrics from `cofinanc.compute_bar_metrics()`
        """
        spread_df = None
        if self._spread_col is not None:
            spread_df = ForecastEvaluatorFromPrices._get_df(df, self._spread_col)
        stats = cofinanc.compute_bar_metrics(
            holdings_notional,
            -executed_trades_notional,
            pnl,
            spread_df,
            compute_extended_stats,
        )
        return stats

    def _apply_burn_in_and_reindex(
        self,
        df: pd.DataFrame,
        derived_dfs: Dict[str, pd.DataFrame],
        burn_in_bars: int,
        burn_in_days: int,
        input_idx: Optional[None],
    ) -> Dict[str, pd.DataFrame]:
        # Remove initial bars.
        if burn_in_bars > 0:
            for key, value in derived_dfs.items():
                derived_dfs[key] = value.iloc[burn_in_bars:]
        if burn_in_days > 0:
            # TODO(Paul): Consider making this more efficient (and less
            # awkward).
            date_idx = df.groupby(lambda x: x.date()).count().index
            hdbg.dassert_lt(burn_in_days, date_idx.size)
            first_date = pd.Timestamp(date_idx[burn_in_days], tz=df.index.tz)
            _LOG.info("Initial date after burn-in=%s", first_date)
            for key, value in derived_dfs.items():
                derived_dfs[key] = value.loc[first_date:]
        # Possibly reindex dataframes.
        if input_idx is not None:
            for key, value in derived_dfs.items():
                derived_dfs[key] = value.reindex(input_idx)
        return derived_dfs


# #############################################################################


def cross_check_portfolio_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the PnL using multiple generally equivalent calculations.

    Computations may legitimately differ in some cases due to overnight
    holdings, corporate actions, and special beginning-of-day/end-of-day
    settings.

    Computations may also differ if trades were calculation using separate
    execution prices (`buy_price_col`, `sell_price_col`).

    :param df: `portfolio_df` output of `ForecastEvaluatorFromPrices.annotate_forecasts()`
    :return: multiindexed dataframe with PnL correlations
    """
    # Reference PnL (nominal).
    reference_pnl = df["pnl"]
    # PnL computed from notional position changes and notional in/outflows.
    holdings_notional_diff_minus_trades_notional = (
        df["holdings_notional"].diff().subtract(df["executed_trades_notional"])
    )
    # PnL computed from share counts and price differences.
    holdings_shares_times_price_diff = (
        df["holdings_shares"].shift(1).multiply(df["price"].diff())
    )
    # PnL computed from notional position changes and recomputed notional trades.
    holdings_notional_minus_trade_times_price = (
        df["holdings_notional"]
        .diff()
        .subtract(df["price"].multiply(df["holdings_shares"].diff()))
    )
    # PnL computed from notional positions and percentage price changes.
    holdings_notional_times_pct_price_change = (
        df["holdings_notional"].shift(1).multiply(df["price"].pct_change())
    )
    # Organize PnL calculations into a multiindexed dataframe.
    pnl_dict = {
        "reference_pnl": reference_pnl,
        "holdings_notional_diff_minus_trades_notional": holdings_notional_diff_minus_trades_notional,
        "holdings_times_price_diff": holdings_shares_times_price_diff,
        "holdings_notional_minus_trade_times_price": holdings_notional_minus_trade_times_price,
        "holdings_notional_times_pct_price_change": holdings_notional_times_pct_price_change,
    }
    pnl_df = pd.concat(pnl_dict.values(), axis=1, keys=pnl_dict.keys())
    pnl_df = pnl_df.swaplevel(i=0, j=1, axis=1)
    # Compute per-instrument correlations of various PnL calculations.
    pnl_corrs = {}
    for col in pnl_df.columns.levels[0]:
        pnl_corrs[col] = pnl_df[col].corr()
    pnl_corr_df = pd.concat(pnl_corrs.values(), keys=pnl_corrs.keys())
    return pnl_corr_df


def normalize_portfolio_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adapt the `ForecastEvaluatorFromPrices` portfolio df to `Portfolio`'s.
    """
    hpandas.dassert_time_indexed_df(
        df, allow_empty=False, strictly_increasing=True
    )
    hdbg.dassert_is_subset(
        [
            "holdings_shares",
            "holdings_notional",
            "executed_trades_shares",
            "executed_trades_notional",
            "pnl",
        ],
        df.columns.levels[0].to_list(),
    )
    #
    holdings = df["holdings_shares"]
    holdings_marked_to_market = df["holdings_notional"]
    flows = -df["executed_trades_notional"]
    pnl = df["pnl"]
    #
    normalized_df = pd.concat(
        {
            "holdings": holdings,
            "holdings_marked_to_market": holdings_marked_to_market,
            "flows": flows,
            "pnl": pnl,
        },
        axis=1,
    )
    return normalized_df