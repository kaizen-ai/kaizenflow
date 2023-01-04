"""
Import as:

import optimizer.forecast_evaluator_with_optimizer as optfewo
"""
import logging
from typing import Dict, Optional, Tuple

import pandas as pd
from tqdm.autonotebook import tqdm

import core.finance as cofinanc
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import optimizer.single_period_optimization as osipeopt

_LOG = logging.getLogger(__name__)


class ForecastEvaluatorWithOptimizer:
    """
    Evaluate returns/volatility forecasts.
    """

    def __init__(
        self,
        price_col: str,
        volatility_col: str,
        prediction_col: str,
        optimizer_config_dict: dict,
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
        _LOG.debug(hprint.to_str("price_col volatility_col prediction_col"))
        # Initialize dataframe columns.
        hdbg.dassert_isinstance(price_col, str)
        self._price_col = price_col
        #
        hdbg.dassert_isinstance(volatility_col, str)
        self._volatility_col = volatility_col
        #
        hdbg.dassert_isinstance(prediction_col, str)
        self._prediction_col = prediction_col
        #
        self._optimizer_config_dict = optimizer_config_dict

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
                log_level=logging.INFO,
            )
        )
        act.append("# holdings_notional=")
        act.append(
            hpandas.df_to_str(
                dfs["holdings_notional"].round(round_precision),
                num_rows=None,
                precision=precision,
                log_level=logging.INFO,
            )
        )
        act.append("# executed_trades_shares=")
        act.append(
            hpandas.df_to_str(
                dfs["executed_trades_shares"].round(round_precision),
                num_rows=None,
                precision=precision,
                log_level=logging.INFO,
            )
        )
        act.append("# executed_trades_notional=")
        act.append(
            hpandas.df_to_str(
                dfs["executed_trades_notional"].round(round_precision),
                num_rows=None,
                precision=precision,
                log_level=logging.INFO,
            )
        )
        act.append("# pnl=")
        act.append(
            hpandas.df_to_str(
                dfs["pnl"].round(round_precision),
                num_rows=None,
                precision=precision,
                log_level=logging.INFO,
            )
        )
        act.append("# statistics=")
        act.append(
            hpandas.df_to_str(
                dfs["stats"].round(round_precision),
                num_rows=None,
                precision=precision,
                log_level=logging.INFO,
            )
        )
        act = "\n".join(act)
        return act

    def compute_portfolio(
        self,
        df: pd.DataFrame,
        *,
        quantization: str = "no_quantization",
        liquidate_at_end_of_day: bool = True,
        initialize_beginning_of_day_trades_to_zero: bool = True,
        # adjust_for_splits: bool = False,
        reindex_like_input: bool = False,
        burn_in_bars: int = 0,
        burn_in_days: int = 0,
        compute_extended_stats: bool = False,
        asset_id_to_share_decimals: Optional[Dict[int, int]] = None,
        **kwargs,
    ) -> Dict[str, pd.DataFrame]:
        _LOG.debug("df=\n%s", hpandas.df_to_str(df, print_shape_info=True))
        self._validate_df(df)
        # Record index in case we reindex the results.
        if reindex_like_input:
            idx = df.index
        else:
            idx = None
        # Trim to indices with prices and beginning of forecast availability.
        df = self._apply_trimming(df)
        # Prepare to process the DAG df row by row.
        iter_ = enumerate(df.iterrows())
        iter_idx = df.index
        num_rows = df.shape[0]
        # Track holdings and trades by timestamp.
        holdings_shares_dict = {}
        holdings_notional_dict = {}
        executed_trades_shares_dict = {}
        executed_trades_notional_dict = {}
        # Initialize holdings and trades at zero.
        # TODO(Paul): support non-zero initialization of holdings.
        initial_timestamp = df.index[0]
        asset_ids = df.columns.levels[1]
        initial_conditions = pd.Series(0, asset_ids, name=initial_timestamp)
        holdings_shares_dict[initial_timestamp] = initial_conditions.rename(
            "holdings_shares"
        )
        holdings_notional_dict[initial_timestamp] = initial_conditions.rename(
            "holdings_notional"
        )
        executed_trades_shares_dict[
            initial_timestamp
        ] = initial_conditions.rename("executed_trades_shares")
        executed_trades_notional_dict[
            initial_timestamp
        ] = initial_conditions.rename("executed_trades_notional")
        bod_timestamps = cofinanc.retrieve_beginning_of_day_timestamps(
            df[self._price_col]
        )
        eod_timestamps = cofinanc.retrieve_end_of_day_timestamps(
            df[self._price_col]
        )
        # Process the DAG row by row.
        for idx, (timestamp, dag_data) in tqdm(iter_, total=num_rows):
            if idx + 1 < num_rows:
                next_timestamp = iter_idx[idx + 1]
            else:
                next_timestamp = None
            _LOG.debug(
                "Processing timestamp=%s; next_timestamp=%s",
                timestamp,
                next_timestamp,
            )
            next_timestamp_is_eod = False
            if (
                next_timestamp is not None
                and eod_timestamps.loc[next_timestamp.date()][0] == next_timestamp
            ):
                next_timestamp_is_eod = True
            next_timestamp_is_bod = False
            if (
                next_timestamp is not None
                and bod_timestamps.loc[next_timestamp.date()][0] == next_timestamp
            ):
                next_timestamp_is_bod = True
            dag_slice = self._extract_and_normalize_slice(dag_data)
            # Get the holdings in shares corresponding to the current DAG row.
            holdings_shares = holdings_shares_dict[timestamp]
            # Compute notional value of current holdings.
            holdings_notional = self._compute_holdings_notional(
                dag_slice, holdings_shares
            )
            holdings_notional_dict[timestamp] = holdings_notional
            # Compute the notional value of the trades that executed over the
            # last bar.
            executed_trades_shares = executed_trades_shares_dict[timestamp]
            executed_trades_notional = self._compute_executed_trades_notional(
                dag_slice, executed_trades_shares
            )
            executed_trades_notional_dict[timestamp] = executed_trades_notional
            # Compute notional value of target holdings.
            liquidate_holdings = liquidate_at_end_of_day and next_timestamp_is_eod
            targets_df = self._optimize(
                dag_slice,
                holdings_shares,
                holdings_notional,
                quantization,
                asset_id_to_share_decimals,
                liquidate_holdings,
            )
            # If the time step is not the last one, set the next-period
            # share holdings and executed trades in shares (assuming orders
            # are fully filled).
            if next_timestamp is not None:
                if (
                    next_timestamp_is_bod
                    and initialize_beginning_of_day_trades_to_zero
                ):
                    holdings_shares_dict[next_timestamp] = holdings_shares.rename(
                        "holdings_shares"
                    )
                    executed_trades_shares_dict[next_timestamp] = pd.Series(
                        0, asset_ids, name="executed_trades_shares"
                    )
                else:
                    holdings_shares_dict[next_timestamp] = targets_df[
                        "target_holdings_shares"
                    ].rename("holdings_shares")
                    executed_trades_shares_dict[next_timestamp] = (
                        targets_df["target_trades_shares"]
                    ).rename("executed_trades_shares")
        # Create the portfolio dataframe.
        holdings_shares = pd.DataFrame(holdings_shares_dict).T
        holdings_notional = pd.DataFrame(holdings_notional_dict).T
        executed_trades_shares = pd.DataFrame(executed_trades_shares_dict).T
        executed_trades_notional = pd.DataFrame(executed_trades_notional_dict).T
        pnl = holdings_notional.subtract(
            holdings_notional.shift(1), fill_value=0
        ).subtract(executed_trades_notional, fill_value=0)
        stats = cofinanc.compute_bar_metrics(
            holdings_notional,
            -executed_trades_notional,
            pnl,
            compute_extended_stats=compute_extended_stats,
        )
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
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Compute target positions, PnL, and portfolio stats.

        :param df: multiindexed dataframe with predictions, price, volatility
        """
        derived_dfs = self.compute_portfolio(df, **kwargs)
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
        portfolio_df = ForecastEvaluatorWithOptimizer._build_multiindex_df(dfs)
        return portfolio_df, derived_dfs["stats"]

    @staticmethod
    def _build_multiindex_df(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        portfolio_df = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        return portfolio_df

    def _compute_holdings_notional(
        self,
        df_slice: pd.DataFrame,
        holdings_shares: pd.Series,
    ) -> pd.Series:
        price = df_slice["price"]
        holdings_notional = (holdings_shares * price).rename("holdings_notional")
        return holdings_notional

    def _compute_executed_trades_notional(
        self,
        df_slice: pd.DataFrame,
        executed_trades_shares: pd.Series,
    ) -> pd.Series:
        price = df_slice["price"]
        # Compute the notional value of the trades that executed over the
        # last bar.
        executed_trades_notional = (executed_trades_shares * price).rename(
            "executed_trades_notional"
        )
        return executed_trades_notional

    def _optimize(
        self,
        df_slice: pd.DataFrame,
        holdings_shares: pd.Series,
        holdings_notional: pd.Series,
        quantization,
        asset_id_to_share_decimals,
        liquidate_holdings,
    ) -> pd.Series:
        # Prepare data for the optimizer.
        holdings_df = pd.concat([holdings_shares, holdings_notional], axis=1)
        targets_input_df = pd.concat([df_slice, holdings_df], axis=1)
        input_df = targets_input_df.reset_index()
        input_df = input_df.rename(columns={"index": "asset_id"})
        _LOG.debug("input_df cols=%s", input_df.columns)
        # Optimize.
        output_df = osipeopt.optimize(
            self._optimizer_config_dict,
            input_df,
            quantization=quantization,
            asset_id_to_share_decimals=asset_id_to_share_decimals,
            liquidate_holdings=liquidate_holdings,
        )
        return output_df

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
        df = df[cols]
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

    def _extract_and_normalize_slice(self, df: pd.DataFrame) -> pd.DataFrame:
        # Extract the required columns from the DAG.
        dag_slice = df.unstack().T[
            [self._price_col, self._volatility_col, self._prediction_col]
        ]
        # Normalize the DAG slice column names.
        dag_slice.index.name = "asset_id"
        dag_slice = dag_slice.rename(
            columns={
                self._price_col: "price",
                self._volatility_col: "volatility",
            }
        )
        return dag_slice

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
        # if input_idx is not None:
        #     for key, value in derived_dfs.items():
        #         derived_dfs[key] = value.reindex(input_idx)
        return derived_dfs
