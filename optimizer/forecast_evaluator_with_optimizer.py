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
        # asset_id_col: str,
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
        # hdbg.dassert_isinstance(asset_id_col, str)
        # self._asset_id_col = asset_id_col,
        #
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

    def annotate_forecasts(
        self,
        df: pd.DataFrame,
        *,
        quantization: str = "no_quantization",
        # liquidate_at_end_of_day: bool = True,
        # initialize_beginning_of_day_trades_to_zero: bool = True,
        compute_extended_stats: bool = False,
        asset_id_to_decimals: Optional[Dict[int, int]] = None,
        **kwargs,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Compute target positions, PnL, and portfolio stats.

        :param df: multiindexed dataframe with predictions, price, volatility
        :param quantization: indicate whether to round to nearest share / lot
        :param liquidate_at_end_of_day: force holdings to zero at the last
            trade if true (otherwise hold overnight)
        :return: dictionary of portfolio dataframes, with keys
            ["holdings_shares", "holdings_notional", "executed_trades_shares",
             "executed_trades_notional", "pnl", "stats"]
        """
        _LOG.debug("df=\n%s", hpandas.df_to_str(df, print_shape_info=True))
        self._validate_df(df)
        # Compute target positions (in dollars).
        iter_ = enumerate(df.iterrows())
        num_rows = df.shape[0]
        asset_ids = df.columns.levels[1]
        targets_dict = {}
        # Track holdings and trades by timestamp.
        holdings_shares_dict = {}
        holdings_notional_dict = {}
        executed_trades_shares_dict = {}
        executed_trades_notional_dict = {}
        # Initialize holdings and trades at zero.
        # TODO(Paul): support non-zero initialization of holdings.
        initial_timestamp = df.index[0]
        initial_conditions = pd.Series(0, asset_ids, name=initial_timestamp)
        holdings_shares_dict[initial_timestamp] = initial_conditions
        holdings_notional_dict[initial_timestamp] = initial_conditions
        executed_trades_shares_dict[initial_timestamp] = initial_conditions
        executed_trades_notional_dict[initial_timestamp] = initial_conditions
        # Process the DAG row by row.
        for idx, (timestamp, dag_data) in tqdm(iter_, total=num_rows):
            # Extract the required columns from the DAG.
            dag_slice = dag_data.unstack().T[
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
            # Get the holdings in shares corresponding to the current DAG row.
            holdings_shares = holdings_shares_dict[timestamp].rename(
                "holdings_shares"
            )
            price = dag_slice["price"]
            # Compute the notional value of the share holdings.
            holdings_notional = (holdings_shares * price).rename(
                "holdings_notional"
            )
            holdings_notional_dict[timestamp] = holdings_notional
            # Compute the notional value of the trades that executed over the
            # last bar.
            executed_trades_notional = (
                executed_trades_shares_dict[timestamp] * price
            ).rename("executed_trades_notional")
            executed_trades_notional_dict[timestamp] = executed_trades_notional
            # Prepare data for the optimizer.
            holdings_df = pd.concat([holdings_shares, holdings_notional], axis=1)
            targets_input_df = pd.concat([dag_slice, holdings_df], axis=1)
            input_df = targets_input_df.reset_index()
            input_df = input_df.rename(columns={"index": "asset_id"})
            _LOG.debug("input_df cols=%s", input_df.columns)
            # Optimize.
            targets_output_df = osipeopt.optimize(
                self._optimizer_config_dict, input_df
            )
            _LOG.debug("optimization output=%s", targets_output_df)
            # The raw optimizer output is notional-only. Now we convert back
            # to shares, quantize the shares, and then recompute the notional
            # post-quantization.
            target_holdings_shares = (
                targets_output_df["target_holdings_notional"] / price
            ).rename("target_holdings_shares")
            # Quantize the shares.
            target_holdings_shares = cofinanc.quantize_shares(
                target_holdings_shares,
                quantization,
                asset_id_to_decimals,
            )
            # Recompute notional from quantized shares.
            target_holdings_notional = (target_holdings_shares * price).rename(
                "target_holdings_notional"
            )
            target_holdings_notional_quantization_difference = (
                targets_output_df["target_holdings_notional"]
                - target_holdings_notional
            )
            _LOG.debug(
                "target_holdings_notional_quantization_difference=%s",
                target_holdings_notional_quantization_difference,
            )
            # Recompute notional trades from quantized trades.
            target_trades_shares = (
                target_holdings_shares - holdings_shares
            ).rename("target_trades_shares")
            target_trades_notional = (target_trades_shares * price).rename(
                "target_trades_notional"
            )
            # Complete the target position dataframe.
            quantized_targets_output_df = pd.concat(
                [
                    target_holdings_shares,
                    target_holdings_notional,
                    target_trades_shares,
                    target_trades_notional,
                ],
                axis=1,
            )
            targets_df = pd.concat(
                [targets_input_df, quantized_targets_output_df], axis=1
            )
            targets_dict[timestamp] = targets_df
            _LOG.debug("targets_df=%s", targets_df)
            # If the time step is not the last one, set the next-period
            # share holdings and executed trades in shares (assuming orders
            # are fully filled).
            if idx + 1 < num_rows:
                next_timestamp = df.index[idx + 1]
                holdings_shares_dict[
                    next_timestamp
                ] = target_holdings_shares.rename("holdings_shares")
                executed_trades_shares_dict[next_timestamp] = (
                    target_holdings_shares - holdings_shares
                ).rename("executed_trades_shares")
        # Create the portfolio dataframe.
        holdings_shares = pd.DataFrame(holdings_shares_dict).T
        holdings_notional = pd.DataFrame(holdings_notional_dict).T
        executed_trades_shares = pd.DataFrame(executed_trades_shares_dict).T
        executed_trades_notional = pd.DataFrame(executed_trades_notional_dict).T
        pnl = holdings_notional.subtract(
            holdings_notional.shift(1), fill_value=0
        ).subtract(executed_trades_notional, fill_value=0)
        portfolio_df = pd.concat(
            {
                "holdings_shares": holdings_shares,
                "holdings_notional": holdings_notional,
                "executed_trades_shares": executed_trades_shares,
                "executed_trades_notional": executed_trades_notional,
                "pnl": pnl,
            },
            axis=1,
        )
        portfolio_stats_df = cofinanc.compute_bar_metrics(
            portfolio_df["holdings_notional"],
            -portfolio_df["executed_trades_notional"],
            portfolio_df["pnl"],
            compute_extended_stats=compute_extended_stats,
        )
        return portfolio_df, portfolio_stats_df, targets_dict

    def _validate_df(self, df: pd.DataFrame) -> None:
        hpandas.dassert_time_indexed_df(
            df, allow_empty=True, strictly_increasing=True
        )
        hdbg.dassert_eq(df.columns.nlevels, 2)
        hdbg.dassert_is_subset(
            [self._price_col, self._volatility_col, self._prediction_col],
            df.columns.levels[0].to_list(),
        )