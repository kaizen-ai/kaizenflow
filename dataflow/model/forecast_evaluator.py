"""
Import as:

import dataflow.model.forecast_evaluator as dtfmofoeva
"""

import logging
from typing import Optional, Tuple

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hprint as hprint

_LOG = logging.getLogger(__name__)


class ForecastEvaluator:
    """
    Evaluate returns/volatility forecasts.
    """

    def __init__(
        self,
        *,
        returns_col: str,
        volatility_col: str,
        prediction_col: str,
    ) -> None:
        """
        Initialize column names.

        Note:
        - the `prediction_col` is a prediction of vol-adjusted returns
        - the `returns_col` is not vol-adjusted
        - by passing `volatility_col` explicitly, we can easily calculate PnL
          at a specified GMV and under a dollar neutrality constraint

        :param returns_col: percentage change of underlying price (not
            volatility adjusted)
        :param volatility_col: volatility used for adjustment of forward returns
        :param prediction_col: prediction of volatility-adjusted returns, two
            steps ahead
        """
        self._returns_col = returns_col
        self._volatility_col = volatility_col
        self._prediction_col = prediction_col

    def to_str(
        self,
        df: pd.DataFrame,
        *,
        target_gmv: Optional[float] = None,
        dollar_neutral: bool = False,
    ) -> str:
        """
        Return the state of the Portfolio in terms of the holdings as a string.

        :param target_gmv: as in `compute_portfolio`
        :param dollar_neutral: as in `compute_portfolio`
        """
        target_positions, pnl, stats = self.compute_portfolio(
            df, target_gmv=target_gmv, dollar_neutral=dollar_neutral
        )
        act = []
        precision = 2
        act.append(
            "# holdings marked to market=\n%s"
            % hprint.dataframe_to_str(
                target_positions.shift(1), precision=precision
            )
        )
        act.append(
            "# pnl=\n%s"
            % hprint.dataframe_to_str(
                pnl,
                precision=precision,
            )
        )
        act.append(
            "# statistics=\n%s"
            % hprint.dataframe_to_str(stats, precision=precision)
        )
        act = "\n".join(act)
        return act

    def compute_portfolio(
        self,
        df: pd.DataFrame,
        *,
        target_gmv: Optional[float] = None,
        dollar_neutral: bool = False,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Compute target positions, PnL, and portfolio stats.

        :param df: multiindexed dataframe with predictions, returns, volatility
        :param target_gmv: if `None`, then GMV may float
        :param dollar_neutral: enforce a hard dollar neutrality constraint
        :return: (target_positions, pnl, stats)
        """
        # Compute first appoximation of dollar value risk-adjusted positions.
        returns_predictions = ForecastEvaluator._get_df(df, self._prediction_col)
        volatility = ForecastEvaluator._get_df(df, self._volatility_col)
        # Units of target positions = cash.
        target_positions = returns_predictions.divide(volatility)
        _LOG.debug(
            "target_positions=\n%s" % hprint.dataframe_to_str(target_positions)
        )
        if dollar_neutral:
            hdbg.dassert_lt(
                1,
                target_positions.shape[1],
                "Unable to enforce dollar neutrality with a single asset.",
            )
            net_asset_value = target_positions.mean(axis=1)
            _LOG.debug(
                "net asset value=\n%s" % hprint.dataframe_to_str(net_asset_value)
            )
            target_positions = target_positions.subtract(net_asset_value, axis=0)
            _LOG.debug(
                "dollar neutral target_positions=\n%s"
                % hprint.dataframe_to_str(target_positions)
            )
        if target_gmv is not None:
            hdbg.dassert_lt(0, target_gmv)
            l1_norm = target_positions.abs().sum(axis=1, min_count=1)
            scale_factor = l1_norm / target_gmv
            _LOG.debug(
                "scale factor=\n%s" % hprint.dataframe_to_str(scale_factor)
            )
            target_positions = target_positions.divide(scale_factor, axis=0)
            _LOG.debug(
                "gmv scaled target_positions=\n%s"
                % hprint.dataframe_to_str(target_positions)
            )
        returns = ForecastEvaluator._get_df(df, self._returns_col)
        pnl = target_positions.shift(2).multiply(returns)
        # Stats
        stats = self._compute_statistics(target_positions, pnl)
        return target_positions, pnl, stats

    def _compute_statistics(
        self,
        target_positions: pd.DataFrame,
        pnl: pd.DataFrame,
    ) -> pd.DataFrame:
        positions = target_positions.shift(1)
        gmv = positions.abs().sum(axis=1, min_count=1)
        nav = positions.mean(axis=1)
        portfolio_pnl = pnl.sum(axis=1, min_count=1)
        stats = pd.DataFrame(
            {
                "net_asset_holdings": nav,
                "gross_exposure": gmv,
                "pnl": portfolio_pnl,
            }
        )
        return stats

    @staticmethod
    def _get_df(df: pd.DataFrame, col: str) -> pd.DataFrame:
        hdbg.dassert_in(col, df.columns)
        return df[col]
