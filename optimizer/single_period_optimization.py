"""
Import as:

import optimizer.single_period_optimization as osipeopt
"""

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

import core.config as cconfig
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import optimizer.base as opbase
import optimizer.hard_constraints as oharcons
import optimizer.soft_constraints as osofcons

# Equivalent to `import cvxpy as cpx`, but skip this module if the module is
# not present.
import pytest  # isort:skip # noqa: E402 # pylint: disable=wrong-import-position

cvx = pytest.importorskip("cvxpy")

_LOG = logging.getLogger(__name__)


# #############################################################################
# Single period optimization with costs and constraints
# #############################################################################


class SinglePeriodOptimizer:
    def __init__(
        self,
        config: cconfig.Config,
        df: pd.DataFrame,
        *,
        restrictions: Optional[pd.DataFrame] = None,
    ) -> None:
        """
        Single period optimization constructor.

        :param config: optimizer config
        :param df: dataframe with assets, volatility forecast,
            returns forecast, and current position. We pass this into the
            constructor because:
            - asset volatility is needed to generate a risk constraint
            - some restriction constraints are position-dependent
        :param restrictions: restrictions dataframe
        """
        # Process `config` and extract parameters.
        config.check_params(
            [
                "dollar_neutrality_penalty",
                "volatility_penalty",
                "turnover_penalty",
                "target_gmv",
                "target_gmv_upper_bound_multiple",
            ]
        )
        self._dollar_neutrality_penalty = config["dollar_neutrality_penalty"]
        self._volatility_penalty = config["volatility_penalty"]
        self._turnover_penalty = config["turnover_penalty"]
        self._target_gmv = config["target_gmv"]
        self._target_gmv_upper_bound_multiple = config[
            "target_gmv_upper_bound_multiple"
        ]
        SinglePeriodOptimizer._validate_df(df)
        self._df = df
        #
        if restrictions is not None:
            SinglePeriodOptimizer._validate_restrictions_df(restrictions)
        self._restrictions = restrictions
        #
        self._asset_ids = self._df["asset_id"]
        self._n_assets = df.shape[0]
        positions = self._df["position"]
        _LOG.debug("positions=\n%s", hpandas.df_to_str(positions))
        self._gmv = positions.abs().sum()
        self._current_weights = positions / self._target_gmv
        _LOG.debug(
            "current_weights=\n%s", hpandas.df_to_str(self._current_weights)
        )

    def optimize(self) -> pd.DataFrame:
        """
        Get target positions (in units of money).
        """
        target_weights, target_weight_diffs = self._optimize_weights()
        result_df = self._process_results(target_weights, target_weight_diffs)
        return result_df

    def compute_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        gross_volume = df["target_notional_trade"].abs().sum()
        net_volume = df["target_notional_trade"].sum()
        gmv = df["target_position"].abs().sum()
        nmv = df["target_position"].sum()
        notional_stats = pd.Series(
            {
                "gross_volume": gross_volume,
                "net_volume": net_volume,
                "gmv": gmv,
                "nmv": nmv,
            }
        ).rename("notional")
        gmv_stats = 100 * (notional_stats / self._target_gmv).rename("percentage")
        return pd.concat([notional_stats, gmv_stats], axis=1)

    def _optimize_weights(self) -> Tuple[cvx.Variable, cvx.Variable]:
        """
        Create and solve the cvx optimization problem.

        :return: target weights and weight diffs (from current weights),
            normalized by current GMV.
        """
        # Determine the current GMV and GMV-normalized weights.
        # Create a placeholder for (current) GMV-normalized weight adjustments.
        target_weight_diffs = cvx.Variable(self._n_assets)
        current_weights = self._current_weights.to_numpy()
        _LOG.debug("current_weights=\n%s", current_weights)
        target_weights = current_weights + target_weight_diffs
        # Create a placeholder for predicted returns (to maximize subject to
        # constraints).
        predictions = self._df["prediction"]
        predicted_returns = cvx.multiply(predictions.values, target_weights)
        mu = cvx.sum(predicted_returns)
        hdbg.dassert(mu.is_concave())
        # Get constraints.
        soft_constraints = self._get_soft_constraints()
        hard_constraints = self._get_hard_constraints()
        # Convert constraints into cvxpy expressions.
        soft_constraint_cvx_expr = [
            constraint.get_expr(
                target_weights, target_weight_diffs, self._target_gmv
            )
            for constraint in soft_constraints
        ]
        hard_constraint_cvx_expr = [
            constraint.get_expr(
                target_weights, target_weight_diffs, self._target_gmv
            )
            for constraint in hard_constraints
        ]
        # Create the cvxpy problem.
        problem = cvx.Problem(
            cvx.Maximize(mu - sum(soft_constraint_cvx_expr)),
            hard_constraint_cvx_expr,
        )
        # Optimize.
        optimal_value = problem.solve()
        hdbg.dassert_eq(problem.status, "optimal")
        _LOG.info("`optimal_value`=%0.2f", optimal_value)
        # TODO(Paul): Compute estimates for PnL, costs.
        return target_weights, target_weight_diffs

    def _get_soft_constraints(self) -> List[opbase.Expression]:
        # Create soft constraints
        soft_constraints = []
        # Add diagonal risk soft constraint.
        volatility = self._df["volatility"]
        diagonal_risk = osofcons.VolatilityRiskModel(
            volatility, self._volatility_penalty
        )
        soft_constraints.append(diagonal_risk)
        # Add dollar neutrality constraint.
        dollar_neutrality = osofcons.DollarNeutralitySoftConstraint(
            self._dollar_neutrality_penalty
        )
        soft_constraints.append(dollar_neutrality)
        # Add turnover soft constraint.
        turnover = osofcons.TurnoverSoftConstraint(self._turnover_penalty)
        soft_constraints.append(turnover)
        return soft_constraints

    def _get_hard_constraints(self) -> List[opbase.Expression]:
        # Create hard constraints.
        hard_constraints = []
        # Add target GMV hard constraint.
        target_gmv_constraint = oharcons.TargetGmvHardConstraint(
            self._target_gmv,
            self._target_gmv_upper_bound_multiple,
        )
        hard_constraints.append(target_gmv_constraint)
        if self._restrictions is not None:
            restriction_constraints = self._get_restriction_constraints()
            if restriction_constraints:
                hard_constraints.extend(restriction_constraints)
        return hard_constraints

    def _get_restriction_constraints(self) -> List[opbase.Expression]:
        constraints = []
        df = self._df.merge(self._restrictions, how="left", on="asset_id").fillna(
            False
        )
        do_not_buy = ((df["position"] >= 0) & df["is_buy_restricted"]) | (
            (df["position"] < 0) & df["is_buy_cover_restricted"]
        )
        if do_not_buy.any():
            do_not_buy_constraint = oharcons.DoNotBuyHardConstraint(do_not_buy)
            constraints.append(do_not_buy_constraint)
        do_not_sell = ((df["position"] > 0) & df["is_sell_long_restricted"]) | (
            (df["position"] <= 0) & df["is_sell_short_restricted"]
        )
        if do_not_sell.any():
            do_not_sell_constraint = oharcons.DoNotSellHardConstraint(do_not_sell)
            constraints.append(do_not_sell_constraint)
        # Note: it is possible for this list to be empty.
        return constraints

    def _process_results(
        self,
        target_weights: cvx.Variable,
        target_weight_diffs: cvx.Variable,
    ) -> pd.DataFrame:
        """
        Translate optimized weights into positions and notional trades.

        :param target_weights: target weights, normalized by current GMV
        :param target_weight_diffs: target weight diffs, normalized by current
            GMV
        :return: dataframe of target positions and notional trades, in addition
            to the raw weights and weight diffs.
        """
        # Collect series.
        srs_list = []
        # Target positions (notional).
        target_positions = pd.Series(
            index=self._asset_ids,
            data=target_weights.value * self._target_gmv,
            name="target_position",
        )
        srs_list.append(target_positions)
        # Target trades (notional).
        target_trades = pd.Series(
            index=self._asset_ids,
            data=target_weight_diffs.value * self._target_gmv,
            name="target_notional_trade",
        )
        srs_list.append(target_trades)
        # Target weights.
        target_weights = pd.Series(
            index=self._asset_ids,
            data=target_weights.value,
            name="target_weight",
        )
        srs_list.append(target_weights)
        # Target weight adjustments.
        target_weight_diffs = pd.Series(
            index=self._asset_ids,
            data=target_weight_diffs.value,
            name="target_weight_diff",
        )
        srs_list.append(target_weight_diffs)
        return pd.concat(srs_list, axis=1)

    @staticmethod
    def _validate_df(df: pd.DataFrame) -> None:
        """
        Sanity-check `df`.
        """
        SinglePeriodOptimizer._is_df_with_asset_id_col(df)
        # Ensure the dataframe has the expected columns.
        expected_cols = ["volatility", "prediction", "position"]
        hdbg.dassert_is_subset(expected_cols, df.columns)
        # Ensure that the dataframe has a range-index.
        hdbg.dassert_isinstance(df.index, pd.RangeIndex)
        # Do not allow NaNs.
        # TODO(Paul): We may need to relax this later
        for col in expected_cols:
            hdbg.dassert(not df[col].isna().any())

    @staticmethod
    def _is_df_with_asset_id_col(df: pd.DataFrame) -> None:
        # Type-check the dataframe.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        hdbg.dassert_in("asset_id", df.columns)
        # Ensure that there are no duplicate asset ids.
        hdbg.dassert_eq(df["asset_id"].nunique(), df["asset_id"].count())

    @staticmethod
    def _validate_restrictions_df(df: pd.DataFrame) -> None:
        """
        Sanity-check `df`.
        """
        SinglePeriodOptimizer._is_df_with_asset_id_col(df)
        # Type-check the dataframe.
        hdbg.dassert_isinstance(df, pd.DataFrame)
        # Ensure the dataframe has the expected columns.
        restriction_cols = [
            "is_buy_restricted",
            "is_buy_cover_restricted",
            "is_sell_short_restricted",
            "is_sell_long_restricted",
        ]
        hdbg.dassert_is_subset(restriction_cols, df.columns)
        for col in restriction_cols:
            hpandas.dassert_series_type_is(df[col], np.bool_)