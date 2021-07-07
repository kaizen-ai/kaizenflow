"""
Import as:

import core.dataflow_model.model_evaluator as modeval
"""

from __future__ import annotations

import collections
import functools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import core.dataflow as cdataf
import core.dataflow_model.stats_computer as cstats
import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class ModelEvaluator:
    """
    Evaluate performance of financial models for returns.

    TODO(Paul): Add setters for `prediction_col` and `target_col`.
    """

    def __init__(
        self,
        data: Dict[str, pd.DataFrame],
        *,
        prediction_col: Optional[str] = None,
        target_col: Optional[str] = None,
        oos_start: Optional[Any] = None,
    ) -> None:
        """
        
        """
        self._data = data
        dbg.dassert(data, msg="Data set must be nonempty.")
        # Use plain attributes (Item #44).
        self.prediction_col = prediction_col
        self.target_col = target_col
        self.oos_start = oos_start
        #
        self.valid_keys = list(self._data.keys())
        self._stats_computer = cstats.StatsComputer()

    def aggregate_models(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        position_method: Optional[str] = None,
        target_volatility: Optional[float] = None,
        mode: Optional[str] = None,
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Combine selected pnls.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param position_method: as in `PositionComputer.compute_positions()`
        :param target_volatility: as in `PositionComputer.compute_positions()`
        :param mode: "all_available", "ins", or "oos"
        :return: aggregate pnl stream, position stream, statistics
        """
        keys = keys or self.valid_keys
        dbg.dassert_is_subset(keys, self.valid_keys)
        mode = mode or "ins"
        pnl_dict = self.compute_pnl(
            keys=keys, position_method=position_method, mode=mode
        )
        pnl_df = pd.concat({k: v["pnl"] for k, v in pnl_dict.items()}, axis=1)
        weights = weights or [1 / len(keys)] * len(keys)
        dbg.dassert_eq(len(keys), len(weights))
        col_map = {keys[idx]: weights[idx] for idx in range(len(keys))}
        # Calculate pnl srs.
        pnl_df = pnl_df.apply(lambda x: x * col_map[x.name]).sum(
            axis=1, min_count=1
        )
        pnl_srs = pnl_df.squeeze()
        pnl_srs.name = "portfolio_pnl"
        #
        pos_df = pd.concat(
            {k: v["positions"] for k, v in pnl_dict.items()}, axis=1
        )
        pos_df = pos_df.apply(lambda x: x * col_map[x.name]).sum(
            axis=1, min_count=1
        )
        pos_srs = pos_df.squeeze()
        pos_srs.name = "portfolio_pos"
        # Maybe rescale.
        if target_volatility is not None:
            if mode != "ins":
                ins_pnl_srs, _, _ = self.aggregate_models(
                    keys=keys,
                    weights=weights,
                    mode="ins",
                    target_volatility=target_volatility,
                )
            else:
                ins_pnl_srs = pnl_srs
            scale_factor = fin.compute_volatility_normalization_factor(
                srs=ins_pnl_srs, target_volatility=target_volatility
            )
            pnl_srs *= scale_factor
            pos_srs *= scale_factor
        portfolio_dict = {"positions": pos_srs, "pnl": pnl_srs}
        aggregate_stats = self._stats_computer.compute_finance_stats(
            pd.DataFrame.from_dict(portfolio_dict),
            positions_col="positions",
            pnl_col="pnl",
        )
        return pnl_srs, pos_srs, aggregate_stats

    def calculate_stats(
        self,
        keys: Optional[List[Any]] = None,
        position_method: Optional[str] = None,
        target_volatility: Optional[float] = None,
        mode: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Calculate performance characteristics of selected models.

        :param keys: Use all available if `None`
        :param position_method: as in `PositionComputer.compute_positions()`
        :param target_volatility: as in `PositionComputer.compute_positions()`
        :param mode: "all_available", "ins", or "oos"
        :return: Dataframe of statistics with `keys` as columns
        """
        pnl_dict = self.compute_pnl(
            keys,
            position_method=position_method,
            target_volatility=target_volatility,
            mode=mode,
        )
        stats_dict = {}
        for key in tqdm(pnl_dict.keys(), desc="Calculating stats"):
            stats_dict[key] = self._stats_computer.compute_finance_stats(
                pnl_dict[key],
                returns_col="returns",
                predictions_col="predictions",
                positions_col="positions",
                pnl_col="pnl",
            )
        stats_df = pd.concat(stats_dict, axis=1)
        # Calculate BH adjustment of pvals.
        adj_pvals = stats.multipletests(
            stats_df.loc["signal_quality"].loc["sr.pval"], nan_mode="drop"
        ).rename("sr.adj_pval")
        adj_pvals = pd.concat(
            [adj_pvals.to_frame().transpose()], keys=["signal_quality"]
        )
        stats_df = pd.concat([stats_df, adj_pvals], axis=0)
        return stats_df

    def compute_pnl(
        self,
        keys: Optional[List[Any]] = None,
        position_method: Optional[str] = None,
        target_volatility: Optional[float] = None,
        mode: Optional[str] = None,
    ) -> Dict[Any, pd.DataFrame]:
        """
        Helper for calculating positions and PnL from returns and predictions.

        :param keys: Use all available if `None`
        :param position_method: as in `PositionComputer.compute_positions()`
        :param target_volatility: as in `PositionComputer.compute_positions()`
        """
        keys = keys or self.valid_keys
        dbg.dassert_is_subset(keys, self.valid_keys)
        #
        returns = {
            k: self._data[k][self.target_col].rename("returns") for k in keys
        }
        predictions = {
            k: self._data[k][self.prediction_col].rename("predictions")
            for k in keys
        }
        positions = {}
        for k in tqdm(returns.keys(), "Calculating positions"):
            position_computer = PositionComputer(
                returns=returns[k],
                predictions=predictions[k],
            )
            positions[k] = position_computer.compute_positions(
                prediction_strategy=position_method,
                target_volatility=target_volatility,
            ).rename("positions")
        pnls = {}
        for k in tqdm(positions.keys(), "Calculating PnL"):
            pnl_computer = PnlComputer(
                returns=returns[k],
                positions=positions[k],
            )
            pnls[k] = pnl_computer.compute_pnl().rename("pnl")
        pnl_dict = {}
        for k in keys:
            pnl_dict[k] = pd.concat(
                [returns[k], predictions[k], positions[k], pnls[k]], axis=1
            )
        pnl_dict = self._trim_time_range(pnl_dict, mode=mode)
        return pnl_dict

    def _trim_time_range(
        self,
        data_dict: Dict[Any, Union[pd.Series, pd.DataFrame]],
        mode: Optional[str] = None,
    ) -> Dict[Any, Union[pd.Series, pd.DataFrame]]:
        """
        Helper to trim to in-sample/out-of-sample region.
        """
        mode = mode or "ins"
        if mode == "all_available":
            trimmed = data_dict
        elif mode == "ins":
            trimmed = {k: v.loc[: self.oos_start] for k, v in data_dict.items()}
        elif mode == "oos":
            dbg.dassert(self.oos_start, msg="No `oos_start` set!")
            trimmed = {k: v.loc[self.oos_start :] for k, v in data_dict.items()}
        else:
            raise ValueError(f"Unrecognized mode `{mode}`.")
        return trimmed


# #############################################################################


class PnlComputer:
    """
    Computes PnL from returns and holdings.
    """

    def __init__(
        self,
        returns: pd.Series,
        positions: pd.Series,
    ) -> None:
        """
        Initialize by supply returns and positions.

        :param returns: financial returns
        :param predictions: returns predictions (aligned with returns)
        """
        self._validate_series(returns)
        self._validate_series(positions)
        self.returns = returns
        self.positions = positions
        # TODO(*): validate indices

    def compute_pnl(self) -> pd.Series:
        """
        Compute PnL from returns and positions.
        """
        pnl = self.returns.multiply(self.positions)
        dbg.dassert(pnl.index.freq)
        pnl.name = "pnl"
        return pnl

    @staticmethod
    def _validate_series(srs: pd.Series) -> None:
        dbg.dassert_isinstance(srs, pd.Series)
        dbg.dassert(not srs.dropna().empty)
        dbg.dassert(srs.index.freq)


# #############################################################################


class PositionComputer:
    """
    Computes target positions from returns, predictions, and constraints.
    """

    def __init__(
        self,
        *,
        returns: pd.Series,
        predictions: pd.Series,
        oos_start: Optional[float] = None,
    ) -> None:
        """
        Initialize by supplying returns and predictions.

        :param returns: financial returns
        :param predictions: returns predictions (aligned with returns)
        :param oos_start: optional end of in-sample/start of out-of-sample.
        """
        self.oos_start = oos_start
        self._validate_series(returns, self.oos_start)
        self._validate_series(predictions, self.oos_start)
        self.returns = returns
        self.predictions = predictions

    def compute_positions(
        self,
        target_volatility: Optional[float] = None,
        prediction_strategy: Optional[str] = None,
        volatility_strategy: Optional[str] = None,
        mode: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.Series:
        """
        Compute positions from returns and predictions.

        :param target_volatility: generate positions to achieve target
            volatility on in-sample region.
        :param prediction_strategy: "raw", "kernel", "squash", "binarize"
        :param volatility_strategy: "rescale", "rolling" (not yet implemented)
        :param mode: "all_available", "ins", or "oos"
        :return: series of positions
        """
        mode = mode or "ins"
        # Process/adjust predictions.
        prediction_strategy = prediction_strategy or "raw"
        if prediction_strategy == "raw":
            predictions = self.predictions.copy()
        elif prediction_strategy == "kernel":
            predictions = self._multiply_kernel(self.predictions, **kwargs)
        elif prediction_strategy == "squash":
            predictions = self._squash(self.predictions, **kwargs)
        elif prediction_strategy == "binarize":
            predictions = self.predictions.divide(np.abs(self.predictions))
        else:
            raise ValueError(
                f"Unrecognized prediction_strategy `{prediction_strategy}`!"
            )
        # Specify strategy for volatility targeting.
        volatility_strategy = volatility_strategy or "rescale"
        if target_volatility is None:
            positions = predictions.copy()
            positions.name = "positions"
            return self._return_srs(positions, mode=mode)
        return self._adjust_for_volatility(
            predictions,
            target_volatility=target_volatility,
            mode=mode,
            volatility_strategy=volatility_strategy,
        )

    @staticmethod
    def _multiply_kernel(
        predictions: pd.Series,
        tau: float,
        delay: int,
        z_mute_point: float,
        z_saturation_point: float,
    ) -> pd.Series:
        zscored_preds = sigp.compute_rolling_zscore(
            predictions, tau=tau, delay=delay
        )
        bump_function = functools.partial(
            sigp.c_infinity_bump_function, a=z_mute_point, b=z_saturation_point
        )
        scale_factors = 1 - zscored_preds.apply(bump_function)
        adjusted_preds = zscored_preds.multiply(scale_factors)
        return adjusted_preds

    @staticmethod
    def _squash(
        predictions: pd.Series,
        tau: float,
        delay: int,
        scale: float,
    ) -> pd.Series:
        zscored_preds = sigp.compute_rolling_zscore(
            predictions, tau=tau, delay=delay
        )
        return sigp.squash(zscored_preds, scale=scale)

    def _adjust_for_volatility(
        self,
        predictions: pd.Series,
        target_volatility: float,
        mode: str,
        volatility_strategy: str,
    ) -> pd.Series:
        """

        :param predictions:
        :param target_volatility:
        :param mode:
        :param volatility_strategy:
        :return:
        """
        # Compute PnL by interpreting predictions as positions.
        pnl_computer = PnlComputer(self.returns, predictions)
        pnl = pnl_computer.compute_pnl()
        pnl.name = "pnl"
        #
        ins_pnl = pnl[: self.oos_start]
        if volatility_strategy == "rescale":
            scale_factor = fin.compute_volatility_normalization_factor(
                srs=ins_pnl, target_volatility=target_volatility
            )
            positions = scale_factor * predictions
            positions.name = "positions"
            return self._return_srs(positions, mode=mode)
        raise ValueError(f"Unrecognized strategy `{volatility_strategy}`!")

    def _return_srs(self, srs: pd.Series, mode: str) -> pd.Series:
        if mode == "ins":
            return srs[: self.oos_start]
        elif mode == "all_available":
            return srs
        elif mode == "oos":
            dbg.dassert(
                self.oos_start,
                msg="Must set `oos_start` to run `oos`",
            )
            return srs[self.oos_start :]
        else:
            raise ValueError(f"Invalid mode `{mode}`!")

    @staticmethod
    def _validate_series(srs: pd.Series, oos_start: Optional[float]) -> None:
        dbg.dassert_isinstance(srs, pd.Series)
        dbg.dassert(not srs.dropna().empty)
        if oos_start is not None:
            dbg.dassert(not srs[:oos_start].dropna().empty)
            dbg.dassert(not srs[oos_start:].dropna().empty)
        dbg.dassert(srs.index.freq)


# #############################################################################


class TransactionCostModeler:
    """
    Estimates transaction costs.
    """

    def __init__(
        self,
        *,
        price: pd.Series,
        positions: pd.Series,
        oos_start: Optional[float] = None,
    ) -> None:
        self.oos_start = oos_start
        self._validate_series(price, self.oos_start)
        self._validate_series(positions, self.oos_start)
        self.price = price
        self.positions = positions

    def compute_transaction_costs(
        self,
        tick_size: Optional[float] = None,
        spread_pct: Optional[float] = None,
        mode: Optional[str] = None,
    ) -> pd.Series:
        """
        Estimate transaction costs by estimating the bid-ask spread.
        """
        mode = mode or "ins"
        tick_size = tick_size or 0.01
        spread_pct = spread_pct or 0.5
        spread = tick_size / self.price
        position_changes = np.abs(self.positions.diff())
        transaction_costs = spread_pct * spread.multiply(position_changes)
        transaction_costs.name = "transaction_costs"
        return self._return_srs(transaction_costs, mode=mode)

    def _return_srs(self, srs: pd.Series, mode: str) -> pd.Series:
        if mode == "ins":
            ret = srs[: self.oos_start]
        elif mode == "all_available":
            ret = srs
        elif mode == "oos":
            dbg.dassert(
                self.oos_start,
                msg="Must set `oos_start` to run `oos`",
            )
            ret = srs[self.oos_start :]
        else:
            raise ValueError(f"Invalid mode `{mode}`!")
        return ret

    @staticmethod
    def _validate_series(srs: pd.Series, oos_start: Optional[float]) -> None:
        dbg.dassert_isinstance(srs, pd.Series)
        dbg.dassert(not srs.dropna().empty)
        if oos_start is not None:
            dbg.dassert(not srs[:oos_start].dropna().empty)
            dbg.dassert(not srs[oos_start:].dropna().empty)
        dbg.dassert(srs.index.freq)


def build_model_evaluator_from_result_bundle_dicts(
    result_bundle_dicts: collections.OrderedDict,
    returns_col: str,
    predictions_col: str,
    oos_start: Optional[Any] = None,
) -> ModelEvaluator:
    """
    Initialize a `ModelEvaluator` from `ResultBundle`s.

    :param result_bundle_dicts: dict of `ResultBundle`s, each of which was
        generated using `ResultBundle.to_dict()`
    :param returns_col: column of `ResultBundle.result_df` to use as the column
        representing returns
    :param predictions_col: like `returns_col`, but for predictions
    :param oos_start: as in `ModelEvaluator`
    :return: `ModelEvaluator` initialized with returns and predictions from
       result bundles
    """
    # Convert each `ResultBundle` dict into a `ResultBundle` class object.
    result_bundles = {
        k: cdataf.ResultBundle.from_dict(v)
        for k, v in result_bundle_dicts.items()
    }
    data_dict = {}
    for key, rb in result_bundles.items():
        df = rb.result_df
        dbg.dassert_in(returns_col, df.columns)
        dbg.dassert_in(predictions_col, df.columns)
        data_dict[key] = df
    # Initialize `ModelEvaluator`.
    evaluator = ModelEvaluator(
        data=data_dict,
        prediction_col=predictions_col,
        target_col=returns_col,
        oos_start=oos_start,
    )
    return evaluator
