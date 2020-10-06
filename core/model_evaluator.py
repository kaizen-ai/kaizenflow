"""
Import as:

import core.model_evaluator as modeval
"""

import functools
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class ModelEvaluator:
    """
    Evaluates performance of financial models for returns.
    """

    def __init__(
        self,
        *,
        returns: Dict[Any, pd.Series],
        predictions: Dict[Any, pd.Series],
        price: Optional[Dict[Any, pd.Series]] = None,
        volume: Optional[Dict[Any, pd.Series]] = None,
        volatility: Optional[Dict[Any, pd.Series]] = None,
        target_volatility: Optional[float] = None,
        oos_start: Optional[Any] = None,
    ) -> None:
        """
        Initialize by supplying returns and predictions.

        :param returns: financial returns
        :param predictions: returns predictions (aligned with returns)
        :param price: price of instrument
        :param volume: volume traded
        :param volatility: returns volatility (used for adjustment)
        :param target_volatility: generate positions to achieve target
            volatility on in-sample region.
        :param oos_start: optional end of in-sample/start of out-of-sample.
        """
        dbg.dassert_isinstance(returns, dict)
        dbg.dassert_isinstance(predictions, dict)
        self.oos_start = oos_start or None
        self.valid_keys = self._get_valid_keys(
            returns, predictions, self.oos_start
        )
        self.rets = {k: returns[k] for k in self.valid_keys}
        self.preds = {k: predictions[k] for k in self.valid_keys}
        self.price = None
        self.volume = None
        self.slippage = None
        if price is not None:
            keys = self._get_valid_keys(returns, price, self.oos_start)
            dbg.dassert(set(self.valid_keys) == set(keys))
            self.price = {k: price[k] for k in self.valid_keys}
        if volume is not None:
            keys = self._get_valid_keys(returns, volume, self.oos_start)
            dbg.dassert(set(self.valid_keys) == set(keys))
            self.volume = {k: volume[k] for k in self.valid_keys}
        if volatility is not None:
            keys = self._get_valid_keys(returns, volatility, self.oos_start)
            dbg.dassert(set(self.valid_keys) == set(keys))
            self.volatility = {k: volatility[k] for k in self.valid_keys}
        self.target_volatility = target_volatility or None
        # Calculate positions
        self.pos = self._calculate_positions()
        # Calculate pnl streams.
        # TODO(*): Allow configurable strategies.
        # TODO(*): Maybe require that this be called instead of always doing it.
        self.pnls = self._calculate_pnls(self.rets, self.pos)

    # TODO(*): Consider exposing positions / returns in the same way.
    def get_series_dict(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
    ) -> Dict[Any, pd.Series]:
        """
        Return pnls for requested keys over requested range.

        :param series: "returns", "predictions", "positions", or "pnls"
        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        :return: Dictionary of rescaled PnL curves
        """
        keys = keys or self.valid_keys
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        mode = mode or "ins"
        # Select the data stream.
        if series == "returns":
            series_dict = self.rets
        elif series == "predictions":
            series_dict = self.preds
        elif series == "positions":
            series_dict = self.pos
        elif series == "pnls":
            series_dict = self.pnls
        elif series == "price":
            dbg.dassert(self.price, msg="No price data supplied")
            series_dict = self.price
        elif series == "volume":
            dbg.dassert(self.volume, msg="No volume data supplied")
            series_dict = self.volume
        elif series == "volatility":
            dbg.dassert(self.volatility, msg="No volume data supplied")
            series_dict = self.volatility
        elif series == "slippage":
            dbg.dassert(self._slippage, msg="Cannot calculate slippage")
            series_dict = self._slippage
        else:
            raise ValueError(f"Unrecognized series `{series}`.")
        # NOTE: ins/oos overlap by one point as-is (consider changing).
        if mode == "all_available":
            series_for_keys = {k: series_dict[k] for k in keys}
        elif mode == "ins":
            series_for_keys = {
                k: series_dict[k].loc[: self.oos_start] for k in keys
            }
        elif mode == "oos":
            dbg.dassert(self.oos_start, msg="No `oos_start` set!")
            series_for_keys = {
                k: series_dict[k].loc[self.oos_start :] for k in keys
            }
        else:
            raise ValueError(f"Unrecognized mode `{mode}`.")
        return series_for_keys

    def aggregate_models(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        target_volatility: Optional[float] = None,
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Combine selected pnls.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param target_volatility: Rescale portfolio to achieve
            `target_volatility` on in-sample region
        :return: aggregate pnl stream, position stream, statistics
        """
        keys = keys or self.valid_keys
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        mode = mode or "ins"
        # Obtain dataframe of (log) returns.
        pnl_df = self._get_series_as_df("pnls", keys, mode)
        # Convert to pct returns before aggregating.
        pnl_df = pnl_df.apply(fin.convert_log_rets_to_pct_rets)
        # Average by default; otherwise use supplied weights.
        weights = weights or [1 / len(keys)] * len(keys)
        dbg.dassert_eq(len(keys), len(weights))
        col_map = {keys[idx]: weights[idx] for idx in range(len(keys))}
        # Calculate pnl srs.
        pnl_df = pnl_df.apply(lambda x: x * col_map[x.name]).sum(
            axis=1, min_count=1
        )
        pnl_srs = pnl_df.squeeze()
        # Convert back to log returns from aggregated pct returns.
        pnl_srs = fin.convert_pct_rets_to_log_rets(pnl_srs)
        pnl_srs.name = "portfolio_pnl"
        # Aggregate positions.
        pos_df = self._get_series_as_df("positions", keys, mode)
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
        # Calculate statistics.
        oos_start = None
        if mode == "all_available" and self.oos_start is not None:
            oos_start = self.oos_start
        aggregate_stats = self._calculate_model_stats(
            positions=pos_srs, pnl=pnl_srs, oos_start=oos_start
        )
        return pnl_srs, pos_srs, aggregate_stats

    def calculate_stats(
        self, keys: Optional[List[Any]] = None, mode: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Calculate performance characteristics of selected models.

        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        :return: Dataframe of statistics with `keys` as columns
        """
        keys = keys or self.valid_keys
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        pnls = self.get_series_dict("pnls", keys, mode)
        pos = self.get_series_dict("positions", keys, mode)
        rets = self.get_series_dict("returns", keys, mode)
        stats_dict = {}
        oos_start = None
        if mode == "all_available" and self.oos_start is not None:
            oos_start = self.oos_start
        for key in tqdm(keys):
            stats_val = self._calculate_model_stats(
                returns=rets[key],
                positions=pos[key],
                pnl=pnls[key],
                oos_start=oos_start,
            )
            stats_dict[key] = stats_val
        stats_df = pd.concat(stats_dict, axis=1)
        # Calculate BH adjustment of pvals.
        adj_pvals = stats.multipletests(stats_df.loc["pval"], nan_mode="drop")
        stats_df = pd.concat(
            [stats_df.transpose(), adj_pvals], axis=1
        ).transpose()
        return stats_df

    @staticmethod
    def _calculate_model_stats(
        *,
        returns: Optional[pd.Series] = None,
        positions: Optional[pd.Series] = None,
        pnl: Optional[pd.Series] = None,
        oos_start: Optional[Any] = None,
    ) -> pd.DataFrame:
        """
        Calculate stats for a single model or portfolio.
        """
        dbg.dassert(
            not pd.isna([pnl, positions, returns]).all(),
            "At least one series should be not `None`.",
        )
        freqs = {
            srs.index.freq for srs in [pnl, positions, returns] if srs is not None
        }
        dbg.dassert_eq(len(freqs), 1, "Series have different frequencies.")
        # Calculate stats.
        stats_dict = {}
        if pnl is not None:
            stats_dict[0] = stats.summarize_sharpe_ratio(pnl)
            stats_dict[1] = stats.ttest_1samp(pnl)
            stats_dict[2] = pd.Series(
                fin.compute_kratio(pnl), index=["kratio"], name=pnl.name
            )
            stats_dict[3] = stats.compute_annualized_return_and_volatility(pnl)
            stats_dict[4] = stats.compute_max_drawdown(pnl)
            stats_dict[5] = stats.summarize_time_index_info(pnl)
            stats_dict[6] = stats.calculate_hit_rate(pnl)
            stats_dict[10] = stats.compute_jensen_ratio(pnl)
            stats_dict[11] = stats.compute_forecastability(pnl)
            stats_dict[13] = stats.compute_moments(pnl)
            stats_dict[14] = stats.compute_special_value_stats(pnl)
        if pnl is not None and returns is not None:
            stats_dict[7] = pd.Series(
                pnl.corr(returns), index=["corr_to_underlying"], name=returns.name
            )
        if positions is not None and returns is not None:
            stats_dict[8] = stats.compute_bet_stats(
                positions, returns[positions.index]
            )
            # TODO(*): Use `predictions` instead.
            stats_dict[12] = pd.Series(
                positions.corr(returns),
                index=["prediction_corr"],
                name=returns.name,
            )
        if positions is not None:
            stats_dict[9] = stats.compute_avg_turnover_and_holding_period(
                positions
            )
        # Z-score OOS SRs.
        if oos_start is not None and pnl is not None:
            dbg.dassert(pnl[:oos_start].any())
            dbg.dassert(pnl[oos_start:].any())
            stats_dict[15] = stats.zscore_oos_sharpe_ratio(pnl, oos_start)
        # Sort dict by integer keys.
        stats_dict = dict(sorted(stats_dict.items()))
        # Combine stats into one series indexed by stats names.
        stats_srs = pd.concat(stats_dict).droplevel(0)
        stats_srs.name = "stats"
        return stats_srs

    def _get_series_as_df(
        self, series: str, keys: List[Any], mode: str,
    ) -> pd.DataFrame:
        """
        Return request series streams as a single dataframe.

        :param keys: stream keys
        :param mode: "all_available", "ins", or "oos"
        :return: Dataframe of series with `keys` as columns
        """
        series_for_keys = self.get_series_dict(series, keys, mode)
        # Confirm that the streams are of the same frequency.
        freqs = set()
        for s in series_for_keys.values():
            freq = s.index.freq
            dbg.dassert(freq, "Data should have a frequency.")
            freqs.add(freq)
        dbg.dassert_eq(len(freqs), 1, "Series should have the same frequency.")
        # Create dataframe.
        df = pd.DataFrame.from_dict(series_for_keys)
        # TODO(*): Add first_valid_index option
        return df

    def _calculate_positions(self) -> Dict[Any, pd.Series]:
        """
        Calculate positions from returns and predictions.

        Rescales to target volatility over in-sample period (if provided).
        """
        position_dict = {}
        for key in tqdm(self.valid_keys):
            position_computer = PositionComputer(
                returns=self.rets[key],
                predictions=self.preds[key],
                oos_start=self.oos_start,
            )
            positions = position_computer.compute_positions(
                target_volatility=self.target_volatility,
                mode="all_available",
                strategy="rescale")
            position_dict[key] = positions
        return position_dict

    @staticmethod
    def _calculate_pnls(
        returns: Dict[Any, pd.Series], positions: Dict[Any, pd.Series],
    ) -> Dict[Any, pd.Series]:
        """
        Calculate returns from positions.
        """
        pnls = {}
        for key in tqdm(returns.keys()):
            pnl_computer = PnlComputer(
                returns=returns[key], positions=positions[key]
            )
            pnl = pnl_computer.compute_pnl()
            pnls[key] = pnl
        return pnls

    def _get_valid_keys(
        self,
        dict1: Dict[Any, pd.Series],
        dict2: Dict[Any, pd.Series],
        oos_start: Optional[float],
    ) -> list:
        """
        Perform basic sanity checks.
        """
        dict1_keys = set(self._get_valid_keys_helper(dict1, oos_start))
        dict2_keys = set(self._get_valid_keys_helper(dict2, oos_start))
        shared_keys = dict1_keys.intersection(dict2_keys)
        dbg.dassert(shared_keys, msg="Set of valid keys must be nonempty!")
        for key in shared_keys:
            dbg.dassert_eq(dict1[key].index.freq, dict2[key].index.freq)
        return list(shared_keys)

    @staticmethod
    def _get_valid_keys_helper(
        input_dict: Dict[Any, pd.Series], oos_start: Optional[float]
    ) -> list:
        """
        Return keys for nonempty values with a `freq`.
        """
        valid_keys = []
        for k, v in input_dict.items():
            if v.empty:
                _LOG.warning("Empty series for `k`=%s", str(k))
                continue
            if v.dropna().empty:
                _LOG.warning("All NaN series for `k`=%s", str(k))
            if oos_start is not None:
                if v[:oos_start].dropna().empty:
                    _LOG.warning("All-NaN in-sample for `k`=%s", str(k))
                    continue
                if v[oos_start:].dropna().empty:
                    _LOG.warning("All-NaN out-of-sample for `k`=%s", str(k))
                    continue
            if v.index.freq is None:
                _LOG.warning("No `freq` for series for `k`=%s", str(k))
                continue
            valid_keys.append(k)
        return valid_keys


class PnlComputer:
    """
    Computes PnL from returns and holdings.
    """

    def __init__(self, returns: pd.Series, positions: pd.Series,) -> None:
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



    def compute_positions(self,
                          target_volatility: Optional[float] = None,
                          mode: Optional[str] = None,
                          prediction_strategy: Optional[str] = None,
                          volatility_strategy: Optional[str] = None,
                          **kwargs: Any,
                          ) -> pd.Series:
        """
        Compute positions from returns and predictions.

        :param target_volatility: generate positions to achieve target
            volatility on in-sample region.
        :param mode: "all_available", "ins", or "oos"
        :param prediction_strategy: "raw", "kernel", "squash"
        :param volatility_strategy: "rescale", "rolling" (not yet implemented)
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
        else:
            raise ValueError(f"Unrecognized prediction_strategy `{prediction_strategy}`!")
        # Specify strategy for volatility targeting.
        volatility_strategy = volatility_strategy or "rescale"
        if target_volatility is None:
            positions = predictions.copy()
            positions.name = "positions"
            return self._return_srs(positions, mode=mode)
        return self._adjust_for_volatility(predictions,
                                           target_volatility=target_volatility,
                                           mode=mode,
                                           volatility_strategy=volatility_strategy,
                                           )
    def _multiply_kernel(self,
                predictions: pd.Series,
                tau: float,
                delay: int,
                z_mute_point: float,
                z_saturation_point: float,
                ) -> pd.Series:
        zscored_preds = sigp.compute_rolling_zscore(predictions,
                                                    tau=tau,
                                                    delay=delay)
        bump_function = functools.partial(sigp.c_infinity_bump_function,
                                          a=z_mute_point,
                                          b=z_saturation_point)
        scale_factors = 1 - zscored_preds.apply(bump_function)
        adjusted_preds = zscored_preds.multiply(scale_factors)
        return adjusted_preds

    def _squash(self,
                predictions: pd.Series,
                tau: float,
                delay: int,
                scale: float,
                ) -> pd.Series:

        zscored_preds = sigp.compute_rolling_zscore(predictions,
                                                    tau=tau,
                                                    delay=delay)
        return sigp.squash(zscored_preds, scale=scale)

    def _adjust_for_volatility(self,
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
        else:
            raise ValueError(f"Unrecognized strategy `{volatility_strategy}`!")

    def _return_srs(self, srs: pd.Series, mode: str) -> pd.Series:
        if mode == "ins":
            return srs[: self.oos_start]
        elif mode == "all_available":
            return srs
        elif mode == "oos":
            dbg.dassert(
                self.oos_start, msg="Must set `oos_start` to run `oos`",
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


class TransactionCostModeler:
    """
    Estimates transaction costs.
    """

    def __init__(self,
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

    def compute_transaction_costs(self,
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
            return srs[: self.oos_start]
        elif mode == "all_available":
            return srs
        elif mode == "oos":
            dbg.dassert(
                self.oos_start, msg="Must set `oos_start` to run `oos`",
            )
            return srs[self.oos_start:]
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
