"""
Import as:

import core.model_evaluator as modeval
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

import core.finance as fin
import core.statistics as stats
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class ModelEvaluator:
    """
    Evaluate performance of financial models for returns.
    """

    def __init__(
        self,
        returns: Dict[Any, pd.Series],
        predictions: Dict[Any, pd.Series],
        target_volatility: Optional[float] = None,
        oos_start: Optional[float] = None,
    ) -> None:
        """
        Initialize by supplying returns and predictions.

        :param returns: financial (log) returns
        :param predictions: returns predictions (aligned with returns)
        :param target_volatility: Generate positions to achieve target
            volatility on in-sample region.
        :param oos_start: Optional end of in-sample/start of out-of-sample.
        """
        dbg.dassert_isinstance(returns, dict)
        dbg.dassert_isinstance(predictions, dict)
        self.oos_start = oos_start or None
        self.valid_keys = self._get_valid_keys(
            returns, predictions, self.oos_start
        )
        self.rets = {k: returns[k] for k in self.valid_keys}
        self.preds = {k: predictions[k] for k in self.valid_keys}
        self.target_volatility = target_volatility or None
        # Calculate positions
        self.pos = self._calculate_positions()
        # Calculate pnl streams.
        # TODO(*): Allow configurable strategies.
        # TODO(*): Maybe required that this be called instead of always doing it.
        self.pnls = self._calculate_pnls(self.rets, self.pos)

    # TODO(*): Consider exposing positions / returns in the same way.
    def get_pnls(
        self, keys: Optional[List[Any]] = None, mode: Optional[str] = None,
    ) -> Dict[Any, pd.Series]:
        """
        Return pnls for requested keys over requested range.

        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        :return: Dictionary of rescaled PnL curves
        """
        keys = keys or self.valid_keys
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        mode = mode or "all_available"
        # NOTE: ins/oos overlap by one point as-is (consider changing).
        if mode == "all_available":
            return {k: v for k, v in self.pnls.items()}
        if mode == "ins":
            return {k: v.loc[: self.oos_start] for k, v in self.pnls.items()}
        if mode == "oos":
            dbg.dassert(self.oos_start, msg="No `oos_start` set!")
            return {k: v.loc[self.oos_start :] for k, v in self.pnls.items()}
        raise ValueError(f"Unrecognized mode {mode}.")

    def aggregate_models(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
    ) -> Tuple[pd.Series, pd.Series]:
        """
        Combine selected pnls.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :return: aggregate pnl stream
        """
        keys = keys or self.valid_keys
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        mode = mode or "all_available"
        # Obtain dataframe of (log) returns.
        pnl_df = self._get_series_as_df("pnls", keys, mode)
        # Convert to pct returns before aggregating.
        pnl_df = pnl_df.apply(fin.convert_log_rets_to_pct_rets)
        # Average by default; otherwise use supplied weights.
        weights = weights or [1 / len(keys)] * len(keys)
        dbg.dassert_eq(len(keys), len(weights))
        col_map = {keys[idx]: weights[idx] for idx in range(len(keys))}
        # Calculate pnl srs.
        pnl_df = pnl_df.apply(lambda x: x * col_map[x.name]).sum(axis=1)
        pnl_srs = pnl_df.squeeze()
        # Convert back to log returns from aggregated pct returns.
        pnl_srs = fin.convert_pct_rets_to_log_rets(pnl_srs)
        pnl_srs.name = "portfolio_pnl"
        # Aggregate positions.
        pos_df = self._get_series_as_df("positions", keys, mode)
        pos_df = pos_df.apply(lambda x: x * col_map[x.name]).sum(axis=1)
        pos_srs = pos_df.squeeze()
        pos_srs.name = "portfolio_pos"
        return pnl_srs, pos_srs

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
        mode = mode or "all_available"
        if mode == "all_available":
            pnl = {k: self.pnls[k] for k in keys}
            pos = {k: self.pos[k] for k in keys}
            rets = {k: self.rets[k] for k in keys}
        elif mode == "ins":
            pnl = {k: self.pnls[k].loc[: self.oos_start] for k in keys}
            pos = {k: self.pos[k].loc[: self.oos_start] for k in keys}
            rets = {k: self.rets[k].loc[: self.oos_start] for k in keys}
        elif mode == "oos":
            dbg.dassert(self.oos_start, msg="No `oos_start` set!")
            pnl = {k: self.pnls[k].loc[self.oos_start :] for k in keys}
            pos = {k: self.pos[k].loc[self.oos_start :] for k in keys}
            rets = {k: self.rets[k].loc[self.oos_start :] for k in keys}
        else:
            raise ValueError(f"Unrecognized mode {mode}.")
        stats_dict = {}
        for key in tqdm(keys):
            stats_val = self.calculate_model_stats(
                returns=rets[key], positions=pos[key], pnl=pnl[key]
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
    def calculate_model_stats(
            *,
            returns: Optional[pd.Series] = None,
            positions: Optional[pd.Series] = None,
            pnl: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        Calculate stats for a single test run.
        """
        dbg.dassert(
            not pd.isna([pnl, positions, returns]).all(),
            "At least one series should be not `None`",
        )
        freqs = {
            srs.index.freq for srs in [pnl, positions, returns] if srs is not None
        }
        dbg.dassert_eq(len(freqs), 1, "Series have different frequencies")
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
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        # Select the data stream.
        if series == "pnls":
            series_dict = self.pnls
        elif series == "positions":
            series_dict = self.pos
        elif series == "returns":
            series_dict = self.rets
        else:
            raise ValueError(f"Unrecognized series {series}")
        series_for_keys = {k: series_dict[k] for k in keys}
        # Confirm that the streams are of the same frequency.
        freqs = set()
        for s in series_for_keys.values():
            freq = s.index.freq
            dbg.dassert(freq, "Data should have a frequency")
            freqs.add(freq)
        dbg.dassert_eq(len(freqs), 1, "Series should have the same frequency")
        # Create dataframe.
        df = pd.DataFrame.from_dict(series_for_keys)
        if mode == "all_available":
            pass
        elif mode == "ins":
            df = df.loc[: self.oos_start]
        elif mode == "oos":
            dbg.dassert(self.oos_start, msg="No `oos_start` set!")
            df = df.loc[self.oos_start :]
        else:
            raise ValueError(f"Unrecognized mode {mode}.")
        # TODO(*): Add first_valid_index option
        return df

    def _calculate_positions(self) -> Dict[Any, pd.Series]:
        """
        Calculate positions from returns and predictions.

        Rescales to target volatility over in-sample period (if provided).
        """
        pnls = self._calculate_pnls(self.rets, self.preds)
        if self.oos_start is not None:
            insample_pnls = {
                k: pnls[k].loc[: self.oos_start] for k in self.valid_keys
            }
        else:
            insample_pnls = pnls
        if self.target_volatility is not None:
            scale_factors = {
                k: fin.compute_volatility_normalization_factor(
                    srs=insample_pnls[k], target_volatility=self.target_volatility
                )
                for k in self.valid_keys
            }
        else:
            scale_factors = {k: 1.0 for k in self.valid_keys}
        return {k: scale_factors[k] * self.preds[k] for k in self.valid_keys}

    @staticmethod
    def _calculate_pnls(
        returns: Dict[Any, pd.Series], positions: Dict[Any, pd.Series]
    ) -> Dict[Any, pd.Series]:
        """
        Calculate returns from positions.
        """
        pnls = {}
        for key in tqdm(returns.keys()):
            pnl = returns[key].multiply(positions[key])
            dbg.dassert(pnl.index.freq)
            pnls[key] = pnl
        return pnls

    def _get_valid_keys(
        self,
        returns: Dict[Any, pd.Series],
        predictions: Dict[Any, pd.Series],
        oos_start: Optional[float],
    ) -> list:
        """
        Perform basic sanity checks.

        :param returns:
        :param predictions:
        :return:
        """
        rets_keys = set(self._get_valid_keys_helper(returns, oos_start))
        preds_keys = set(self._get_valid_keys_helper(predictions, oos_start))
        shared_keys = rets_keys.intersection(preds_keys)
        dbg.dassert(shared_keys, msg="Set of valid keys must be nonempty!")
        for key in shared_keys:
            dbg.dassert_eq(returns[key].index.freq, predictions[key].index.freq)
        return list(shared_keys)

    @staticmethod
    def _get_valid_keys_helper(
        input_dict: Dict[Any, pd.Series], oos_start: Optional[float]
    ) -> list:
        """
        Return keys for nonempty values with a `freq`.

        :param input_dict:
        :return:
        """
        valid_keys = []
        for k, v in input_dict.items():
            if v.empty:
                _LOG.warning("Empty series for `k`=%s", str(k))
                continue
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
