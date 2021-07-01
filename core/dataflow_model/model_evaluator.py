"""
Import as:

import core.dataflow_model.model_evaluator as modeval
"""

from __future__ import annotations

import functools
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import core.dataflow as cdataf
import core.finance as fin
import core.signal_processing as sigp
import core.statistics as stats
import core.stats_computer as cstats
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
        self.volatility = None
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
        self.pnls = self._calculate_pnls(self.rets, self.pos)
        self.stats_computer = cstats.StatsComputer()

    def dump_json(self) -> str:
        """
        Dump `ModelEvaluator` instance to json.

        Implementation details:
          - series' indices are converted to `str`. This way they can be easily
            restored
          - if `self.oos_start` is `None`, it is saved as is. Otherwise, it is
            converted to `str`
        :return: json with "returns", "predictions", "price", "volume",
            "volatility", "target_volatility", "oos_start" fields
        """
        oos_start = self.oos_start
        if oos_start is not None:
            oos_start = str(oos_start)
        json_dict = {
            "returns": self._dump_series_dict_to_json_dict(self.rets),
            "predictions": self._dump_series_dict_to_json_dict(self.preds),
            "price": self._dump_series_dict_to_json_dict(self.price),
            "volume": self._dump_series_dict_to_json_dict(self.volume),
            "volatility": self._dump_series_dict_to_json_dict(self.volatility),
            "target_volatility": self.target_volatility,
            "oos_start": oos_start,
        }
        json_str = json.dumps(json_dict, indent=4)
        return json_str

    @classmethod
    def load_json(cls, json_str: str, keys_to_int: bool = True) -> ModelEvaluator:
        """
        Load `ModelEvaluator` instance from json.

        :param json_str: the output of `ModelEvaluator.dump_json`
        :param keys_to_int: if `True`, convert dict keys to `int`
        :return: `ModelEvaluator` instance
        """
        json_dict = json.loads(json_str)
        for key in ["returns", "predictions", "price", "volume", "volatility"]:
            json_dict[key] = cls._load_series_dict_from_json_dict(
                json_dict[key], keys_to_int=keys_to_int
            )
        if json_dict["oos_start"] is not None:
            json_dict["oos_start"] = pd.Timestamp(json_dict["oos_start"])
        model_evaluator = cls(**json_dict)
        return model_evaluator

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
            dbg.dassert(self.volatility, msg="No volatility data supplied")
            series_dict = self.volatility
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
        self,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
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
        adj_pvals = stats.multipletests(
            stats_df.loc["signal_quality"].loc["sr.pval"], nan_mode="drop"
        )
        adj_pvals = pd.concat(
            [adj_pvals.to_frame().transpose()], keys=["signal_quality"]
        )
        stats_df = pd.concat([stats_df, adj_pvals], axis=0)
        return stats_df.sort_index(level=0)

    def _calculate_model_stats(
        self,
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
        name = "stats"
        results = []
        if pnl is not None:
            results.append(
                self.stats_computer.compute_stats(
                    pnl, time_series_type="pnl"
                ).rename(name)
            )
        if pnl is not None and returns is not None:
            corr = pd.Series(
                pnl.corr(returns), index=["corr_to_underlying"], name=name
            )
            results.append(pd.concat([corr], keys=["correlation"]))
        if positions is not None and returns is not None:
            bets = self.stats_computer.compute_bet_stats(
                positions=positions, returns=returns[positions.index]
            )
            bets.name = name
            results.append(pd.concat([bets], keys=["bets"]))
            # TODO(*): Use `predictions` instead.
            corr = pd.Series(
                positions.corr(returns), index=["prediction_corr"], name=name
            )
            results.append(pd.concat([corr], keys=["correlation"]))
        if positions is not None:
            finance = self.stats_computer.compute_finance_stats(
                positions, time_series_type="positions"
            )
            results.append(pd.concat([finance], keys=["finance"]))
        # Z-score OOS SRs.
        if oos_start is not None and pnl is not None:
            dbg.dassert(pnl[:oos_start].any())
            dbg.dassert(pnl[oos_start:].any())
            oos_sr = stats.zscore_oos_sharpe_ratio(pnl, oos_start).rename(name)
            results.append(pd.concat([oos_sr]), keys=["signal_quality"])
        result = pd.concat(results, axis=0)
        return result.sort_index(level=0)

    def _get_series_as_df(
        self,
        series: str,
        keys: List[Any],
        mode: str,
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

        Rescales to target volatility over in-sample period (if
        provided).
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
                strategy="rescale",
            )
            position_dict[key] = positions
        return position_dict

    @staticmethod
    def _calculate_pnls(
        returns: Dict[Any, pd.Series],
        positions: Dict[Any, pd.Series],
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

    @staticmethod
    def _dump_series_to_json(srs: pd.Series) -> str:
        srs = srs.copy()
        srs.index = srs.index.astype(str)
        return srs.to_json()

    @staticmethod
    def _load_series_from_json(json_srs: str) -> pd.Series:
        srs = json.loads(json_srs)
        srs = pd.Series(srs)
        srs.index = pd.to_datetime(srs.index)
        if srs.shape[0] > 2:
            srs.index.freq = pd.infer_freq(srs.index)
        return srs

    @staticmethod
    def _dump_series_dict_to_json_dict(
        series_dict: Optional[Dict[Any, pd.Series]]
    ) -> Optional[Dict[Any, str]]:
        if series_dict is None:
            json_dict = None
        else:
            json_dict = {
                key: ModelEvaluator._dump_series_to_json(srs)
                for key, srs in series_dict.items()
            }
        return json_dict

    @staticmethod
    def _load_series_dict_from_json_dict(
        json_dict: Optional[Dict[str, str]], keys_to_int: bool = True
    ) -> Optional[Dict[Union[str, int], pd.Series]]:
        if json_dict is None:
            return None
        series_dict = {
            key: ModelEvaluator._load_series_from_json(srs)
            for key, srs in json_dict.items()
        }
        if keys_to_int:
            series_dict = {int(key): srs for key, srs in series_dict.items()}
        return series_dict


def build_model_evaluator_from_df(
    df: pd.DataFrame,
    returns_col_group: tuple,
    predictions_col_group: tuple,
    target_volatility: Optional[float] = None,
    oos_start: Optional[Any] = None,
) -> ModelEvaluator:
    """
    Build a `ModelEvaluator` from a multiindexed column dataframe.

    :param df: multiindexed column dataframe
    :param returns_col_group: df.columns.nlevels - 1 depth tuple
    :param predictions_col_group: df.columns.nlevels - 1 depth tuple
    :param target_volatility: as in `ModelEvaluator`
    :param oos_start: as in `ModelEvaluator`
    :return: initialized `ModelEvaluator`
    """
    dbg.dassert_isinstance(df, pd.DataFrame)
    dbg.dassert_lt(1, df.columns.nlevels)
    returns = df[returns_col_group].to_dict(orient="series")
    predictions = df[predictions_col_group].to_dict(orient="series")
    model_evaluator = ModelEvaluator(
        returns=returns,
        predictions=predictions,
        target_volatility=target_volatility,
        oos_start=oos_start,
    )
    return model_evaluator


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

    def _multiply_kernel(
        self,
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

    def _squash(
        self,
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
        else:
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


def build_model_evaluator_from_result_bundle_dicts(
    result_bundle_dicts: collections.OrderedDict,
    returns_col: str,
    predictions_col: str,
    target_volatility: Optional[float] = None,
    oos_start: Optional[Any] = None,
) -> ModelEvaluator:
    """
    :param result_bundle_dicts: dict of `ResultBundle`s, each of which was
        generated using `ResultBundle.to_dict()`
    :param returns_col: column of `ResultBundle.result_df` to use as the column
        representing returns
    :param predictions_col: like `returns_col`, but for predictions
    :param target_volatility: as in `ModelEvaluator`
    :param oos_start: as in `ModelEvaluator`
    :return: `ModelEvaluator` initialized with returns and predictions from
       result bundles
    """
    # Convert each `ResultBundle` dict into a `ResultBundle` class object.
    result_bundles = {
        k: cdataf.ResultBundle.from_dict(v)
        for k, v in result_bundle_dicts.items()
    }
    # Extract returns and predictions from result bundles.
    returns = {}
    predictions = {}
    for key, rb in result_bundles.items():
        df = rb.result_df
        dbg.dassert_in(returns_col, df.columns)
        # TODO(Paul): if `rb` is a `PredictionResultBundle`, we should warn if
        #     if the provided `returns_col` disagrees with any that the bundle
        #     provides. We may also want to load the provided one by default.
        returns[key] = df[returns_col]
        dbg.dassert_in(predictions_col, df.columns)
        # TODO(Paul): Same as for `returns_col`.
        predictions[key] = df[predictions_col]
    # Initialize `ModelEvaluator`.
    evaluator = ModelEvaluator(
        returns=returns,
        predictions=predictions,
        target_volatility=target_volatility,
        oos_start=oos_start,
    )
    return evaluator
