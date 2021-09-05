"""
Import as:

import core.dataflow_model.model_evaluator as cdtfmomoev
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import core.config as cconfig
import core.dataflow as cdataf
import core.dataflow_model.stats_computer as cdtfmostco
import core.dataflow_model.utils as cdtfmouti
import core.finance as cofinanc
import core.signal_processing as csigproc
import core.statistics as costatis
import helpers.dbg as hdbg
import helpers.introspection as hintros

_LOG = logging.getLogger(__name__)

# _LOG.debug = _LOG.info

# #############################################################################
# StrategyEvaluator
# #############################################################################

# Each model / experiment is represented by a key, encoded as an int.
Key = int


# TODO(Paul): Deprecate.
class StrategyEvaluator:
    """
    Evaluate the performance of a strategy driven by an alpha.
    """

    def __init__(
        self,
        data: Dict[Key, pd.DataFrame],
        position_intent_col: str,
        returns_col: str,
        spread_col: str,
        # TODO(Paul): Allow specification of start and end times for stats.
        # This is useful for interactive analysis and/or zooming in on a
        # specific time period.
        # start: Optional[pd.Timestamp] = None,
        # end: Optional[pd.Timestamp] = None,
    ) -> None:
        """
        Constructor.

        The passed time series need to have a proper alignment.
        TODO(gp): Check with Paul and specify this clearly.

        :param data: same meaning as in `ModelEvaluator`
        :param position_intent_col: column storing the position intent for the
            corresponding returns. This is derived from the alpha of the model
        :param returns_col: column with the return that we are predicting
        :param spread_col: column with the spread associated to the corresponding
            `returns_col`
        """
        self._data = data
        hdbg.dassert(data, msg="Data set must be nonempty")
        # This is required by the current implementation otherwise when we extract
        # columns from dataframes we get dataframes and not series.
        hdbg.dassert_ne(
            returns_col,
            spread_col,
        )
        self.position_intent_col = position_intent_col
        self.returns_col = returns_col
        self.spread_col = spread_col
        # self.start = start
        # self.end = end
        # The valid keys are the keys in the data dict.
        self.valid_keys = list(self._data.keys())
        self._stats_computer = cdtfmostco.StatsComputer()

    # TODO(Paul): This looks like the corresponding method for `ModelEvaluator`
    #  except for the columns needed. Factor out the common part.
    @classmethod
    def from_result_bundle_dict(
        cls,
        result_bundle_dict: Dict[Key, cdataf.ResultBundle],
        position_intent_col: str,
        returns_col: str,
        spread_col: str,
        abort_on_error: bool = True,
    ) -> StrategyEvaluator:
        """
        Initialize a `StrategyEvaluator` from a result bundle dictionary.
        """
        _LOG.info(
            "Before building StrategyEvaluator: memory_usage=%s",
            hdbg.get_memory_usage_as_str(None),
        )
        data_dict: Dict[Key, pd.DataFrame] = {}
        # Convert each `ResultBundle` dict into a `ResultBundle` class object.
        for key, result_bundle in result_bundle_dict.items():
            _LOG.debug("Loading key=%s", key)
            try:
                _LOG.debug("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
                df = result_bundle.result_df
                hdbg.dassert_is_not(df, None)
                _LOG.debug(
                    "result_df.memory_usage=%s",
                    hintros.format_size(
                        df.memory_usage(index=True, deep=True).sum()
                    ),
                )
                # Extract the needed columns.
                for col in (position_intent_col, returns_col, spread_col):
                    hdbg.dassert_in(col, df.columns)
                hdbg.dassert_not_in(key, data_dict.keys())
                df_tmp = df[[position_intent_col, returns_col, spread_col]]
                data_dict[key] = df_tmp
            except Exception as e:
                _LOG.error(
                    "Error while loading ResultBundle for config %s with exception:\n%s"
                    % (key, str(e))
                )
                if abort_on_error:
                    raise e
                else:
                    _LOG.warning("Continuing as per user request")
        # Initialize `StrategyEvaluator`.
        evaluator = cls(
            data=data_dict,
            position_intent_col=position_intent_col,
            returns_col=returns_col,
            spread_col=spread_col,
        )
        _LOG.info(
            "After building StrategyEvaluator: memory_usage=%s",
            hdbg.get_memory_usage_as_str(None),
        )
        return evaluator

    @classmethod
    def from_eval_config(cls, eval_config: cconfig.Config) -> "StrategyEvaluator":
        load_config = eval_config["load_experiment_kwargs"].to_dict()
        # Load only the columns needed by the StrategyEvaluator.
        load_config["load_rb_kwargs"] = {
            "columns": [
                eval_config["strategy_evaluator_kwargs"]["returns_col"],
                eval_config["strategy_evaluator_kwargs"]["position_intent_col"],
                eval_config["strategy_evaluator_kwargs"]["spread_col"],
            ]
        }
        result_bundle_dict = cdtfmouti.load_experiment_artifacts(**load_config)
        # Build the StrategyEvaluator.
        evaluator = StrategyEvaluator.from_result_bundle_dict(
            result_bundle_dict,
            **eval_config["strategy_evaluator_kwargs"].to_dict(),
        )
        return evaluator

    # TODO(gp): Maybe we should separate the pivoting logic in a different function
    #  to avoid to complicate the types.
    def compute_pnl(
        self,
        spread_fraction_paid: float,
        keys: Optional[List[Key]] = None,
        key_type: str = "instrument",
    ) -> Dict[Union[Key, str], pd.DataFrame]:
        """
        Compute PnL from position intents, ret_0, and spread.

        :param keys: use all available keys if `None`
        :param spread_fraction_paid: same interpretation as in `compute_spread_cost()`
        :param key_type: how to index the output data structure (e.g., by instrument
            or by attribute)
        """
        # TODO(gp): Add some logic to cache this data and not recompute.
        _LOG.info(
            "Before StrategyEvaluator.compute_pnl: memory_usage=%s",
            hdbg.get_memory_usage_as_str(None),
        )
        keys = keys or self.valid_keys
        hdbg.dassert_is_subset(keys, self.valid_keys)
        # Build the dict from experiment key to dataframe.
        pnl_dict: Dict[Union[Key, str], pd.DataFrame] = {}
        for key in tqdm(keys):
            _LOG.debug("Process key=%s", key)
            # Extract the needed data from the current dataframe.
            for col in (
                self.returns_col,
                self.position_intent_col,
                self.spread_col,
            ):
                hdbg.dassert_in(col, self._data[key].columns)
            df = self._data[key][
                [self.returns_col, self.position_intent_col, self.spread_col]
            ]
            # TODO(gp): Not sure about renaming the columns, since it might prevent
            #  re-running on the same data. It might be simpler to leave the data
            #  as it is and use meaningful vars for the cols, e.g.,
            #  `ret_0 = self.returns_col`.
            df.rename(
                columns={
                    self.returns_col: "ret_0",
                    self.position_intent_col: "position_intent_1",
                    self.spread_col: "spread_0",
                },
                inplace=True,
            )
            # Compute PnL.
            pnl = (
                cofinanc.compute_pnl(
                    df,
                    position_intent_col="position_intent_1",
                    return_col="ret_0",
                )
                .squeeze()
                .rename("pnl_0")
            )
            df["pnl_0"] = pnl
            # Compute spread cost.
            spread_cost = (
                cofinanc.compute_spread_cost(
                    df,
                    target_position_col="position_intent_1",
                    spread_col="spread_0",
                    spread_fraction_paid=spread_fraction_paid,
                )
                .squeeze()
                .rename("spread_cost_0")
            )
            # Add the info.
            df["spread_cost_0"] = spread_cost
            # TODO(gp): This is simple enough that we should recompute it from
            # the columns, to avoid using memory.
            df["ex_cost_pnl_0"] = pnl - spread_cost
            pnl_dict[key] = df
        # Organize the resulting output.
        if key_type == "instrument":
            pass
        elif key_type == "attribute":
            pnl_dict_pivoted = {}
            for attribute in [
                "ret_0",
                "position_intent_1",
                "spread_0",
                "spread_cost_0",
                "pnl_0",
                "ex_cost_pnl_0",
            ]:
                data = []
                for key in pnl_dict.keys():
                    data.append(pnl_dict[key][attribute].rename(key))
                df = pd.concat(data, axis=1)
                pnl_dict_pivoted[attribute] = df
            pnl_dict = pnl_dict_pivoted
        else:
            raise ValueError("Invalid key_type='%s'" % key_type)
        _LOG.info("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
        return pnl_dict

    def calculate_stats(
        self,
        keys: Optional[List[Any]] = None,
        spread_fraction_paid: float = 0,
    ) -> pd.DataFrame:
        """
        Calculate performance characteristics of selected models.

        :param keys: use all available if `None`
        :return: dataframe of statistics with `keys` as columns
        """
        pnl_dict = self.compute_pnl(
            spread_fraction_paid,
            keys=keys,
        )
        #
        stats_dict = {}
        for key in tqdm(pnl_dict.keys(), desc="Calculating stats"):
            _LOG.debug("key=%s", key)
            pnl_df = pnl_dict[key]
            if pnl_df.empty:
                _LOG.warning("PnL series for key=%i is empty", key)
                continue
            if pnl_df.dropna().empty:
                _LOG.warning("PnL series for key=%i is all-NaN", key)
                continue
            # Compute PnL without spread cost.
            ex_cost_pnl_0 = pnl_df["pnl_0"] - pnl_df["spread_cost_0"]
            ex_cost_pnl_0 = ex_cost_pnl_0.to_frame().rename("ex_cost_pnl_0")
            stats_dict[key] = self._stats_computer.compute_finance_stats(
                ex_cost_pnl_0,
                pnl_col="ex_cost_pnl_0",
            )
        # TODO(gp): Factor out this piece since it's common to `ModelEvaluator`.
        stats_df = pd.concat(stats_dict, axis=1)
        # Calculate BH adjustment of pvals.
        adj_pvals = costatis.multipletests(
            stats_df.loc["signal_quality"].loc["sr.pval"], nan_mode="drop"
        ).rename("sr.adj_pval")
        adj_pvals = pd.concat(
            [adj_pvals.to_frame().transpose()], keys=["signal_quality"]
        )
        stats_df = pd.concat([stats_df, adj_pvals], axis=0)
        _LOG.info("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
        return stats_df


# #############################################################################
# ModelEvaluator
# #############################################################################

# A model / experiment is represented by a key, encoded as a string.
Key = str


# TODO(Paul): Deprecate.
class ModelEvaluator:
    """
    Evaluate returns predictions.
    """

    def __init__(
        self,
        # TODO(gp): data -> df_dict or data_dict? Make it uniform across the code.
        data: Dict[Key, pd.DataFrame],
        *,
        prediction_col: str,
        target_col: str,
        oos_start: Optional[pd.Timestamp],
    ) -> None:
        """
        Constructor.

        The `prediction_col` and `target_col` should be aligned, i.e., the
        prediction at a given index location should be a prediction for the
        target at that same index location.

        :param data: a dict key (tag of model / experiment) -> dataframe (containing
            `ResultBundle.result_df`). Each model / experiment is represented by
            a key.

            E.g.,

            ```
            {0:                            vwap_ret_0_vol_adj_clipped_2 ...
             end_time
             2009-01-02 09:05:00-05:00                              NaN ...
             2009-01-02 09:10:00-05:00                              NaN ...
             2009-01-02 09:15:00-05:00                              NaN ...
            ```

        :param prediction_col: column of to use as predictions
        :param target_col: column of to use as targets (e.g., returns)
        :param oos_start: start of the OOS period, or None for nothing
        """
        self._data = data
        hdbg.dassert(data, msg="Data set must be nonempty")
        # This is required by the current implementation otherwise when we extract
        # columns from dataframes we get dataframes and not series.
        hdbg.dassert_ne(
            prediction_col,
            target_col,
            "Prediction and target columns need to be different",
        )
        self.prediction_col = prediction_col
        self.target_col = target_col
        self.oos_start = oos_start
        # The valid keys are the keys in the data dict.
        self.valid_keys = list(self._data.keys())
        # TODO(gp): This is used only in `calculate_stats`, so it doesn't have to be
        #  part of the state.
        self._stats_computer = cdtfmostco.StatsComputer()

    @classmethod
    def from_result_bundle_dict(
        cls,
        result_bundle_dict: Dict[Key, cdataf.ResultBundle],
        predictions_col: str,
        target_col: str,
        oos_start: Optional[pd.Timestamp],
        abort_on_error: bool = True,
    ) -> ModelEvaluator:
        """
        Initialize a `ModelEvaluator` from a dictionary `key` ->
        `ResultBundle`.

        :param result_bundle_dict: mapping from key to `ResultBundle`
        :param *: as in `ModelEvaluator` constructor
        :return: `ModelEvaluator` initialized with returns and predictions from
           result bundles
        """
        _LOG.info(
            "Before building ModelEvaluator: memory_usage=%s",
            hdbg.get_memory_usage_as_str(None),
        )
        data_dict: Dict[Key, pd.DataFrame] = {}
        # Convert each `ResultBundle` dict into a `ResultBundle` class object.
        for key, result_bundle in result_bundle_dict.items():
            _LOG.debug("Loading key=%s", key)
            try:
                _LOG.debug("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
                df = result_bundle.result_df
                hdbg.dassert_is_not(df, None)
                _LOG.debug(
                    "result_df.memory_usage=%s",
                    hintros.format_size(
                        df.memory_usage(index=True, deep=True).sum()
                    ),
                )
                # Extract the needed columns.
                hdbg.dassert_in(target_col, df.columns)
                hdbg.dassert_in(predictions_col, df.columns)
                hdbg.dassert_not_in(key, data_dict.keys())
                data_dict[key] = df[[target_col, predictions_col]]
            except Exception as e:
                _LOG.error(
                    "Error while loading ResultBundle for config %s with exception:\n%s"
                    % (key, str(e))
                )
                if abort_on_error:
                    raise e
                else:
                    _LOG.warning("Continuing as per user request")
        # Initialize `ModelEvaluator`.
        evaluator = cls(
            data=data_dict,
            prediction_col=predictions_col,
            target_col=target_col,
            oos_start=oos_start,
        )
        _LOG.info(
            "After building ModelEvaluator: memory_usage=%s",
            hdbg.get_memory_usage_as_str(None),
        )
        return evaluator

    @classmethod
    def from_eval_config(
        cls,
        eval_config: cconfig.Config,
    ) -> ModelEvaluator:
        """
        Initialize a `ModelEvaluator` from an eval config.
        """
        load_config = eval_config["load_experiment_kwargs"].to_dict()
        # Load only the columns needed by the ModelEvaluator.
        load_config["load_rb_kwargs"] = {
            "columns": [
                eval_config["model_evaluator_kwargs"]["target_col"],
                eval_config["model_evaluator_kwargs"]["predictions_col"],
            ]
        }
        result_bundle_dict = cdtfmouti.load_experiment_artifacts(**load_config)
        # Build the ModelEvaluator.
        evaluator = ModelEvaluator.from_result_bundle_dict(
            result_bundle_dict,
            **eval_config["model_evaluator_kwargs"].to_dict(),
        )
        return evaluator

    # TODO(gp): Maybe `resolve_keys()` is a better name.
    def get_keys(self, keys: Optional[List[Key]]) -> List[Any]:
        """
        Return the keys to select models, or all available keys for
        `keys=None`.
        """
        keys = keys or self.valid_keys
        hdbg.dassert_is_subset(keys, self.valid_keys)
        return keys

    def aggregate_models(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        position_method: Optional[str] = None,
        target_volatility: Optional[float] = None,
        returns_shift: Optional[int] = 0,
        predictions_shift: Optional[int] = 0,
        mode: Optional[str] = None,
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Combine PnLs selected through `keys`.

        :param keys: use all available keys if `None`
        :param weights: average if `None`
        :param position_method: as in `PositionComputer.compute_positions()`
        :param target_volatility: as in `PositionComputer.compute_positions()`
        :param returns_shift: as in `compute_pnl()`
        :param predictions_shift: as in `compute_pnl()`
        :param mode: "all_available", "ins" (default), or "oos"
        :return: aggregate pnl stream, position stream, statistics
        """
        keys = self.get_keys(keys)
        mode = mode or "ins"
        pnl_dict = self.compute_pnl(
            keys=keys,
            position_method=position_method,
            mode=mode,
            returns_shift=returns_shift,
            predictions_shift=predictions_shift,
        )
        pnl_df = pd.concat({k: v["pnl"] for k, v in pnl_dict.items()}, axis=1)
        # Get the weights.
        weights = weights or [1 / len(keys)] * len(keys)
        hdbg.dassert_eq(len(keys), len(weights))
        col_map = {keys[idx]: weights[idx] for idx in range(len(keys))}
        # Calculate PnL series.
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
            scale_factor = cofinanc.compute_volatility_normalization_factor(
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
        _LOG.info("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
        return pnl_srs, pos_srs, aggregate_stats

    # TODO(gp): This is second.
    def calculate_stats(
        self,
        keys: Optional[List[Any]] = None,
        position_method: Optional[str] = None,
        target_volatility: Optional[float] = None,
        returns_shift: Optional[int] = 0,
        predictions_shift: Optional[int] = 0,
        mode: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Calculate performance characteristics of selected models.

        :param keys: Use all available if `None`
        :param position_method: as in `PositionComputer.compute_positions()`
        :param target_volatility: as in `PositionComputer.compute_positions()`
        :param returns_shift: as in `compute_pnl()`
        :param predictions_shift: as in `compute_pnl()`
        :param mode: "all_available", "ins", or "oos"
        :return: Dataframe of statistics with `keys` as columns
        """
        #
        pnl_dict = self.compute_pnl(
            keys,
            position_method=position_method,
            target_volatility=target_volatility,
            returns_shift=returns_shift,
            predictions_shift=predictions_shift,
            mode=mode,
        )
        stats_dict = {}
        for key in tqdm(pnl_dict.keys(), desc="Calculating stats"):
            _LOG.debug("key=%s", key)
            if pnl_dict[key].empty:
                _LOG.warning("PnL series for key=%i is empty", key)
                continue
            if pnl_dict[key].dropna().empty:
                _LOG.warning("PnL series for key=%i is all-NaN", key)
                continue
            stats_dict[key] = self._stats_computer.compute_finance_stats(
                pnl_dict[key],
                returns_col="returns",
                predictions_col="predictions",
                positions_col="positions",
                pnl_col="pnl",
            )
        stats_df = pd.concat(stats_dict, axis=1)
        # Calculate BH adjustment of pvals.
        adj_pvals = costatis.multipletests(
            stats_df.loc["signal_quality"].loc["sr.pval"], nan_mode="drop"
        ).rename("sr.adj_pval")
        adj_pvals = pd.concat(
            [adj_pvals.to_frame().transpose()], keys=["signal_quality"]
        )
        stats_df = pd.concat([stats_df, adj_pvals], axis=0)
        _LOG.info("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
        return stats_df

    # TODO(gp): This is first.
    def compute_pnl(
        self,
        keys: Optional[List[Key]] = None,
        position_method: Optional[str] = None,
        target_volatility: Optional[float] = None,
        returns_shift: Optional[int] = 0,
        predictions_shift: Optional[int] = 0,
        mode: Optional[str] = None,
    ) -> Dict[Any, pd.DataFrame]:
        """
        Helper for calculating positions and PnL from returns and predictions.

        :param keys: use all available models if `None`
        :param position_method: as in `PositionComputer.compute_positions()`
        :param target_volatility: as in `PositionComputer.compute_positions()`
        :param returns_shift: number of shifts to pre-apply to returns col
        :param predictions_shift: number of shifts to pre-apply to predictions
            col
        :param mode: "all_available", "ins", or "oos"
        :return: dict of dataframes with columns ["returns", "predictions",
            "positions", "pnl"]
        """
        keys = self.get_keys(keys)
        # Extract and align the returns.
        _LOG.debug("Process returns")
        returns = {}
        for key in keys:
            hdbg.dassert_in(self.target_col, self._data[key].columns)
            srs = self._data[key][self.target_col]
            _validate_series(srs)
            hdbg.dassert_lte(0, returns_shift)
            srs = srs.shift(returns_shift)
            srs.name = "returns"
            _validate_series(srs)
            returns[key] = srs
        # Extract and align the predictions.
        _LOG.debug("Process predictions")
        predictions = {}
        for key in keys:
            hdbg.dassert_in(self.prediction_col, self._data[key].columns)
            srs = self._data[key][self.prediction_col]
            _validate_series(srs)
            hdbg.dassert_lte(0, predictions_shift)
            srs = srs.shift(predictions_shift)
            srs.name = "predictions"
            _validate_series(srs)
            predictions[key] = srs
        # Compute the positions.
        _LOG.debug("Process positions")
        positions = {}
        for key in tqdm(returns.keys(), "Calculating positions"):
            _LOG.debug("Process key=%s", key)
            position_computer = PositionComputer(
                returns=returns[key],
                predictions=predictions[key],
            )
            positions[key] = position_computer.compute_positions(
                prediction_strategy=position_method,
                target_volatility=target_volatility,
            ).rename("positions")
        # Compute PnLs.
        _LOG.debug("Process PnLs")
        pnls = {}
        for key in tqdm(positions.keys(), "Calculating PnL"):
            _LOG.debug("Process key=%s", key)
            pnl_computer = PnlComputer(
                returns=returns[key],
                positions=positions[key],
            )
            pnls[key] = pnl_computer.compute_pnl().rename("pnl")
        # Assemble the results into a dictionary of dataframes.
        _LOG.debug("Assemble results into pnl_dict")
        pnl_dict = {}
        for key in keys:
            pnl_dict[key] = pd.concat(
                [returns[key], predictions[key], positions[key], pnls[key]],
                axis=1,
            )
        _LOG.debug("Trim pnl_dict")
        pnl_dict = self._trim_time_range(pnl_dict, mode=mode)
        _LOG.info("memory_usage=%s", hdbg.get_memory_usage_as_str(None))
        return pnl_dict

    # TODO(gp): Maybe trim when they are generated so we can discard.
    def _trim_time_range(
        self,
        data_dict: Dict[Key, Union[pd.Series, pd.DataFrame]],
        mode: Optional[str] = None,
    ) -> Dict[Any, Union[pd.Series, pd.DataFrame]]:
        """
        Trim the dataframes in the data in-sample/out-of-sample region.
        """
        mode = mode or "ins"
        if mode == "all_available":
            trimmed = data_dict
        elif mode == "ins":
            trimmed = {k: v.loc[: self.oos_start] for k, v in data_dict.items()}
        elif mode == "oos":
            hdbg.dassert(self.oos_start, msg="No `oos_start` set!")
            trimmed = {k: v.loc[self.oos_start :] for k, v in data_dict.items()}
        else:
            raise ValueError(f"Unrecognized mode `{mode}`.")
        return trimmed


# #############################################################################
# PnlComputer
# #############################################################################


def _validate_series(srs: pd.Series, oos_start: Optional[float] = None) -> None:
    hdbg.dassert_isinstance(srs, pd.Series)
    hdbg.dassert(not srs.empty, "Empty series")
    hdbg.dassert(not srs.dropna().empty, "Series with only nans")
    if oos_start is not None:
        hdbg.dassert(
            not srs[:oos_start].dropna().empty,  # type: ignore[misc]
            "Empty in-sample series",
        )
        hdbg.dassert(
            not srs[oos_start:].dropna().empty,  # type: ignore[misc]
            "Empty OOS series",
        )
    hdbg.dassert(srs.index.freq)


# TODO(gp): This goes first.
# TODO(gp): -> _?
class PnlComputer:
    """
    Compute PnL from returns and positions (i.e., holdings).
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
        _validate_series(returns)
        self.returns = returns
        _validate_series(positions)
        self.positions = positions
        # TODO(Paul): validate indices by making sure they are the same.

    def compute_pnl(self) -> pd.Series:
        """
        Compute PnL from returns and positions.
        """
        pnl = self.returns.multiply(self.positions)
        _validate_series(pnl)
        pnl.name = "pnl"
        return pnl


# #############################################################################
# PositionComputer
# #############################################################################


# TODO(gp): This goes second.
# TODO(gp): -> _?
class PositionComputer:
    """
    Compute target positions from returns, predictions, and constraints.
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
        _validate_series(returns, self.oos_start)
        self.returns = returns
        _validate_series(predictions, self.oos_start)
        self.predictions = predictions

    # TODO(gp): Use None only when the default parameter needs to be propagated
    #  above in the call chain.
    # TODO(gp): "all_available" -> "all"
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

        :param target_volatility: generate positions to achieve target volatility
            on in-sample region
        :param prediction_strategy: "raw" (default), "kernel", "squash", "binarize"
        :param volatility_strategy: "rescale", "rolling" (not yet implemented)
        :param mode: "all_available", "ins", or "oos"
        :return: series of positions
        """
        mode = mode or "ins"
        # Process/adjust predictions.
        prediction_strategy = prediction_strategy or "raw"
        if prediction_strategy == "raw":
            # TODO(gp): Why this copy?
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
            # TODO(gp): Why this copy?
            positions = predictions.copy()
            positions.name = "positions"
            ret = self._return_srs(positions, mode=mode)
        else:
            ret = self._adjust_for_volatility(
                predictions,
                target_volatility,
                mode,
                volatility_strategy,
            )
        return ret

    @staticmethod
    def _multiply_kernel(
        predictions: pd.Series,
        tau: float,
        delay: int,
        z_mute_point: float,
        z_saturation_point: float,
    ) -> pd.Series:
        # z-score.
        zscored_preds = csigproc.compute_rolling_zscore(
            predictions, tau=tau, delay=delay
        )
        # Multiple by a kernel.
        bump_function = functools.partial(
            csigproc.c_infinity_bump_function,
            a=z_mute_point,
            b=z_saturation_point,
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
        zscored_preds = csigproc.compute_rolling_zscore(
            predictions, tau=tau, delay=delay
        )
        return csigproc.squash(zscored_preds, scale=scale)

    def _adjust_for_volatility(
        self,
        predictions: pd.Series,
        target_volatility: float,
        mode: str,
        volatility_strategy: str,
    ) -> pd.Series:
        """
        Adjust predictions to achieve a given target volatility.
        """
        # Compute PnL by interpreting predictions as positions.
        pnl_computer = PnlComputer(self.returns, predictions)
        pnl = pnl_computer.compute_pnl()
        pnl.name = "pnl"
        # Rescale in-sample.
        ins_pnl = pnl[: self.oos_start]  # type: ignore[misc]
        if volatility_strategy == "rescale":
            scale_factor = cofinanc.compute_volatility_normalization_factor(
                srs=ins_pnl, target_volatility=target_volatility
            )
            positions = scale_factor * predictions
            positions.name = "positions"
            ret = self._return_srs(positions, mode=mode)
        elif volatility_strategy == "rolling":
            raise NotImplementedError
        else:
            raise ValueError(f"Invalid strategy `{volatility_strategy}`")
        return ret

    # TODO(gp): -> _extract_srs
    # TODO(gp): Extract and reuse it.
    def _return_srs(self, srs: pd.Series, mode: str) -> pd.Series:
        """
        Extract part of the time series depending on which period is selected.

        :param mode: "ins", "oos", "all"
        """
        if mode == "ins":
            ret = srs[: self.oos_start]  # type: ignore[misc]
        elif mode == "oos":
            hdbg.dassert(
                self.oos_start,
                msg="Must set `oos_start` to run `oos`",
            )
            ret = srs[self.oos_start :]  # type: ignore[misc]
        elif mode == "all_available":
            ret = srs
        else:
            raise ValueError(f"Invalid mode `{mode}`")
        return ret
