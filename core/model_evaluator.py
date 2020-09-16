"""Import as:

import core.model_evaluator as modeval
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.auto import tqdm

import core.finance as fin
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class ModelEvaluator:
    """

    """

    def __init__(
        self, returns: Dict[Any, pd.Series], predictions: Dict[Any, pd.Series]
    ) -> None:
        """

        TODO: Add optional target volatility and OOS start.

        :param returns:
        :param prediction:
        """
        self.valid_keys = self._get_valid_keys(returns, predictions)
        self.rets = {k: returns[k] for k in self.valid_keys}
        self.preds = {k: predictions[k] for k in self.valid_keys}
        # Calculate pnl streams.
        # TODO(*): Allow configurable strategies.
        # TODO(*): Maybe required that this be called instead of always doing it.
        self.pnls = self._calculate_pnls()

    def get_pnls(
        self,
        keys: Optional[List[Any]] = None,
        target_volatility: Optional[float] = None,
        oos_start: Optional[pd.datetime] = None,
        mode: Optional[str] = None,
    ):
        """
        Calculate PnLs from `returns` and `predictions`

        :param keys: Use all available if `None`
        :param target_volatility: Rescale PnLs to value over in-sample period
        :param oos_start: Start of out-of-sample window (end of in-sample).
            This parameter has no effect if `target_volatility` is `None`.
        :return: Dictionary of rescaled PnL curves
        """
        keys = keys or self.valid_keys
        dbg.dassert_isinstance(keys, list)
        dbg.dassert_is_subset(keys, self.valid_keys)
        mode = mode or "all_available"
        if oos_start is not None:
            insample_pnls = {k: self.pnls[k].loc[:oos_start] for k in keys}
        else:
            insample_pnls = self.pnls
        if target_volatility is not None:
            scale_factors = {
                k: fin.compute_volatility_normalization_factor(
                    srs=insample_pnls[k], target_volatility=target_volatility
                )
                for k in keys
            }
        else:
            scale_factors = {k: 1.0 for k in keys}
        rescaled_pnls = {k: scale_factors[k] * self.pnls[k] for k in keys}
        # NOTE: ins/oos overlap by one point as-is (consider changing).
        if mode == "all_available":
            return rescaled_pnls
        elif mode == "ins":
            return {k: v.loc[:oos_start] for k, v in rescaled_pnls.items()}
        elif mode == "oos":
            return {k: v.loc[oos_start:] for k, v in rescaled_pnls.items()}
        else:
            raise ValueError(f"Unrecognized mode {mode}.")

    def _calculate_pnls(self) -> Dict[Any, pd.Series]:
        """
        TODO(*): Add volatility adjustment over period.

        :return:
        """
        pnls = {}
        for key in tqdm(self.valid_keys):
            pnl = self.rets[key].multiply(self.preds[key])
            dbg.dassert(pnl.index.freq)
            pnls[key] = pnl
        return pnls

    def _get_valid_keys(
        self, returns: Dict[Any, pd.Series], predictions: Dict[Any, pd.Series]
    ) -> set:
        """
        Perform basic sanity checks.

        :param returns:
        :param predictions:
        :return:
        """
        rets_keys = set(self._get_valid_keys_helper(returns))
        preds_keys = set(self._get_valid_keys_helper(predictions))
        shared_keys = rets_keys.intersection(preds_keys)
        dbg.dassert(shared_keys, msg="Set of valid keys must be nonempty!")
        for key in shared_keys:
            dbg.dassert_eq(returns[key].index.freq, predictions[key].index.freq)
        return shared_keys

    def _get_valid_keys_helper(self, input_dict: Dict[Any, pd.Series]):
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
            if v.index.freq is None:
                _LOG.warning("No `freq` for series for `k`=%s", str(k))
                continue
            valid_keys.append(k)
        return valid_keys
