import logging
from typing import Any, Dict

import pandas as pd
from tqdm.auto import tqdm

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
