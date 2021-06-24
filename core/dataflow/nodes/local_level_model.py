import collections
import logging
from typing import Callable, Dict, List, Optional, Union

import pandas as pd

import core.dataflow.nodes.base as cdnb
import core.dataflow.utils as cdu
import core.signal_processing as csigna
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


_COL_TYPE = Union[int, str]
_TO_LIST_MIXIN_TYPE = Union[List[_COL_TYPE], Callable[[], List[_COL_TYPE]]]


class LocalLevelModel(cdnb.FitPredictNode, cdnb.ColModeMixin):
    """
    Fit and predict a steady-state local level model.
    """

    def __init__(
        self,
        nid: str,
        cols: _TO_LIST_MIXIN_TYPE,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
    ) -> None:
        super().__init__(nid)
        self._cols = cols
        self._col_mode = col_mode
        if nan_mode is None:
            self._nan_mode = "raise"
        else:
            self._nan_mode = nan_mode
        self._tau = None

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=False)

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: bool
    ) -> Dict[str, pd.DataFrame]:
        cols = cdu.convert_to_list(self._cols)
        dbg.dassert_eq(
            len(cols), 1, msg="`LocalLevelModel` only supports a single column."
        )
        col = cols[0]
        srs = df_in[col]
        if self._nan_mode == "drop":
            srs = srs.dropna()
        idx = df_in.index
        self._handle_nans(idx, srs.index)
        # Calculate local-level model stats.
        stats = cstati.compute_local_level_model_stats(srs)
        com = stats["com"]
        tau = csigna.calculate_tau_from_com(com)
        if fit:
            self._tau = tau
        # Compute EWMA.
        _LOG.debug(f"Computing ewma with tau={self._tau}.")
        ewma = csigna.compute_smooth_moving_average(srs, tau=self._tau)
        ewma.name = str(col) + "_ewma"
        ewma = ewma.to_frame()
        ewma = ewma.reindex(idx)
        #
        info = collections.OrderedDict()
        info["stats"] = stats
        info["tau_from_input"] = tau
        info["tau_for_ema"] = self._tau
        df_out = self._apply_col_mode(
            df_in, ewma, cols=[col], col_mode=self._col_mode
        )
        method = "fit" if fit else "predict"
        self._set_info(method, info)
        return {"df_out": df_out}

    def _handle_nans(
        self, idx: pd.DataFrame.index, non_nan_idx: pd.DataFrame.index
    ) -> None:
        if self._nan_mode == "raise":
            if idx.shape[0] != non_nan_idx.shape[0]:
                nan_idx = idx.difference(non_nan_idx)
                raise ValueError(f"NaNs detected at {nan_idx}")
        elif self._nan_mode == "drop":
            pass
        elif self._nan_mode == "leave_unchanged":
            pass
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")
