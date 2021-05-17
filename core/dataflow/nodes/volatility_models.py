import collections
import logging
from typing import Dict, Optional

import pandas as pd

import core.finance as cfinan
import helpers.dbg as dbg
from core.dataflow.nodes.base import ColModeMixin, FitPredictNode

_LOG = logging.getLogger(__name__)


# #############################################################################
# Volatility modeling
# #############################################################################


class VolatilityNormalizer(FitPredictNode, ColModeMixin):
    def __init__(
        self,
        nid: str,
        col: str,
        target_volatility: float,
        col_mode: Optional[str] = None,
    ) -> None:
        """
        Normalize series to target annual volatility.

        :param nid: node identifier
        :param col: name of column to rescale
        :param target_volatility: target volatility as a proportion
        :param col_mode: `merge_all` or `replace_all`. If `replace_all`, return
            only the rescaled column, if `merge_all`, append the rescaled
            column to input dataframe
        """
        super().__init__(nid)
        self._col = col
        self._target_volatility = target_volatility
        self._col_mode = col_mode or "merge_all"
        dbg.dassert_in(
            self._col_mode,
            ["merge_all", "replace_all"],
            "Invalid `col_mode`='%s'",
            self._col_mode,
        )
        self._scale_factor: Optional[float] = None

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_in(self._col, df_in.columns)
        self._scale_factor = cfinan.compute_volatility_normalization_factor(
            df_in[self._col], self._target_volatility
        )
        rescaled_y_hat = self._scale_factor * df_in[self._col]
        df_out = self._apply_col_mode(
            df_in,
            rescaled_y_hat.to_frame(),
            cols=[self._col],
            col_rename_func=lambda x: f"rescaled_{x}",
            col_mode=self._col_mode,
        )
        # Store info.
        info = collections.OrderedDict()
        info["scale_factor"] = self._scale_factor
        self._set_info("fit", info)
        return {"df_out": df_out}

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        dbg.dassert_in(self._col, df_in.columns)
        rescaled_y_hat = self._scale_factor * df_in[self._col]
        df_out = self._apply_col_mode(
            df_in,
            rescaled_y_hat.to_frame(),
            cols=[self._col],
            col_rename_func=lambda x: f"rescaled_{x}",
            col_mode=self._col_mode,
        )
        return {"df_out": df_out}
