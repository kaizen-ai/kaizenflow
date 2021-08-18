import collections
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import sklearn as sklear

import core.data_adapters as cdataa
import core.dataflow.core as cdtfc
import core.dataflow.nodes.base as cdnb
import core.dataflow.utils as cdtfu
import core.signal_processing as csigna
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class LinearRegression(cdnb.FitPredictNode, cdnb.ColModeMixin):
    """
    Fit and predict a linear regression model.
    """
    def __init__(
        self,
        nid: cdtfc.NodeId,
        x_vars: cdtfu.NodeColumnList,
        y_vars: cdtfu.NodeColumnList,
        steps_ahead: int,
        smoothing: Optional[float] = 0,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
        # TODO(*): Add support for weighted data.
        # sample_weight_col: Optional[cdtfu.NodeColumnList] = None,
    ) -> None:
        super().__init__(nid)
        self._x_vars = x_vars
        self._y_vars = y_vars
        self._fit_coefficients = None
        self._steps_ahead = steps_ahead
        self._smoothing = smoothing
        dbg.dassert_lte(
            0, self._steps_ahead, "Non-causal prediction attempted! Aborting..."
        )
        # NOTE: Set to "replace_all" for backward compatibility.
        self._col_mode = col_mode or "replace_all"
        dbg.dassert_in(self._col_mode, ["replace_all", "merge_all"])
        self._nan_mode = nan_mode or "raise"

    def fit(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=True)

    def predict(self, df_in: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._fit_predict_helper(df_in, fit=False)

    def get_fit_state(self) -> Dict[str, Any]:
        fit_state = {"_fit_coefficients": self._fit_coefficients, "_info['fit']": self._info["fit"]}
        return fit_state

    def set_fit_state(self, fit_state: Dict[str, Any]):
        self._fit_coefficients = fit_state["_fit_coefficients"]
        self._info["fit"] = fit_state["_info['fit']"]

    def _fit_predict_helper(
        self, df_in: pd.DataFrame, fit: True
    ) -> Dict[str, pd.DataFrame]:
        # Materialize names of x and y vars.
        x_vars = cdtfu.convert_to_list(self._x_vars)
        y_vars = cdtfu.convert_to_list(self._y_vars)
        # Get x and forward y df.
        if fit:
            # This df has no NaNs.
            df = cdtfu.get_x_and_forward_y_fit_df(
                df_in, x_vars, y_vars, self._steps_ahead
            )
        else:
            # This df has no `x_vars` NaNs.
            df = cdtfu.get_x_and_forward_y_predict_df(
                df_in, x_vars, y_vars, self._steps_ahead
            )
        # Handle presence of NaNs according to `nan_mode`.
        idx = df_in.index[: -self._steps_ahead] if fit else df_in.index
        self._handle_nans(idx, df.index)
        # Isolate the forward y piece of `df`.
        forward_y_cols = df.drop(
            x_vars, axis=1
        ).columns.to_list()
        dbg.dassert_eq(1, len(forward_y_cols))
        forward_y_col = forward_y_cols[0]
        coefficients = cstati.compute_regression_coefficients(
            df,
            x_vars,
            forward_y_col,
        )
        if fit:
            self._fit_coefficients = coefficients.copy()
        dbg.dassert(
            self._fit_coefficients is not None, "Model not found! Check if `fit()` has been run."
        )
        # Generate x_var weights.
        self._fit_coefficients["weight"] = self._fit_coefficients["beta"] / (
            self._fit_coefficients["turn"] ** self._smoothing
        )
        self._fit_coefficients["norm_weight"] = csigna.normalize(self._fit_coefficients["weight"])
        # Generate predictions.
        forward_y_hat = df[x_vars].multiply(self._fit_coefficients["weight"]).sum(axis=1)
        forward_y_hat_col = f"{forward_y_col}_hat"
        forward_y_hat = forward_y_hat.rename(forward_y_hat_col)
        info = collections.OrderedDict()
        info["fit_coefficients"] = self._fit_coefficients
        if not fit:
            info["predict_coefficients"] = coefficients
        df_out = df[[forward_y_col]].merge(
            forward_y_hat, how="outer", left_index=True, right_index=True
        )
        hat_coefficients = cstati.compute_regression_coefficients(
            df_out,
            [forward_y_hat_col],
            forward_y_col,
        )
        info["hat_coefficients"] = hat_coefficients
        df_out = df_out.reindex(idx)
        df_out = self._apply_col_mode(
            df_in,
            df_out,
            cols=y_vars,
            col_mode=self._col_mode,
        )
        info["df_out_info"] = cdtfu.get_df_info_as_string(df_out)
        mode = "fit" if fit else "predict"
        self._set_info(mode, info)
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
        else:
            raise ValueError(f"Unrecognized nan_mode `{self._nan_mode}`")

