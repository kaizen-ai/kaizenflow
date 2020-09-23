from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

import core.dataflow as dtf
import helpers.dbg as dbg
from core.dataflow.nodes import (
    ContinuousSkLearnModel,
    FitPredictNode,
    Residualizer,
    UnsupervisedSkLearnModel,
    VolatilityModel,
)

_LOG = logging.getLogger(__name__)


class DataFrameModeler:
    """
    Wraps common dataframe modeling and exploratory analysis functionality.

    TODO(*): Return a DataFrameModeler instead.

    TODO(*): Add
      - seasonal decomposition
      - stats (e.g., stationarity, autocorrelation)
      - correlation / clustering options
    """

    def __init__(
        self,
        df: pd.DataFrame,
        oos_start: Optional[float] = None,
        info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize by supplying a dataframe of time series.

        :param df: time series dataframe
        :param oos_start: Optional end of in-sample/start of out-of-sample.
            For methods supporting "fit"/"predict", "fit" applies to
            in-sample only, and "predict" requires `oos_start`.
        """
        dbg.dassert_isinstance(df, pd.DataFrame)
        dbg.dassert(pd.DataFrame)
        self.df = df
        self.oos_start = oos_start or None
        self.info = info or None

    def apply_sklearn_model(
        self,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        y_vars: Union[List[str], Callable[[], List[str]]],
        steps_ahead: int,
        model_kwargs: Optional[Any] = None,
        col_mode: Optional[str] = "merge_all",
        nan_mode: Optional[str] = "drop",
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Apply a supervised sklearn model.

        Both x and y vars should be indexed by knowledge time.
        """
        model = ContinuousSkLearnModel(
            nid="sklearn",
            model_func=model_func,
            x_vars=x_vars,
            y_vars=y_vars,
            steps_ahead=steps_ahead,
            model_kwargs=model_kwargs,
            col_mode=col_mode,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def apply_unsupervised_sklearn_model(
        self,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        model_kwargs: Optional[Any] = None,
        col_mode: Optional[str] = "merge_all",
        nan_mode: Optional[str] = "drop",
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Apply an unsupervised model, e.g., PCA.
        """
        model = UnsupervisedSkLearnModel(
            nid="unsupervised_sklearn",
            model_func=model_func,
            x_vars=x_vars,
            model_kwargs=model_kwargs,
            col_mode=col_mode,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def apply_residualizer(
        self,
        model_func: Callable[..., Any],
        x_vars: Union[List[str], Callable[[], List[str]]],
        model_kwargs: Optional[Any] = None,
        nan_mode: Optional[str] = "drop",
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Apply an unsupervised model and residualize.
        """
        model = Residualizer(
            nid="sklearn_residualizer",
            model_func=model_func,
            x_vars=x_vars,
            model_kwargs=model_kwargs,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def apply_sma_model(
        self,
        col: str,
        steps_ahead: int,
        tau: Optional[float] = None,
        nan_mode: Optional[str] = "drop",
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Apply a smooth moving average model.
        """
        model = dtf.SmaModel(
            nid="sma_model",
            col=[col],
            steps_ahead=steps_ahead,
            tau=tau,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def apply_volatility_model(
        self,
        col: str,
        steps_ahead: int,
        p_moment: float = 2,
        tau: Optional[float] = None,
        nan_mode: Optional[str] = "drop",
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Model volatility.
        """
        model = VolatilityModel(
            nid="volatility_model",
            col=[col],
            steps_ahead=steps_ahead,
            p_moment=p_moment,
            tau=tau,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def _run_model(self, model: FitPredictNode, method: str) -> DataFrameModeler:
        if method == "fit":
            df_out = model.fit(self.df[: self.oos_start])["df_out"]
            info = model.get_info("fit")
        elif method == "predict":
            model.fit(self.df[self.oos_start :])
            df_out = model.predict(self.df)["df_out"]
            info = model.get_info("predict")
        else:
            raise ValueError(f"Unrecognized method `{method}`.")
        dfm = DataFrameModeler(df_out, self.oos_start, info)
        return dfm
