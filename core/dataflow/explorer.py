from __future__ import annotations

import collections
import datetime
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import pandas as pd

import core.finance as fin
import core.signal_processing as sigp
import helpers.dbg as dbg
from core.dataflow.nodes import (
    ColumnTransformer,
    ContinuousSkLearnModel,
    FitPredictNode,
    Resample,
    Residualizer,
    SmaModel,
    UnsupervisedSkLearnModel,
    VolatilityModel,
)

_LOG = logging.getLogger(__name__)


class DataFrameModeler:
    """
    Wraps common dataframe modeling and exploratory analysis functionality.

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
        self._df = df
        self.oos_start = oos_start or None
        self.info = info or None

    @property
    def ins_df(self) -> pd.DataFrame:
        return self._df[:self.oos_start].copy()

    @property
    def oos_df(self) -> pd.DataFrame:
        dbg.dassert(self.oos_start, msg="`oos_start` must be set")
        return self._df[self.oos_start:].copy()

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    # #########################################################################
    # Dataflow nodes
    # #########################################################################

    def apply_column_transformer(
        self,
        transformer_func: Callable[..., pd.DataFrame],
        # TODO(Paul): Tighten this type annotation.
        transformer_kwargs: Optional[Any] = None,
        # TODO(Paul): May need to assume `List` instead.
        cols: Optional[Iterable[str]] = None,
        col_rename_func: Optional[Callable[[Any], Any]] = None,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Apply a function a a select of columns.
        """
        model = ColumnTransformer(
            nid="column_transformer",
            transformer_func=transformer_func,
            transformer_kwargs=transformer_kwargs,
            cols=cols,
            col_rename_func=col_rename_func,
            col_mode=col_mode,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def apply_resampler(
        self,
        rule: str,
        agg_func: str,
        resample_kwargs: Optional[Dict[str, Any]] = None,
        agg_func_kwargs: Optional[Dict[str, Any]] = None,
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Resample the dataframe (causally, by default).
        """
        agg_func_kwargs = agg_func_kwargs or {"min_count": 1}
        model = Resample(
            nid="resample",
            rule=rule,
            agg_func=agg_func,
            resample_kwargs=resample_kwargs,
            agg_func_kwargs=agg_func_kwargs,
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
        model = SmaModel(
            nid="sma_model",
            col=[col],
            steps_ahead=steps_ahead,
            tau=tau,
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

    # #########################################################################
    # Convenience methods
    # #########################################################################

    def compute_ret_0(
        self,
        rets_mode: str = "log_rets",
        cols: Optional[Iterable[str]] = None,
        col_rename_func: Optional[Callable[[Any], Any]] = None,
        col_mode: Optional[str] = None,
        nan_mode: Optional[str] = None,
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Calculate returns (realized at timestamp).
        """
        col_rename_func = col_rename_func or (lambda x: str(x) + "_ret_0")
        col_mode = col_mode or "replace_all"
        model = ColumnTransformer(
            nid="compute_ret_0",
            transformer_func=fin.compute_ret_0,
            transformer_kwargs={"mode": rets_mode},
            cols=cols,
            col_rename_func=col_rename_func,
            col_mode=col_mode,
            nan_mode=nan_mode,
        )
        return self._run_model(model, method)

    def set_non_ath_to_nan(
        self,
        start_time: Optional[datetime.time] = None,
        end_time: Optional[datetime.time] = None,
        method: str = "fit",
    ) -> DataFrameModeler:
        """
        Replace values at non active trading hours with NaNs.
        """
        model = ColumnTransformer(
            nid="set_non_ath_to_nan",
            transformer_func=fin.set_non_ath_to_nan,
            col_mode="replace_all",
            transformer_kwargs={"start_time": start_time, "end_time": end_time},
        )
        return self._run_model(model, method)

    def set_weekends_to_nan(self, method: str = "fit") -> DataFrameModeler:
        """
        Replace values over weekends with NaNs.
        """
        model = ColumnTransformer(
            nid="set_weekends_to_nan",
            transformer_func=fin.set_weekends_to_nan,
            col_mode="replace_all",
        )
        return self._run_model(model, method)

    def correlate_with_lag(self,
                           lag: int,
                           cols: Optional[Iterable[str]] = None,
                           method: str = "fit") -> DataFrameModeler:
        """
        Calculate correlation of `cols` with lags of `cols`.
        """
        # Validate column selection.
        cols = cols or self._df.columns
        dbg.dassert_is_subset(cols, self._df.columns)
        # Slice dataframe according to `fit` or `predict`.
        if method == "fit":
            df = self.ins_df[cols]
        elif method == "predict":
            df = self.df[cols]
        else:
            raise ValueError(f"Unrecognized method `{method}`.")
        # Calculate correlation.
        corr_df = sigp.correlate_with_lag(df, lag=lag)
        # Add info.
        method_info = collections.OrderedDict()
        method_info["n_samples"] = df.index.size
        method_info["correlation_matrix"] = corr_df
        info = collections.OrderedDict()
        info[method] = method_info
        return DataFrameModeler(self.df, oos_start=self.oos_start, info=info)

    # #########################################################################
    # Private helpers
    # #########################################################################

    def _run_model(self, model: FitPredictNode, method: str) -> DataFrameModeler:
        info = collections.OrderedDict()
        if method == "fit":
            df_out = model.fit(self._df[: self.oos_start])["df_out"]
            info["fit"] = model.get_info("fit")
            oos_start = None
        elif method == "predict":
            dbg.dassert(
                self.oos_start, msg="Must set `oos_start` to run `predict()`"
            )
            model.fit(self._df[self.oos_start :])
            info["fit"] = model.get_info("fit")
            df_out = model.predict(self._df)["df_out"]
            info["predict"] = model.get_info("predict")
            oos_start = self.oos_start
        else:
            raise ValueError(f"Unrecognized method `{method}`.")
        dfm = DataFrameModeler(df_out, oos_start, info)
        return dfm
