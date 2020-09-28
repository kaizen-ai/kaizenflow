"""
Import as:

import core.dataframe_modeler as dfmod
"""

from __future__ import annotations

import collections
import datetime
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import matplotlib as mpl
import numpy as np
import pandas as pd

import core.dataflow as dtf
import core.finance as fin
import core.plotting as plot
import core.signal_processing as sigp
import core.statistics as stats
import core.timeseries_study as tss
import helpers.dbg as dbg

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
        return self._df[: self.oos_start].copy()

    @property
    def oos_df(self) -> pd.DataFrame:
        dbg.dassert(self.oos_start, msg="`oos_start` must be set")
        return self._df[self.oos_start :].copy()

    @property
    def df(self) -> pd.DataFrame:
        return self._df.copy()

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
        model = dtf.ColumnTransformer(
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
        model = dtf.Resample(
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
        model = dtf.Residualizer(
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
        model = dtf.ContinuousSkLearnModel(
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
        model = dtf.SmaModel(
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
        model = dtf.UnsupervisedSkLearnModel(
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
        model = dtf.VolatilityModel(
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
        model = dtf.ColumnTransformer(
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
        model = dtf.ColumnTransformer(
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
        model = dtf.ColumnTransformer(
            nid="set_weekends_to_nan",
            transformer_func=fin.set_weekends_to_nan,
            col_mode="replace_all",
        )
        return self._run_model(model, method)

    # #########################################################################
    # Dataframe stats and plotting
    # #########################################################################

    def calculate_stats(
        self, cols: Optional[List[Any]] = None, mode: str = "ins"
    ) -> pd.DataFrame:
        """
        Calculate stats for selected columns.
        """
        df = self._get_df(cols=cols, mode=mode)
        # Calculate stats.
        stats_dict = {}
        for col in df.columns:
            stats_val = self._calculate_series_stats(df[col])
            stats_dict[col] = stats_val
        stats_df = pd.concat(stats_dict, axis=1)
        return stats_df

    def plot_correlation_with_lag(
        self, lag: int, cols: Optional[List[Any]] = None, mode: str = "ins"
    ) -> pd.DataFrame:
        """
        Calculate correlation of `cols` with lags of `cols`.
        """
        df = self._get_df(cols=cols, mode=mode)
        # Calculate correlation.
        corr_df = sigp.correlate_with_lag(df, lag=lag)
        return plot.plot_correlation_matrix(corr_df)

    def plot_autocorrelation(
        self,
        cols: Optional[List[Any]] = None,
        plot_auto_correlation_kwargs: Optional[dict] = None,
        mode: str = "ins",
    ) -> None:
        df = self._get_df(cols=cols, mode=mode)
        plot_auto_correlation_kwargs = plot_auto_correlation_kwargs or {}
        plot.plot_autocorrelation(df, **plot_auto_correlation_kwargs)

    def plot_sequence_and_density(
        self,
        cols: Optional[List[Any]] = None,
        plot_cols_kwargs: Optional[dict] = None,
        mode: str = "ins",
    ) -> None:
        df = self._get_df(cols=cols, mode=mode)
        plot_cols_kwargs = plot_cols_kwargs or {}
        plot.plot_cols(df, **plot_cols_kwargs)

    def plot_spectrum(
        self,
        cols: Optional[List[Any]] = None,
        plot_spectrum_kwargs: Optional[dict] = None,
        mode: str = "ins",
    ) -> None:
        df = self._get_df(cols=cols, mode=mode)
        plot_spectrum_kwargs = plot_spectrum_kwargs or {}
        plot.plot_spectrum(df, **plot_spectrum_kwargs)

    def plot_correlation_matrix(
        self,
        cols: Optional[List[Any]] = None,
        plot_correlation_matrix_kwargs: Optional[dict] = None,
        mode: str = "ins",
    ) -> pd.DataFrame:
        df = self._get_df(cols=cols, mode=mode)
        plot_correlation_matrix_kwargs = plot_correlation_matrix_kwargs or {}
        return plot.plot_correlation_matrix(df, **plot_correlation_matrix_kwargs)

    def plot_dendrogram(
        self,
        cols: Optional[List[Any]] = None,
        figsize: Optional[Tuple[int, int]] = None,
        mode: str = "ins",
    ) -> None:
        df = self._get_df(cols=cols, mode=mode)
        plot.plot_dendrogram(df, figsize)

    def plot_pca_components(
        self,
        cols: Optional[List[Any]] = None,
        num_components: Optional[int] = None,
        mode: str = "ins",
    ) -> None:
        df = self._get_df(cols=cols, mode=mode)
        pca = plot.PCA(mode="standard")
        pca.fit(df.replace([np.inf, -np.inf], np.nan).fillna(0))
        pca.plot_components(num_components)

    def plot_explained_variance(
        self, cols: Optional[List[Any]] = None, mode: str = "ins"
    ) -> None:
        df = self._get_df(cols=cols, mode=mode)
        pca = plot.PCA(mode="standard")
        pca.fit(df.replace([np.inf, -np.inf], np.nan).fillna(0))
        pca.plot_explained_variance()

    def plot_qq(
        self,
        col: Any,
        ax: Optional[mpl.axes.Axes] = None,
        dist: Optional[str] = None,
        nan_mode: Optional[str] = None,
        mode: str = "ins",
    ) -> None:
        srs = self._get_df(cols=[col], mode=mode).squeeze()
        plot.plot_qq(srs, ax=ax, dist=dist, nan_mode=nan_mode)

    def plot_rolling_correlation(
        self,
        col1: Any,
        col2: Any,
        tau: float,
        plot_rolling_correlation_kwargs: Optional[dict] = None,
        mode: str = "ins",
    ) -> None:
        df = self._get_df(cols=[col1, col2], mode=mode)
        plot_rolling_correlation_kwargs = plot_rolling_correlation_kwargs or {}
        plot.plot_rolling_correlation(
            df[col1], df[col2], tau=tau, **plot_rolling_correlation_kwargs,
        )

    def plot_time_series_study(self, col: Any, mode: str = "ins") -> None:
        srs = self._get_df(cols=[col], mode=mode).squeeze()
        tsds = tss.TimeSeriesDailyStudy(srs, data_name=str(col))
        tsds.execute()

    # #########################################################################
    # Private helpers
    # #########################################################################

    def _get_df(self, cols: Optional[List[Any]], mode: str) -> pd.DataFrame:
        cols = cols or self._df.columns
        dbg.dassert_is_subset(cols, self._df.columns)
        if mode == "ins":
            return self.ins_df[cols]
        if mode == "oos":
            return self.oos_df[cols]
        if mode == "all_available":
            return self.df[cols]
        raise ValueError(f"Unrecognized mode `{mode}`")

    def _run_model(
        self, model: dtf.FitPredictNode, method: str
    ) -> DataFrameModeler:
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

    @staticmethod
    def _calculate_series_stats(srs: pd.Series) -> pd.Series:
        """
        Calculate stats for a single series.
        """
        dbg.dassert(not srs.empty, msg="Series should not be empty")
        stats_dict = {}
        stats_dict[0] = stats.summarize_time_index_info(srs)
        stats_dict[1] = stats.compute_jensen_ratio(srs)
        stats_dict[2] = stats.compute_forecastability(srs)
        stats_dict[3] = stats.compute_moments(srs)
        stats_dict[4] = stats.compute_special_value_stats(srs)
        stats_dict[5] = stats.apply_normality_test(srs, prefix="normality_")
        stats_dict[6] = stats.apply_adf_test(srs, prefix="adf_")
        stats_dict[7] = stats.apply_kpss_test(srs, prefix="kpss_")
        # Sort dict by integer keys.
        stats_dict = dict(sorted(stats_dict.items()))
        stats_srs = pd.concat(stats_dict).droplevel(0)
        stats_srs.name = "stats"
        return stats_srs
