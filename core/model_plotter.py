"""
Import as:

import core.model_plotter as cmodel
"""

import logging
from typing import Any, List, Optional

import matplotlib as matplo
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

import core.finance as cfinan
import core.model_evaluator as cmodel
import core.plotting as cplott
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class ModelPlotter:
    """
    Wraps a ModelEvaluator with plotting functionality.
    """

    def __init__(
        self,
        model_evaluator: cmodel.ModelEvaluator,
    ) -> None:
        """
        Initialize by supplying an initialized `ModelEvaluator`.

        :param model_evaluator: initialized ModelEvaluator
        """
        dbg.dassert_isinstance(model_evaluator, cmodel.ModelEvaluator)
        self.model_evaluator = model_evaluator

    def plot_rets_signal_analysis(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        target_volatility: Optional[float] = None,
        resample_rule: Optional[str] = None,
    ) -> None:
        """
        Plot a panel of signal analyses for (log) returns.

        Plots include:
        - Q-Q plot
        - histogram
        - time evolution
        - cumulative sum
        - ACF and PACF
        - spectral density and spectrogram

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param target_volatility: Rescale portfolio to achieve
            `target_volatility` on in-sample region
        :param resample_rule: Resampling frequency to apply before plotting
        """
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys,
            weights=weights,
            mode=mode,
            target_volatility=target_volatility,
        )
        if resample_rule is not None:
            rets = rets.resample(rule=resample_rule).sum(min_count=1)
        num_rows = 6
        fig = plt.figure(constrained_layout=True, figsize=(20, 5 * num_rows))
        gs = matplo.gridspec.GridSpec(num_rows, 2, figure=fig)
        # qq-plot against normal.
        cplott.plot_qq(rets, ax=fig.add_subplot(gs[0, :]))
        # Plot lineplot and density cplott.
        cplott.plot_cols(
            rets, axes=[fig.add_subplot(gs[1, :]), fig.add_subplot(gs[2, :])]
        )
        # Plot pnl.
        cplott.plot_pnl({"rets pnl": rets}, ax=fig.add_subplot(gs[3, :]))
        # Plot ACF and PACF.
        cplott.plot_autocorrelation(
            rets,
            axes=[[fig.add_subplot(gs[4, 0]), fig.add_subplot(gs[4, -1])]],
            fft=True,
        )
        # Plot power spectral density and spectrogram.
        cplott.plot_spectrum(
            rets, axes=[[fig.add_subplot(gs[5, 0]), fig.add_subplot(gs[5, -1])]]
        )

    def plot_performance(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        target_volatility: Optional[float] = None,
        resample_rule: Optional[str] = None,
        benchmark: Optional[pd.Series] = None,
        plot_cumulative_returns_kwargs: Optional[dict] = None,
        plot_rolling_beta_kwargs: Optional[dict] = None,
        plot_rolling_annualized_sharpe_ratio_kwargs: Optional[dict] = None,
        plot_drawdown_kwargs: Optional[dict] = None,
    ) -> None:
        """
        Plot strategy performance.

        Plots include:
        - Cumulative returns
        - Rolling Sharpe Ratio
        - Drawdown

        If a benchmark is provided, then
        - Cumulative returns against benchmark is displayed
        - Rolling beta against benchmark is displayed

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param target_volatility: Rescale portfolio to achieve
            `target_volatility` on in-sample region
        :param resample_rule: Resampling frequency to apply before plotting
        :param benchmark: Benchmark returns to compare against
        """
        # Obtain (log) returns.
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys,
            weights=weights,
            mode=mode,
            target_volatility=target_volatility,
        )
        if resample_rule is not None:
            rets = rets.resample(rule=resample_rule).sum(min_count=1)
        # Set kwargs.
        plot_cumulative_returns_kwargs = plot_cumulative_returns_kwargs or {
            "mode": "pct",
            "unit": "%",
        }
        plot_rolling_beta_kwargs = plot_rolling_beta_kwargs or {"window": 52}
        plot_rolling_annualized_sharpe_ratio_kwargs = (
            plot_rolling_annualized_sharpe_ratio_kwargs
            or {"tau": 52, "max_depth": 1, "ci": 0.5}
        )
        # Set OOS start if applicable.
        events = None
        if mode == "all_available" and self.model_evaluator.oos_start is not None:
            events = [(self.model_evaluator.oos_start, "OOS start")]
        # Set number of plots.
        if benchmark is not None:
            num_plots = 4
        else:
            num_plots = 3
        _, axs = plt.subplots(
            num_plots, 1, figsize=(20, 5 * num_plots), constrained_layout=True
        )
        cumrets = rets.cumsum()
        cumrets_mode = plot_cumulative_returns_kwargs["mode"]
        if cumrets_mode == "log":
            pass
        elif cumrets_mode == "pct":
            cumrets = cfinan.convert_log_rets_to_pct_rets(cumrets)
        else:
            raise ValueError("Invalid cumulative returns mode `{cumrets_mode}`")
        cplott.plot_cumulative_returns(
            cumrets,
            benchmark_series=benchmark,
            ax=axs[0],
            events=events,
            **plot_cumulative_returns_kwargs,
        )
        if benchmark is not None:
            cplott.plot_rolling_beta(
                rets,
                benchmark,
                ax=axs[1],
                events=events,
                **plot_rolling_beta_kwargs,
            )
        cplott.plot_rolling_annualized_sharpe_ratio(
            rets,
            ax=axs[-2],
            events=events,
            **plot_rolling_annualized_sharpe_ratio_kwargs,
        )
        plot_drawdown_kwargs = plot_drawdown_kwargs or {}
        cplott.plot_drawdown(
            rets, ax=axs[-1], events=events, **plot_drawdown_kwargs
        )

    def plot_rets_and_vol(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        target_volatility: Optional[float] = None,
        resample_rule: Optional[str] = None,
        plot_yearly_barplot_kwargs: Optional[dict] = None,
        plot_monthly_heatmap_kwargs: Optional[dict] = None,
        plot_rolling_annualized_volatility_kwargs: Optional[dict] = None,
    ) -> None:
        """
        Plot returns by year and month, plot rolling volatility.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param target_volatility: Rescale portfolio to achieve
            `target_volatility` on in-sample region
        :param resample_rule: Resampling frequency to apply before plotting
        """
        plot_yearly_barplot_kwargs = plot_yearly_barplot_kwargs or {"unit": "%"}
        plot_monthly_heatmap_kwargs = plot_monthly_heatmap_kwargs or {"unit": "%"}
        plot_rolling_annualized_volatility_kwargs = (
            plot_rolling_annualized_volatility_kwargs
            or {"tau": 52, "max_depth": 1, "unit": "%"}
        )
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys,
            weights=weights,
            mode=mode,
            target_volatility=target_volatility,
        )
        if resample_rule is not None:
            rets = rets.resample(rule=resample_rule).sum(min_count=1)
        num_plots = 3
        _, axs = plt.subplots(
            num_plots, 1, figsize=(20, 5 * num_plots), constrained_layout=True
        )
        # Plot yearly returns.
        cplott.plot_yearly_barplot(
            rets,
            ax=axs[0],
            figsize=(20, 5 * num_plots),
            **plot_yearly_barplot_kwargs,
        )
        # Plot monthly returns.
        cplott.plot_monthly_heatmap(
            rets, ax=axs[1], **plot_monthly_heatmap_kwargs
        )
        # Set OOS start if applicable.
        events = None
        if mode == "all_available" and self.model_evaluator.oos_start is not None:
            events = [(self.model_evaluator.oos_start, "OOS start")]
        # Plot volatility.
        cplott.plot_rolling_annualized_volatility(
            rets,
            ax=axs[2],
            events=events,
            **plot_rolling_annualized_volatility_kwargs,
        )

    def plot_positions(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        target_volatility: Optional[float] = None,
    ) -> None:
        """
        Plot holdings and turnover.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param target_volatility: Rescale portfolio to achieve
            `target_volatility` on in-sample region
        """
        _, pos, _ = self.model_evaluator.aggregate_models(
            keys=keys,
            weights=weights,
            mode=mode,
            target_volatility=target_volatility,
        )
        num_plots = 2
        _, axs = plt.subplots(
            num_plots, 1, figsize=(20, 5 * num_plots), constrained_layout=True
        )
        # Set OOS start if applicable.
        events = None
        if mode == "all_available" and self.model_evaluator.oos_start is not None:
            events = [(self.model_evaluator.oos_start, "OOS start")]
        # Plot holdings.
        cplott.plot_holdings(pos, ax=axs[0], events=events)
        # Plot turnover.
        cplott.plot_turnover(pos, unit="%", ax=axs[1], events=events)

    def plot_sharpe_ratio_panel(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        frequencies: Optional[List[str]] = None,
    ) -> None:
        """
        Plot how the SR varies under resampling.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param resample_rule: Resampling frequency to apply before plotting
        """
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys, weights=weights, mode=mode
        )
        cplott.plot_sharpe_ratio_panel(rets, frequencies=frequencies)

    def plot_returns_and_predictions(
        self,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
    ) -> None:
        """
        Plot returns and model predictions (per key).

        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        :param resample_rule: Resampling frequency to apply before plotting
        """
        keys = keys or self.model_evaluator.valid_keys
        rets = self.model_evaluator.get_series_dict(
            "returns", keys=keys, mode=mode
        )
        preds = self.model_evaluator.get_series_dict(
            "predictions", keys=keys, mode=mode
        )
        _, axes = cplott.get_multiple_plots(
            len(keys), 1, y_scale=5, sharex=True, sharey=True
        )
        if not isinstance(axes, np.ndarray):
            axes = [axes]
        for idx, key in enumerate(keys):
            y_yhat = pd.concat([rets[key], preds[key]], axis=1)
            if resample_rule is not None:
                y_yhat = y_yhat.resample(rule=resample_rule).sum(min_count=1)
            y_yhat.plot(ax=axes[idx], title=f"Model {key}")
        plt.suptitle("Returns and predictions over time", y=1.01)
        plt.tight_layout()

    def plot_multiple_tests_adjustment(
        self,
        threshold: float,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        multipletests_plot_kwargs: Optional[dict] = None,
    ) -> None:
        """
        Adjust p-values for selected keys and cplott.

        :param threshold: Adjust p-value threshold for a "pass"
        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        """
        multipletests_plot_kwargs = multipletests_plot_kwargs or {}
        pnls = self.model_evaluator.get_series_dict("pnls", keys=keys, mode=mode)
        pvals = {k: cstati.ttest_1samp(v).loc["pval"] for k, v in pnls.items()}
        cplott.multipletests_plot(
            pd.Series(pvals), threshold, **multipletests_plot_kwargs
        )

    def plot_multiple_pnls(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
    ) -> None:
        """
        Plot multiple pnl series (cumulatively summed) simultaneously.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        :param resample_rule: Resampling frequency to apply before plotting
        """
        keys = keys or self.model_evaluator.valid_keys
        pnls = self.model_evaluator.get_series_dict("pnls", keys=keys, mode=mode)
        aggregate_pnl, _, _ = self.model_evaluator.aggregate_models(
            keys=keys, weights=weights, mode=mode
        )
        dbg.dassert_not_in("aggregated", pnls.keys())
        pnls["aggregated"] = aggregate_pnl
        if resample_rule is not None:
            for k, v in pnls.items():
                pnls[k] = v.resample(rule=resample_rule).sum(min_count=1)
        cplott.plot_pnl(pnls)

    def plot_correlation_matrix(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
        plot_correlation_matrix_kwargs: Optional[dict] = None,
    ) -> None:
        """
        Plot correlation matrix.

        :param series: "returns", "predictions", "positions", or "pnls"
        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        """
        plot_correlation_matrix_kwargs = plot_correlation_matrix_kwargs or {
            "mode": "heatmap"
        }
        df = self._get_series_as_df(
            series, keys=keys, mode=mode, resample_rule=resample_rule
        )
        cplott.plot_correlation_matrix(df, **plot_correlation_matrix_kwargs)

    def plot_clustermap(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
        clustermap_kwargs: Optional[dict] = None,
    ) -> None:
        """
        Plot correlation matrix together with dendrogram.

        :param series: "returns", "predictions", "positions", or "pnls"
        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        """
        clustermap_kwargs = clustermap_kwargs or {}
        df = self._get_series_as_df(
            series, keys=keys, mode=mode, resample_rule=resample_rule
        )
        corr = df.corr().fillna(0)
        sns.clustermap(corr, **clustermap_kwargs)

    def plot_dendrogram(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
    ) -> None:
        """
        Plot dendrogram of correlation of selected series.

        :param series: "returns", "predictions", "positions", or "pnls"
        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        """
        df = self._get_series_as_df(
            series, keys=keys, mode=mode, resample_rule=resample_rule
        )
        cplott.plot_dendrogram(df.fillna(0))

    def plot_multiple_time_series(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
        plot_time_series_dict_kwargs: Optional[dict] = None,
    ) -> None:
        """
        Plot one time series per cplott.

        :param series: "returns", "predictions", "positions", or "pnls"
        :param keys: Use all available if `None`
        :param mode: "all_available", "ins", or "oos"
        :param resample_rule: Resampling frequency to apply before plotting
        """
        plot_time_series_dict_kwargs = plot_time_series_dict_kwargs or {}
        series_dict = self.model_evaluator.get_series_dict(
            series, keys=keys, mode=mode
        )
        if resample_rule is not None:
            for k, v in series_dict.items():
                series_dict[k] = v.resample(rule=resample_rule).sum(min_count=1)
        cplott.plot_time_series_dict(series_dict, **plot_time_series_dict_kwargs)

    def plot_pca_components(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
        num_components: Optional[int] = None,
    ) -> None:
        df = self._get_series_as_df(
            series, keys=keys, mode=mode, resample_rule=resample_rule
        )
        pca = cplott.PCA(mode="standard")
        pca.fit(df.fillna(0))
        pca.plot_components(num_components)

    def plot_explained_variance(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
    ) -> None:
        df = self._get_series_as_df(
            series, keys=keys, mode=mode, resample_rule=resample_rule
        )
        pca = cplott.PCA(mode="standard")
        pca.fit(df.fillna(0))
        pca.plot_explained_variance()

    def _get_series_as_df(
        self,
        series: str,
        keys: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
    ) -> pd.DataFrame:
        series_dict = self.model_evaluator.get_series_dict(
            series, keys=keys, mode=mode
        )
        if resample_rule is not None:
            for k, v in series_dict.items():
                series_dict[k] = v.resample(rule=resample_rule).sum(min_count=1)
        df = pd.DataFrame.from_dict(series_dict)
        return df
