"""
Import as:

import core.model_plotter as modplot
"""

import logging
from typing import Any, List, Optional

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import core.finance as fin
import core.model_evaluator as modeval
import core.plotting as plot
import core.statistics as stats
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class ModelPlotter:
    """
    Wraps a ModelEvaluator with plotting functionality.
    """

    def __init__(self, model_evaluator: modeval.ModelEvaluator,) -> None:
        """
        Initialize by supplying an initialized `ModelEvaluator`.

        :param model_evaluator: initialized ModelEvaluator
        """
        dbg.dassert_isinstance(model_evaluator, modeval.ModelEvaluator)
        self.model_evaluator = model_evaluator

    def plot_rets_signal_analysis(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
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
        :param resample_rule: Resampling frequency to apply before plotting
        """
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys, weights=weights, mode=mode
        )
        if resample_rule is not None:
            rets = rets.resample(rule=resample_rule).sum(min_count=1)
        num_rows = 6
        fig = plt.figure(constrained_layout=True, figsize=(20, 5 * num_rows))
        gs = mpl.gridspec.GridSpec(num_rows, 2, figure=fig)
        # qq-plot against normal.
        plot.plot_qq(rets, ax=fig.add_subplot(gs[0, :]))
        # Plot lineplot and density plot.
        plot.plot_cols(
            rets, axes=[fig.add_subplot(gs[1, :]), fig.add_subplot(gs[2, :])]
        )
        # Plot pnl.
        plot.plot_pnl({"rets pnl": rets}, ax=fig.add_subplot(gs[3, :]))
        # Plot ACF and PACF.
        plot.plot_autocorrelation(
            rets,
            axes=[[fig.add_subplot(gs[4, 0]), fig.add_subplot(gs[4, -1])]],
            fft=True,
        )
        # Plot power spectral density and spectrogram.
        plot.plot_spectrum(
            rets, axes=[[fig.add_subplot(gs[5, 0]), fig.add_subplot(gs[5, -1])]]
        )

    def plot_performance(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
        resample_rule: Optional[str] = None,
        benchmark: Optional[pd.Series] = None,
        plot_cumulative_returns_kwargs: Optional[dict] = None,
        plot_rolling_beta_kwargs: Optional[dict] = None,
        plot_rolling_annualized_sharpe_ratio_kwargs: Optional[dict] = None,
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
        :param resample_rule: Resampling frequency to apply before plotting
        :param benchmark: Benchmark returns to compare against
        """
        # Obtain (log) returns.
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys, weights=weights, mode=mode
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
            cumrets = fin.convert_log_rets_to_pct_rets(cumrets)
        else:
            raise ValueError("Invalid cumulative returns mode `{cumrets_mode}`")
        plot.plot_cumulative_returns(
            cumrets,
            benchmark_series=benchmark,
            ax=axs[0],
            events=events,
            **plot_cumulative_returns_kwargs,
        )
        if benchmark is not None:
            plot.plot_rolling_beta(
                rets,
                benchmark,
                ax=axs[1],
                events=events,
                **plot_rolling_beta_kwargs,
            )
        plot.plot_rolling_annualized_sharpe_ratio(
            rets,
            ax=axs[-2],
            events=events,
            **plot_rolling_annualized_sharpe_ratio_kwargs,
        )
        plot.plot_drawdown(rets, ax=axs[-1], events=events)

    def plot_rets_and_vol(
        self,
        keys: Optional[List[Any]] = None,
        weights: Optional[List[Any]] = None,
        mode: Optional[str] = None,
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
        :param resample_rule: Resampling frequency to apply before plotting
        """
        plot_yearly_barplot_kwargs = plot_yearly_barplot_kwargs or {"unit": "%"}
        plot_monthly_heatmap_kwargs = plot_monthly_heatmap_kwargs or {"unit": "%"}
        plot_rolling_annualized_volatility_kwargs = (
            plot_rolling_annualized_volatility_kwargs
            or {"tau": 52, "max_depth": 1, "unit": "%"}
        )
        rets, _, _ = self.model_evaluator.aggregate_models(
            keys=keys, weights=weights, mode=mode
        )
        if resample_rule is not None:
            rets = rets.resample(rule=resample_rule).sum(min_count=1)
        num_plots = 3
        _, axs = plt.subplots(
            num_plots, 1, figsize=(20, 5 * num_plots), constrained_layout=True
        )
        # Plot yearly returns.
        plot.plot_yearly_barplot(
            rets,
            ax=axs[0],
            figsize=(20, 5 * num_plots),
            **plot_yearly_barplot_kwargs,
        )
        # Plot monthly returns.
        plot.plot_monthly_heatmap(rets, ax=axs[1], **plot_monthly_heatmap_kwargs)
        # Set OOS start if applicable.
        events = None
        if mode == "all_available" and self.model_evaluator.oos_start is not None:
            events = [(self.model_evaluator.oos_start, "OOS start")]
        # Plot volatility.
        plot.plot_rolling_annualized_volatility(
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
    ) -> None:
        """
        Plot holdings and turnover.

        :param keys: Use all available if `None`
        :param weights: Average if `None`
        :param mode: "all_available", "ins", or "oos"
        """
        _, pos, _ = self.model_evaluator.aggregate_models(
            keys=keys, weights=weights, mode=mode
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
        plot.plot_holdings(pos, ax=axs[0], events=events)
        # Plot turnover.
        plot.plot_turnover(pos, unit="%", ax=axs[1], events=events)

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
        plot.plot_sharpe_ratio_panel(rets, frequencies=frequencies)

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
        _, axes = plot.get_multiple_plots(
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
        multipletests_plot_kwargs = multipletests_plot_kwargs or {}
        pnls = self.model_evaluator.get_series_dict("pnls", keys=keys, mode=mode)
        pvals = {k: stats.ttest_1samp(v).loc["pval"] for k, v in pnls.items()}
        plot.multipletests_plot(
            pd.Series(pvals), threshold, **multipletests_plot_kwargs
        )
