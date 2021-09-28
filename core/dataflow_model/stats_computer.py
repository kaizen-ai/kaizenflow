"""
Import as:

import core.stats_computer as cstats
"""

import functools
import logging
from typing import Callable, List, Optional

import pandas as pd

import core.finance as cfinan
import core.statistics as cstati
import helpers.timer as htimer

_LOG = logging.getLogger(__name__)


# TODO(gp): Add unit test.
class StatsComputer:
    """
    Compute a particular piece of stats instead of the whole stats table.
    """

    def compute_time_series_stats(self, srs: pd.Series) -> pd.Series:
        """
        Compute statistics for a non-necessarily financial time series.
        """
        # List of pd.Series each with various metrics.
        stats = []
        with htimer.TimedScope(logging.DEBUG, "Computing samplings stats"):
            stats.append(self.compute_sampling_stats(srs))
        with htimer.TimedScope(logging.DEBUG, "Computing summary stats"):
            stats.append(self.compute_summary_stats(srs))
        with htimer.TimedScope(logging.DEBUG, "Computing stationarity stats"):
            stats.append(self.compute_stationarity_stats(srs))
        with htimer.TimedScope(logging.DEBUG, "Computing normality stats"):
            stats.append(self.compute_normality_stats(srs))
        # This seems to be slow.
        # stats.append(self.compute_autocorrelation_stats(srs))
        with htimer.TimedScope(logging.DEBUG, "Computing spectral stats"):
            stats.append(self.compute_spectral_stats(srs))
        with htimer.TimedScope(logging.DEBUG, "Computing signal quality stats"):
            stats.append(self.compute_signal_quality_stats(srs))
        # Concatenate the resulting series into a single multi-index series.
        names = [stat.name for stat in stats]
        result = pd.concat(stats, axis=0, keys=names)
        result.name = srs.name
        return result

    def compute_sampling_stats(self, srs: pd.Series) -> pd.Series:
        name = "sampling"
        functions = [
            cstati.summarize_time_index_info,
            cstati.compute_special_value_stats,
        ]
        return self._compute_stat_functions(srs, name, functions)

    def compute_summary_stats(self, srs: pd.Series) -> pd.Series:
        name = "summary"
        # TODO(*): Add
        #   - var and std assuming zero mean
        functions = [
            functools.partial(
                cstati.compute_moments,
                prefix="scipy.",
            ),
            functools.partial(cstati.ttest_1samp, prefix="null_mean_zero."),
            cstati.compute_jensen_ratio,
            lambda x: x.describe(),
        ]
        return self._compute_stat_functions(srs, name, functions)

    def compute_stationarity_stats(self, srs: pd.Series) -> pd.Series:
        name = "stationarity"
        # Restrict the number of lags because
        #   1. On long time series, auto-selection is time-consuming
        #   2. In practice, the focus is typically on lower order lags
        lags = 16
        functions = [
            functools.partial(cstati.apply_adf_test, maxlag=lags, prefix="adf."),
            functools.partial(cstati.apply_kpss_test, nlags=lags, prefix="kpss."),
        ]
        return self._compute_stat_functions(srs, name, functions)

    def compute_normality_stats(self, srs: pd.Series) -> pd.Series:
        name = "normality"
        functions = [
            functools.partial(
                cstati.apply_normality_test, prefix="omnibus_null_normal."
            ),
            functools.partial(
                cstati.compute_centered_gaussian_total_log_likelihood,
                prefix="centered_gaussian.",
            ),
        ]
        # TODO(*): cstati.compute_centered_gaussian_log_likelihood
        return self._compute_stat_functions(srs, name, functions)

    @staticmethod
    def compute_autocorrelation_stats(srs: pd.Series) -> pd.Series:
        # name = "autocorrelation"
        # ljung_box = cstati.apply_ljung_box_test(srs)
        # TODO(Paul): Only return pvals. Rename according to test and lag.
        #     Change default lags reported.
        raise NotImplementedError

    def compute_spectral_stats(self, srs: pd.Series) -> pd.Series:
        name = "spectral"
        functions = [
            cstati.compute_forecastability,
        ]
        return self._compute_stat_functions(srs, name, functions)

    def compute_signal_quality_stats(self, srs: pd.Series) -> pd.Series:
        name = "signal_quality"
        functions = [
            cstati.summarize_sharpe_ratio,
            functools.partial(cstati.ttest_1samp, prefix="sr."),
        ]
        result = self._compute_stat_functions(srs, name, functions)
        kratio = pd.Series(cfinan.compute_kratio(srs), index=["kratio"])
        kratio.name = name
        return pd.concat([result, kratio])

    def compute_finance_stats(
        self,
        df: pd.DataFrame,
        *,
        returns_col: Optional[str] = None,
        predictions_col: Optional[str] = None,
        positions_col: Optional[str] = None,
        pnl_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Compute financially meaningful statistics.
        """
        results = []
        # Compute stats related to positions.
        if positions_col is not None:
            positions = df[positions_col]
            #
            name = "finance"
            functions = [cstati.compute_avg_turnover_and_holding_period]
            stats = self._compute_stat_functions(positions, name, functions)
            results.append(pd.concat([stats], keys=["finance"]))
        # Compute stats related to PnL.
        if pnl_col is not None:
            pnl = df[pnl_col]
            #
            results.append(self.compute_time_series_stats(pnl))
            #
            name = "pnl"
            functions = [
                cstati.compute_annualized_return_and_volatility,
                cstati.compute_max_drawdown,
                cstati.calculate_hit_rate,
            ]
            stats = self._compute_stat_functions(pnl, name, functions)
            results.append(pd.concat([stats], keys=["finance"]))
            #
            corr = pd.Series(
                cstati.compute_implied_correlation(pnl),
                index=["prediction_corr_implied_by_pnl"],
                name=name,
            )
            results.append(pd.concat([corr], keys=["correlation"]))
        # Currently we do not calculate individual prediction/returns stats.
        if returns_col is not None and predictions_col is not None:
            name = "pnl"
            returns = df[returns_col]
            predictions = df[predictions_col]
            #
            prediction_corr = predictions.corr(returns)
            corr = pd.Series(
                prediction_corr, index=["prediction_corr"], name=name
            )
            results.append(pd.concat([corr], keys=["correlation"]))
            #
            srs = pd.Series(
                cstati.compute_implied_sharpe_ratio(predictions, prediction_corr),
                index=["sr_implied_by_prediction_corr"],
                name=name,
            )
            results.append(pd.concat([srs], keys=["signal_quality"]))
            #
            j_ratio = cstati.compute_jensen_ratio(returns)["jensen_ratio"]
            hit_rate = pd.Series(
                cstati.compute_hit_rate_implied_by_correlation(
                    prediction_corr, j_ratio
                ),
                index=["hit_rate_implied_by_prediction_corr"],
                name=name,
            )
            results.append(pd.concat([hit_rate], keys=["finance"]))
            #
            hit_rate = cstati.calculate_hit_rate(returns * predictions)
            hit_rate = hit_rate["hit_rate_point_est_(%)"] / 100
            corr2 = pd.Series(
                cstati.compute_correlation_implied_by_hit_rate(hit_rate, j_ratio),
                index=["prediction_corr_implied_by_hit_rate"],
                name=name,
            )
            results.append(pd.concat([corr2], keys=["correlation"]))
        if returns_col is not None and positions_col is not None:
            returns = df[returns_col]
            positions = df[positions_col]
            #
            name = "pnl"
            bets = cstati.compute_bet_stats(positions, returns)
            bets.name = name
            results.append(pd.concat([bets], keys=["bets"]))
        if returns_col is not None and pnl_col is not None:
            returns = df[returns_col]
            pnl = df[pnl_col]
            #
            corr = pd.Series(
                pnl.corr(returns), index=["pnl_corr_to_underlying"], name=name
            )
            results.append(pd.concat([corr], keys=["correlation"]))
        # No predictions and positions calculations yet.
        # No predictions and PnL calculations yet.
        # No positions and PnL calculations yet.
        return pd.concat(results, axis=0)

    @staticmethod
    def _compute_stat_functions(
        srs: pd.Series,
        name: str,
        functions: List[Callable],
    ) -> pd.Series:
        """
        Apply a list of functions to a series.
        """
        # Apply the functions.
        stats = [function(srs).rename(name) for function in functions]
        # Concat the list of series in a single one.
        srs_out = pd.concat(stats)
        return srs_out
