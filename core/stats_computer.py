"""
Import as:

import core.stats_computer as cstats
"""

import functools
import logging
from typing import Optional

import pandas as pd

import core.finance as cfinan
import core.statistics as cstati

_LOG = logging.getLogger(__name__)


class StatsComputer:
    """
    Allows to get particular piece of stats instead of the whole stats table.
    """

    def compute_stats(
        self, srs: pd.Series, time_series_type: Optional[str] = None
    ) -> pd.Series:
        stats = []
        stats.append(self.compute_sampling_stats(srs))
        stats.append(self.compute_summary_stats(srs))
        stats.append(self.compute_stationarity_stats(srs))
        stats.append(self.compute_normality_stats(srs))
        # stats.append(self.compute_autocorrelation_stats(srs))
        stats.append(self.compute_spectral_stats(srs))
        stats.append(self.compute_signal_quality_stats(srs))
        if time_series_type is not None:
            stats.append(self.compute_finance_stats(srs, time_series_type))
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
            functools.partial(cstati.compute_moments, prefix="scipy.",),
            functools.partial(cstati.ttest_1samp, prefix="null_mean_zero."),
            cstati.compute_jensen_ratio,
            lambda x: x.describe(),
        ]
        return self._compute_stat_functions(srs, name, functions)

    def compute_stationarity_stats(self, srs: pd.Series) -> pd.Series:
        name = "stationarity"
        functions = [
            functools.partial(cstati.apply_adf_test, prefix="adf."),
            functools.partial(cstati.apply_kpss_test, prefix="kpss."),
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
                prefix="centered_gaussian."
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
        #
        return pd.concat([result, kratio])

    def compute_finance_stats(self, srs: pd.Series, time_series_type: str) -> pd.Series:
        """
        Assumes `srs` is a PnL curve.

        :mode: pnl, positions
        """
        name = "finance"
        if time_series_type == "pnl":
            functions = [
                cstati.compute_annualized_return_and_volatility,
                cstati.compute_max_drawdown,
                cstati.calculate_hit_rate,
            ]
        elif time_series_type == "positions":
            functions = [cstati.compute_avg_turnover_and_holding_period]
        else:
            raise ValueError
        return self._compute_stat_functions(srs, name, functions)

    @staticmethod
    def compute_bet_stats(
        *, positions: pd.Series, returns: pd.Series
    ) -> pd.Series:
        name = "bets"
        bets = cstati.compute_bet_stats(positions, returns[positions.index])
        bets.name = name
        return bets

    # TODO(Paul): Add correlation for calculating
    #   - correlation of pnl to rets
    #   - correlation of predictions to rets

    @staticmethod
    def _compute_stat_functions(
        srs: pd.Series,
        name: str,
        functions: list,
    ) -> pd.Series:
        stats = [function(srs).rename(name) for function in functions]
        return pd.concat(stats)
