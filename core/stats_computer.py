"""
Import as:

import core.stats_computer as cstats
"""

import logging
from typing import Optional

import pandas as pd

import core.finance as cfinan
import core.statistics as cstati
import helpers.dbg as dbg

_LOG = logging.getLogger(__name__)


class StatsComputer:
    """
    Allows to get particular piece of stats instead of the whole stats table.
    """

    @staticmethod
    def compute_sampling_stats(srs: pd.Series) -> pd.Series:
        name = "sampling"
        #
        time_index_info = cstati.summarize_time_index_info(srs)
        time_index_info.name = name
        #
        special_value_stats = cstati.compute_special_value_stats(srs)
        special_value_stats.name = name
        return pd.concat([time_index_info, special_value_stats])

    @staticmethod
    def compute_summary_stats(srs: pd.Series) -> pd.Series:
        name = "summary"
        #
        moments = cstati.compute_moments(srs)
        moments.name = name
        #
        descriptive = srs.describe()
        descriptive.name = name
        #
        jensen = cstati.compute_jensen_ratio(srs)
        jensen.name = name
        # TODO(*): Add
        #   - t-test for zero mean
        #   - var and std assuming zero mean
        #   - jensen ratio
        return pd.concat([moments, descriptive])

    @staticmethod
    def compute_stationarity_stats(srs: pd.Series) -> pd.Series:
        name = "stationarity"
        #
        adf = cstati.apply_adf_test(srs, prefix="adf_")
        adf.name = name
        #
        kpss = cstati.apply_kpss_test(srs, prefix="kpss_")
        kpss.name = name
        return pd.concat([adf, kpss])

    @staticmethod
    def compute_normality_stats(srs: pd.Series) -> pd.Series:
        name = "normality"
        #
        normality = cstati.apply_normality_test(srs)
        normality.name = name
        # TODO(*): cstati.compute_centered_gaussian_log_likelihood
        return pd.concat([normality])

    @staticmethod
    def compute_autocorrelation_stats(srs: pd.Series) -> pd.Series:
        # name = "autocorrelation"
        # ljung_box = cstati.apply_ljung_box_test(srs)
        # TODO(Paul): Only return pvals. Rename according to test and lag.
        #     Change default lags reported.
        raise NotImplementedError

    @staticmethod
    def compute_spectral_stats(srs: pd.Series) -> pd.Series:
        # TODO(Paul): `compute_forecastability()` goes here.
        raise NotImplementedError

    @staticmethod
    def compute_signal_quality_stats(srs: pd.Series) -> pd.Series:
        name = "signal_quality"
        #
        sharpe_ratio = cstati.summarize_sharpe_ratio(srs)
        sharpe_ratio.name = name
        #
        ttest = cstati.ttest_1samp(srs)
        ttest.name = name
        #
        kratio = pd.Series(cfinan.compute_kratio(srs), index=["kratio"])
        kratio.name = name
        #
        return pd.concat([sharpe_ratio, ttest, kratio])

    @staticmethod
    def compute_finance_stats(srs: pd.Series, mode: str) -> pd.Series:
        """
        Assumes `srs` is a PnL curve.

        :mode: pnl, positions
        """
        name = "finance"
        if mode == "pnl":
            ret_and_vol = cstati.compute_annualized_return_and_volatility(srs)
            ret_and_vol.name = name
            #
            drawdown = cstati.compute_max_drawdown(srs)
            drawdown.name = name
            #
            hit_rate = cstati.calculate_hit_rate(srs)
            hit_rate.name = name
            #
            return pd.concat([ret_and_vol, drawdown, hit_rate])
        elif mode == "positions":
            turnover = cstati.compute_avg_turnover_and_holding_period(srs)
            turnover.name = name
            return pd.concat([turnover])
        else:
            raise ValueError


class PnlReturnsMixin:
    """
    Add methods with pnl and returns inputs.
    """

    @staticmethod
    def calculate_corr_to_underlying(
        pnl: pd.Series, returns: pd.Series
    ) -> pd.Series:
        return pd.Series(pnl.corr(returns), index=["corr_to_underlying"])

    def _calculate_pnl_returns_stats(
        self,
        pnl: pd.Series,
        returns: pd.Series,
    ) -> pd.Series:
        """
        Calculate stats for methods with pnl and returns inputs.
        """
        if pnl is None or returns is None:
            return None
        dbg.dassert_isinstance(pnl, pd.Series)
        dbg.dassert_isinstance(returns, pd.Series)
        return self.calculate_corr_to_underlying(pnl, returns)


class PositionsReturnsMixin:
    """
    Add methods with positions and returns inputs.
    """

    @staticmethod
    def compute_bet_stats(positions: pd.Series, returns: pd.Series) -> pd.Series:
        return cstati.compute_bet_stats(positions, returns[positions.index])

    @staticmethod
    def compute_prediction_corr(
        positions: pd.Series, returns: pd.Series
    ) -> pd.Series:
        return pd.Series(positions.corr(returns), index=["prediction_corr"])

    def _calculate_positions_returns_stats(
        self,
        positions: pd.Series,
        returns: pd.Series,
    ) -> pd.Series:
        """
        Calculate stats for methods with positions and returns inputs.
        """
        if positions is None or returns is None:
            return None
        dbg.dassert_isinstance(positions, pd.Series)
        dbg.dassert_isinstance(returns, pd.Series)
        stats_vals = pd.concat(
            [
                self.compute_bet_stats(positions, returns),
                self.compute_prediction_corr(positions, returns),
            ]
        )
        return stats_vals


class SeriesStatsComputer(StatsComputer):
    """
    Class for series stats only.
    """

    def calculate_stats(self, srs: pd.Series) -> pd.Series:
        """
        Calculate all available stats as in dataframe_modeler.
        """
        dbg.dassert_isinstance(srs, pd.Series)
        stats_vals = pd.concat(
            [
                self.summarize_time_index_info(srs),
                self.compute_jensen_ratio(srs),
                self.compute_forecastability(srs),
                self.compute_moments(srs),
                self.compute_special_value_stats(srs),
                self.apply_normality_test(srs),
                self.apply_stationarity_tests(srs),
            ]
        )
        return stats_vals


class ModelStatsComputer(StatsComputer, PnlReturnsMixin, PositionsReturnsMixin):
    """
    Class for model stats only.
    """

    def calculate_stats(
        self,
        pnl: Optional[pd.Series] = None,
        positions: Optional[pd.Series] = None,
        returns: Optional[pd.Series] = None,
    ) -> pd.Series:
        """
        Calculate all available stats as in model_evaluator.
        """
        dbg.dassert(
            not pd.isna([pnl, positions, returns]).all(),
            "At least one input series should be not `None`.",
        )
        freqs = {
            srs.index.freq for srs in [pnl, positions, returns] if srs is not None
        }
        dbg.dassert_eq(len(freqs), 1, "Series have different frequencies.")
        stats_vals = pd.Series()
        if pnl is not None:
            stats_vals = pd.concat(
                [
                    self.summarize_sharpe_ratio(pnl),
                    self.ttest_1samp(pnl),
                    self.compute_kratio(pnl),
                    self.compute_annualized_return_and_volatility(pnl),
                    self.compute_max_drawdown(pnl),
                    self.summarize_time_index_info(pnl),
                    self.calculate_hit_rate(pnl),
                    self.compute_jensen_ratio(pnl),
                    self.compute_forecastability(pnl),
                    self.compute_moments(pnl),
                    self.compute_special_value_stats(pnl),
                ]
            )
        if positions is not None:
            stats_vals = pd.concat(
                [
                    stats_vals,
                    self.compute_avg_turnover_and_holding_period(positions),
                ]
            )
        stats_vals = pd.concat(
            [
                stats_vals,
                self._calculate_pnl_returns_stats(pnl, returns),
                self._calculate_positions_returns_stats(positions, returns),
            ]
        )
        return stats_vals
